package echoqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/settle.lua
var settleScriptSource string

// settleScript runs the embedded script through EVALSHA with an automatic
// EVAL fallback, so the hot path never resends the script body.
var settleScript = redis.NewScript(settleScriptSource)

var errSettle = errors.New("echoqueue: settle failed")

// errResultTooLarge is the internal, unexported marker for Settle outcomes
// whose Result data exceeds the configured byte limits. Oversized results are
// rejected before any Redis capability probe, Pending read, or Lua call, so
// Pending, deadline, Receipt, and effect lists are never touched.
var errResultTooLarge = errors.New("echoqueue: result exceeds configured size limit")

// Settle accepts a batch ID and worker outcome. The pending snapshot supplies
// the queue and policy, so callers never repeat task or routing information.
func (s *Scheduler) Settle(ctx context.Context, batchID string, outcome Outcome) (Receipt, error) {
	if s == nil || s.rdb == nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: scheduler is nil", errSettle)
	}
	if ctx == nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: context is nil", errSettle)
	}
	if err := validateText("batch_id", batchID, true); err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: %v", errSettle, err)
	}
	if err := outcome.validate(); err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: %v", errSettle, err)
	}
	if err := validateOutcomeResultSizes(s.config.MaxPayloadBytes, s.config.MaxBatchBytes, outcome); err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: %v", errSettle, err)
	}
	if err := s.ensureRedis(ctx); err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, err
	}
	command, err := commandHash(batchID, outcome)
	if err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: command hash: %v", errSettle, err)
	}

	pendingKey := s.keys.pending(batchID)
	receiptKey := s.keys.receipt(batchID)
	deadlineKey := s.keys.deadline()
	var snapshot pendingSnapshot
	pendingRaw, pendingErr := s.rdb.Get(ctx, pendingKey).Result()
	if pendingErr != nil && pendingErr != redis.Nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: read pending: %v", errSettle, pendingErr)
	}
	resultRecords := make([]resultRecord, 0)
	retryTasks := make([]Task, 0)
	deadRecords := make([]deadRecord, 0)
	if pendingErr == nil {
		if err := json.Unmarshal([]byte(pendingRaw), &snapshot); err != nil {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: decode pending: %v", errSettle, err)
		}
		if err := snapshot.validate(); err != nil {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: pending: %v", errSettle, err)
		}
		if snapshot.BatchID != batchID {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: pending batch id mismatch", errSettle)
		}
		resultRecords, retryTasks, deadRecords, err = buildOutcomeEffects(snapshot, outcome)
		if err != nil {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: %v", errSettle, err)
		}
	}

	resultKey := s.keys.result("unknown")
	sourceKey := ""
	deadKey := s.keys.deadFor("unknown")
	if pendingErr == nil {
		sourceKey = snapshot.Queue.Source
		resultKey = snapshot.Queue.Result
		if resultKey == "" {
			resultKey = s.keys.result(snapshot.Queue.TaskName)
		}
		deadKey = snapshot.Queue.Dead
		if deadKey == "" {
			deadKey = s.keys.deadFor(snapshot.Queue.TaskName)
		}
	}
	receiptJSON, err := json.Marshal(storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       batchID,
		RequestID:     outcome.RequestID,
		CommandHash:   command,
		Winner:        "settle",
		ResultCount:   len(resultRecords),
		RetryCount:    len(retryTasks),
		DeadCount:     len(deadRecords),
	})
	if err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: encode receipt: %v", errSettle, err)
	}
	resultJSON, _ := json.Marshal(resultRecords)
	retryJSON, _ := json.Marshal(retryTasks)
	deadJSON, _ := json.Marshal(deadRecords)
	value, err := settleScript.Eval(ctx, s.rdb, []string{pendingKey, receiptKey, deadlineKey, resultKey, sourceKey, deadKey},
		batchID, outcome.RequestID, command, string(receiptJSON), string(resultJSON), string(retryJSON), string(deadJSON), s.config.ReceiptTTL.Milliseconds()).Result()
	if err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: Redis script: %v", errSettle, err)
	}
	return parseReceiptResponse(batchID, value, errSettle)
}

// validateOutcomeResultSizes enforces the same byte limits Dispatch applies to
// task payloads: each Result.Data must fit within maxPayloadBytes and the sum
// of all Result.Data in the outcome must fit within maxBatchBytes. Sizes are
// measured as the raw JSON bytes of Result.Data, and the total is accumulated
// with a remaining-budget that can never overflow. Failure entries are not
// counted because they reference payloads already limited at Dispatch time.
func validateOutcomeResultSizes(maxPayloadBytes, maxBatchBytes int, outcome Outcome) error {
	remaining := maxBatchBytes
	for _, result := range outcome.Results {
		size := len(result.Data)
		if size > maxPayloadBytes {
			return fmt.Errorf("%w: result task_id %q is %d bytes, exceeds max_payload_bytes %d", errResultTooLarge, result.TaskID, size, maxPayloadBytes)
		}
		if size > remaining {
			return fmt.Errorf("%w: result data for outcome totals %d bytes, exceeds max_batch_bytes %d", errResultTooLarge, maxBatchBytes-remaining+size, maxBatchBytes)
		}
		remaining -= size
	}
	return nil
}

func buildOutcomeEffects(snapshot pendingSnapshot, outcome Outcome) ([]resultRecord, []Task, []deadRecord, error) {
	tasks := make(map[string]Task, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		tasks[task.TaskID] = task
	}
	seen := make(map[string]struct{}, len(outcome.Results)+len(outcome.Failures))
	results := make([]resultRecord, 0, len(outcome.Results))
	for _, result := range outcome.Results {
		task, ok := tasks[result.TaskID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("unknown result task_id %q", result.TaskID)
		}
		seen[result.TaskID] = struct{}{}
		results = append(results, resultRecord{
			SchemaVersion: protocolVersion,
			Protocol:      protocolVersion,
			EffectID:      effectID("result", snapshot.BatchID, result.TaskID, task.RetryCount),
			TaskID:        result.TaskID,
			BatchID:       snapshot.BatchID,
			Data:          result.Data,
		})
	}
	retries := make([]Task, 0, len(outcome.Failures))
	dead := make([]deadRecord, 0, len(outcome.Failures))
	for _, failure := range outcome.Failures {
		task, ok := tasks[failure.TaskID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("unknown failure task_id %q", failure.TaskID)
		}
		seen[failure.TaskID] = struct{}{}
		if failure.Retryable && task.RetryCount < snapshot.MaxRetry {
			task.RetryCount++
			retries = append(retries, task)
			continue
		}
		dead = append(dead, deadRecord{
			SchemaVersion: protocolVersion,
			Protocol:      protocolVersion,
			EffectID:      effectID("dead", snapshot.BatchID, failure.TaskID, task.RetryCount),
			TaskID:        failure.TaskID,
			BatchID:       snapshot.BatchID,
			RetryCount:    task.RetryCount,
			Reason:        failure.Reason,
			Payload:       task.Payload,
		})
	}
	if len(seen) != len(tasks) {
		return nil, nil, nil, fmt.Errorf("outcome covers %d of %d pending tasks", len(seen), len(tasks))
	}
	sort.Slice(results, func(i, j int) bool { return results[i].TaskID < results[j].TaskID })
	sort.Slice(retries, func(i, j int) bool { return retries[i].TaskID < retries[j].TaskID })
	sort.Slice(dead, func(i, j int) bool { return dead[i].TaskID < dead[j].TaskID })
	return results, retries, dead, nil
}

func parseReceiptResponse(batchID string, value interface{}, operationErr error) (Receipt, error) {
	parts, ok := value.([]interface{})
	if !ok || len(parts) == 0 {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: malformed script response", operationErr)
	}
	statusText, err := scriptString(parts[0])
	if err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: status: %v", operationErr, err)
	}
	status := ReceiptStatus(statusText)
	receipt := Receipt{Status: status, BatchID: batchID}
	switch status {
	case ReceiptApplied, ReceiptDuplicate, ReceiptConflict, ReceiptStale:
		if len(parts) < 2 {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: receipt is missing", operationErr)
		}
		raw, rawErr := scriptString(parts[1])
		if rawErr != nil || raw == "" {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: receipt response: %v", operationErr, rawErr)
		}
		stored, decodeErr := decodeStoredReceipt(raw)
		if decodeErr != nil {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: receipt: %v", operationErr, decodeErr)
		}
		receipt = publicReceipt(stored, status)
	case ReceiptNotFound, ReceiptNotDue:
		// These statuses intentionally carry no stored receipt.
	case ReceiptInvalid:
		// The error text is handled below.
	default:
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: unknown script status %q", operationErr, status)
	}
	if status == ReceiptInvalid {
		message := "script rejected command"
		if len(parts) > 1 {
			message, _ = scriptString(parts[1])
		}
		return receipt, fmt.Errorf("%w: %s", operationErr, message)
	}
	return receipt, nil
}
