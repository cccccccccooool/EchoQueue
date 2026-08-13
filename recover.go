package echoqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/recover.lua
var recoverScriptSource string

// recoverScript runs the embedded script through EVALSHA with an automatic
// EVAL fallback, so the recovery loop never resends the script body.
var recoverScript = redis.NewScript(recoverScriptSource)

var errRecover = errors.New("echoqueue: recover failed")

// Run is the scheduler lifecycle. It scans the bounded deadline index and
// performs the same atomic recovery command used by the Settle race fence.
// Context cancellation is returned to the caller; a Redis error during one
// candidate does not delete evidence or stop later candidates.
func (s *Scheduler) Run(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: scheduler is nil", errRecover)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", errRecover)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.beginRun() {
		return ErrRunAlreadyActive
	}
	defer s.endRun()
	if err := s.ensureRedis(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(s.config.RunInterval)
	defer ticker.Stop()
	for {
		if err := s.recoverExpired(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) recoverExpired(ctx context.Context) error {
	now, err := serverMillis(ctx, s.rdb)
	if err != nil {
		return err
	}
	ids, err := s.rdb.ZRangeByScore(ctx, s.keys.deadline(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now, 10),
		Offset: 0,
		Count:  int64(s.config.RunBatchSize),
	}).Result()
	if err != nil {
		return err
	}
	var firstErr error
	for _, batchID := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, recoverErr := s.recoverBatch(ctx, batchID); recoverErr == nil {
			continue
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			_, deferErr := s.deferRecover(ctx, batchID)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			batchErr := recoverErr
			if deferErr != nil {
				batchErr = errors.Join(recoverErr, deferErr)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("echoqueue: recover batch %q: %w", batchID, batchErr)
			}
		}
	}
	return firstErr
}

func (s *Scheduler) recoverBatch(ctx context.Context, batchID string) (Receipt, error) {
	if s == nil || s.rdb == nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: scheduler is nil", errRecover)
	}
	if err := validateText("batch_id", batchID, true); err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: %v", errRecover, err)
	}
	if err := s.ensureRedis(ctx); err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, err
	}
	pendingKey := s.keys.pending(batchID)
	receiptKey := s.keys.receipt(batchID)
	deadlineKey := s.keys.deadline()
	pendingRaw, pendingErr := s.rdb.Get(ctx, pendingKey).Result()
	if pendingErr != nil && pendingErr != redis.Nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: read pending: %v", errRecover, pendingErr)
	}
	retryTasks := make([]Task, 0)
	deadRecords := make([]deadRecord, 0)
	sourceKey := ""
	deadKey := ""
	if pendingErr == nil {
		var snapshot pendingSnapshot
		if err := json.Unmarshal([]byte(pendingRaw), &snapshot); err != nil {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: decode pending: %v", errRecover, err)
		}
		if err := snapshot.validate(); err != nil {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: pending: %v", errRecover, err)
		}
		if snapshot.BatchID != batchID {
			return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: pending batch id mismatch", errRecover)
		}
		retryTasks, deadRecords = buildRecoveryEffects(snapshot)
		sourceKey = snapshot.Queue.Source
		deadKey = snapshot.Queue.Dead
		if deadKey == "" {
			deadKey = s.keys.deadFor(snapshot.Queue.TaskName)
		}
	}
	recoverRequest := "recover:" + batchID
	recoverHash := recoverCommandHash(batchID)
	receiptJSON, err := json.Marshal(storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       batchID,
		RequestID:     recoverRequest,
		CommandHash:   recoverHash,
		Winner:        "recover",
		RetryCount:    len(retryTasks),
		DeadCount:     len(deadRecords),
	})
	if err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: encode receipt: %v", errRecover, err)
	}
	retryJSON, _ := json.Marshal(retryTasks)
	deadJSON, _ := json.Marshal(deadRecords)
	value, err := recoverScript.Eval(ctx, s.rdb, []string{pendingKey, receiptKey, deadlineKey, sourceKey, deadKey},
		batchID, recoverRequest, recoverHash, string(receiptJSON), string(retryJSON), string(deadJSON), s.config.ReceiptTTL.Milliseconds()).Result()
	if err != nil {
		return Receipt{Status: ReceiptInvalid, BatchID: batchID}, fmt.Errorf("%w: Redis script: %v", errRecover, err)
	}
	return parseReceiptResponse(batchID, value, errRecover)
}

func buildRecoveryEffects(snapshot pendingSnapshot) ([]Task, []deadRecord) {
	retries := make([]Task, 0, len(snapshot.Tasks))
	dead := make([]deadRecord, 0, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		if task.RetryCount < snapshot.MaxRetry {
			task.RetryCount++
			retries = append(retries, task)
			continue
		}
		dead = append(dead, deadRecord{
			SchemaVersion: protocolVersion,
			Protocol:      protocolVersion,
			EffectID:      effectID("dead", snapshot.BatchID, task.TaskID, task.RetryCount),
			TaskID:        task.TaskID,
			BatchID:       snapshot.BatchID,
			RetryCount:    task.RetryCount,
			Reason:        "visibility timeout",
			Payload:       task.Payload,
		})
	}
	return retries, dead
}
