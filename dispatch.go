package echoqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

//go:embed scripts/dispatch.lua
var dispatchScript string

type dispatchBase struct {
	SchemaVersion int         `json:"schema_version"`
	Protocol      int         `json:"protocol_version"`
	State         string      `json:"state"`
	BatchID       string      `json:"batch_id"`
	Queue         QueueConfig `json:"queue"`
	MaxRetry      int         `json:"max_retry"`
}

// Dispatch atomically snapshots and removes up to batchSize tasks. An empty
// source queue returns a zero Batch and nil error.
func (q *Queue) Dispatch(ctx context.Context, batchSize int) (Batch, error) {
	if err := q.validate(); err != nil {
		return Batch{}, err
	}
	if ctx == nil {
		return Batch{}, fmt.Errorf("echoqueue: context is nil")
	}
	if batchSize <= 0 || batchSize > q.settings.MaxBatchSize {
		return Batch{}, fmt.Errorf("echoqueue: batch size %d exceeds limit %d", batchSize, q.settings.MaxBatchSize)
	}
	if err := q.scheduler.ensureRedis(ctx); err != nil {
		return Batch{}, err
	}

	batchID := uuid.NewString()
	base, err := json.Marshal(dispatchBase{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		State:         pendingState,
		BatchID:       batchID,
		Queue:         q.config,
		MaxRetry:      q.settings.MaxRetry,
	})
	if err != nil {
		return Batch{}, fmt.Errorf("echoqueue: encode pending base: %w", err)
	}
	pendingKey := q.scheduler.keys.pending(batchID)
	deadlineKey := q.scheduler.keys.deadline()
	value, err := q.scheduler.rdb.Eval(ctx, dispatchScript, []string{q.config.Source, pendingKey, deadlineKey},
		batchSize, batchID, string(base), q.settings.VisibilityTimeout.Milliseconds(), q.settings.MaxPayloadBytes, q.settings.MaxBatchBytes, q.settings.MaxBatchSize).Result()
	if err != nil {
		return Batch{}, fmt.Errorf("echoqueue: dispatch: %w", err)
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) == 0 {
		return Batch{}, fmt.Errorf("echoqueue: dispatch returned malformed response")
	}
	status, err := scriptString(parts[0])
	if err != nil {
		return Batch{}, err
	}
	if status == "empty" {
		return Batch{}, nil
	}
	if status != "applied" || len(parts) < 5 {
		message := "unknown dispatch response"
		if len(parts) > 1 {
			message, _ = scriptString(parts[1])
		}
		return Batch{}, fmt.Errorf("echoqueue: dispatch rejected: %s", message)
	}
	returnedID, err := scriptString(parts[1])
	if err != nil || returnedID != batchID {
		return Batch{}, fmt.Errorf("echoqueue: dispatch batch id mismatch")
	}
	createdAt, err := scriptInt64(parts[2])
	if err != nil {
		return Batch{}, fmt.Errorf("echoqueue: dispatch created_at: %w", err)
	}
	deadlineAt, err := scriptInt64(parts[3])
	if err != nil {
		return Batch{}, fmt.Errorf("echoqueue: dispatch deadline_at: %w", err)
	}
	count, err := scriptInt64(parts[4])
	if err != nil || count < 1 || count > int64(q.settings.MaxBatchSize) || int64(len(parts)-5) != count {
		return Batch{}, fmt.Errorf("echoqueue: dispatch task count is invalid")
	}
	tasks := make([]Task, 0, count)
	for i := 5; i < len(parts); i++ {
		raw, err := scriptString(parts[i])
		if err != nil {
			return Batch{}, fmt.Errorf("echoqueue: dispatch task %d: %w", i-5, err)
		}
		var task Task
		if err := json.Unmarshal([]byte(raw), &task); err != nil {
			return Batch{}, fmt.Errorf("echoqueue: decode task %d: %w", i-5, err)
		}
		if err := task.validate(); err != nil {
			return Batch{}, err
		}
		tasks = append(tasks, task)
	}
	return Batch{
		ID:         batchID,
		Tasks:      tasks,
		CreatedAt:  time.UnixMilli(createdAt).UTC(),
		DeadlineAt: time.UnixMilli(deadlineAt).UTC(),
	}, nil
}

func scriptString(value interface{}) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("echoqueue: unexpected Redis script value %T", value)
	}
}

func scriptInt64(value interface{}) (int64, error) {
	text, err := scriptString(value)
	if err == nil {
		var result int64
		if _, scanErr := fmt.Sscan(text, &result); scanErr != nil {
			return 0, scanErr
		}
		return result, nil
	}
	if number, ok := value.(int64); ok {
		return number, nil
	}
	return 0, err
}
