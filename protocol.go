package echoqueue

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	protocolVersion = 1
	pendingState    = "PENDING"
)

// Task is the stable unit moved through EchoQueue. TaskID must remain the
// same when a task is retried; business side effects still need TaskID-based
// idempotency because the delivery guarantee is At-Least-Once.
type Task struct {
	TaskID     string          `json:"task_id"`
	RetryCount int             `json:"retry_count"`
	Payload    json.RawMessage `json:"payload"`
}

// Batch is the immutable dispatch result. A zero Batch means the source queue
// was empty.
type Batch struct {
	ID         string    `json:"batch_id"`
	Tasks      []Task    `json:"tasks"`
	CreatedAt  time.Time `json:"created_at"`
	DeadlineAt time.Time `json:"deadline_at"`
}

type Result struct {
	TaskID string          `json:"task_id"`
	Data   json.RawMessage `json:"data"`
}

type Failure struct {
	TaskID    string `json:"task_id"`
	Reason    string `json:"reason,omitempty"`
	Retryable bool   `json:"retryable"`
}

// Outcome is the worker response. Every dispatched TaskID must occur exactly
// once in Results or Failures. RequestID is the caller's stable retry token.
type Outcome struct {
	RequestID string    `json:"request_id"`
	Results   []Result  `json:"results"`
	Failures  []Failure `json:"failures"`
}

type ReceiptStatus string

const (
	ReceiptApplied   ReceiptStatus = "applied"
	ReceiptDuplicate ReceiptStatus = "duplicate"
	ReceiptConflict  ReceiptStatus = "conflict"
	ReceiptStale     ReceiptStatus = "stale"
	ReceiptNotFound  ReceiptStatus = "not_found"
	ReceiptNotDue    ReceiptStatus = "not_due"
	ReceiptInvalid   ReceiptStatus = "invalid"
)

// Receipt is the public terminal record and the status of the current call.
// CommandHash is intentionally not exposed; it is an internal replay fence.
type Receipt struct {
	Status      ReceiptStatus `json:"status"`
	BatchID     string        `json:"batch_id"`
	RequestID   string        `json:"request_id"`
	Winner      string        `json:"winner"`
	ClosedAt    time.Time     `json:"closed_at"`
	ResultCount int           `json:"result_count"`
	RetryCount  int           `json:"retry_count"`
	DeadCount   int           `json:"dead_count"`
}

type pendingSnapshot struct {
	SchemaVersion int         `json:"schema_version"`
	Protocol      int         `json:"protocol_version"`
	State         string      `json:"state"`
	BatchID       string      `json:"batch_id"`
	Queue         QueueConfig `json:"queue"`
	MaxRetry      int         `json:"max_retry"`
	CreatedAt     int64       `json:"created_at"`
	DeadlineAt    int64       `json:"deadline_at"`
	Tasks         []Task      `json:"tasks"`
}

type storedReceipt struct {
	SchemaVersion int    `json:"schema_version"`
	Protocol      int    `json:"protocol_version"`
	BatchID       string `json:"batch_id"`
	RequestID     string `json:"request_id"`
	CommandHash   string `json:"command_hash"`
	Winner        string `json:"winner"`
	ClosedAt      int64  `json:"closed_at"`
	ResultCount   int    `json:"result_count"`
	RetryCount    int    `json:"retry_count"`
	DeadCount     int    `json:"dead_count"`
}

type resultRecord struct {
	SchemaVersion int             `json:"schema_version"`
	Protocol      int             `json:"protocol_version"`
	EffectID      string          `json:"effect_id"`
	TaskID        string          `json:"task_id"`
	BatchID       string          `json:"batch_id"`
	Data          json.RawMessage `json:"data"`
}

type deadRecord struct {
	SchemaVersion int             `json:"schema_version"`
	Protocol      int             `json:"protocol_version"`
	EffectID      string          `json:"effect_id"`
	TaskID        string          `json:"task_id"`
	BatchID       string          `json:"batch_id"`
	RetryCount    int             `json:"retry_count"`
	Reason        string          `json:"reason,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

func (t Task) validate() error {
	if err := validateText("task_id", t.TaskID, true); err != nil {
		return err
	}
	if t.RetryCount < 0 {
		return fmt.Errorf("task %q has negative retry_count", t.TaskID)
	}
	if len(t.Payload) == 0 || !json.Valid(t.Payload) {
		return fmt.Errorf("task %q has invalid payload JSON", t.TaskID)
	}
	return nil
}

func (o Outcome) validate() error {
	if err := validateText("request_id", o.RequestID, true); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(o.Results)+len(o.Failures))
	for _, result := range o.Results {
		if err := validateText("result.task_id", result.TaskID, true); err != nil {
			return err
		}
		if len(result.Data) == 0 || !json.Valid(result.Data) {
			return fmt.Errorf("result %q has invalid data JSON", result.TaskID)
		}
		if _, exists := seen[result.TaskID]; exists {
			return fmt.Errorf("task_id %q appears more than once", result.TaskID)
		}
		seen[result.TaskID] = struct{}{}
	}
	for _, failure := range o.Failures {
		if err := validateText("failure.task_id", failure.TaskID, true); err != nil {
			return err
		}
		if err := validateText("failure.reason", failure.Reason, false); err != nil {
			return err
		}
		if _, exists := seen[failure.TaskID]; exists {
			return fmt.Errorf("task_id %q appears more than once", failure.TaskID)
		}
		seen[failure.TaskID] = struct{}{}
	}
	if len(seen) == 0 {
		return fmt.Errorf("outcome must contain at least one result or failure")
	}
	return nil
}

func (p pendingSnapshot) validate() error {
	if p.SchemaVersion != protocolVersion || p.Protocol != protocolVersion {
		return fmt.Errorf("unsupported pending protocol")
	}
	if p.State != pendingState {
		return fmt.Errorf("pending batch is not open")
	}
	if err := validateText("batch_id", p.BatchID, true); err != nil {
		return err
	}
	if _, err := p.Queue.normalized(); err != nil {
		return fmt.Errorf("pending queue: %w", err)
	}
	if p.MaxRetry < 0 || p.CreatedAt <= 0 || p.DeadlineAt <= p.CreatedAt {
		return fmt.Errorf("pending timing or retry policy is invalid")
	}
	if len(p.Tasks) == 0 {
		return fmt.Errorf("pending task list is empty")
	}
	seen := make(map[string]struct{}, len(p.Tasks))
	for _, task := range p.Tasks {
		if err := task.validate(); err != nil {
			return err
		}
		if _, exists := seen[task.TaskID]; exists {
			return fmt.Errorf("duplicate task_id %q", task.TaskID)
		}
		seen[task.TaskID] = struct{}{}
	}
	return nil
}

func publicReceipt(value storedReceipt, status ReceiptStatus) Receipt {
	result := Receipt{
		Status:      status,
		BatchID:     value.BatchID,
		RequestID:   value.RequestID,
		Winner:      value.Winner,
		ResultCount: value.ResultCount,
		RetryCount:  value.RetryCount,
		DeadCount:   value.DeadCount,
	}
	if value.ClosedAt > 0 {
		result.ClosedAt = time.UnixMilli(value.ClosedAt).UTC()
	}
	return result
}

func decodeStoredReceipt(raw string) (storedReceipt, error) {
	var value storedReceipt
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return storedReceipt{}, err
	}
	if value.SchemaVersion != protocolVersion || value.Protocol != protocolVersion || value.BatchID == "" || value.RequestID == "" || value.CommandHash == "" || value.Winner == "" || value.ClosedAt <= 0 {
		return storedReceipt{}, fmt.Errorf("invalid receipt")
	}
	return value, nil
}
