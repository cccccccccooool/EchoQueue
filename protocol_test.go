package echoqueue

import (
	"encoding/json"
	"testing"
)

func validSnapshot(maxRetry int) pendingSnapshot {
	return pendingSnapshot{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		State:         pendingState,
		BatchID:       "batch-1",
		Queue:         QueueConfig{TaskName: "email", Source: "source"},
		MaxRetry:      maxRetry,
		CreatedAt:     1000,
		DeadlineAt:    2000,
		Tasks: []Task{
			{TaskID: "task-1", RetryCount: 0, Payload: json.RawMessage(`{"value":1}`)},
			{TaskID: "task-2", RetryCount: 1, Payload: json.RawMessage(`{"value":2}`)},
		},
	}
}

func TestOutcomeEffectsHonorMaxRetryZero(t *testing.T) {
	snapshot := validSnapshot(0)
	outcome := Outcome{
		RequestID: "request-1",
		Failures: []Failure{
			{TaskID: "task-1", Reason: "timeout", Retryable: true},
			{TaskID: "task-2", Reason: "bad input", Retryable: false},
		},
	}
	results, retries, dead, err := buildOutcomeEffects(snapshot, outcome)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 || len(retries) != 0 || len(dead) != 2 {
		t.Fatalf("effects = results=%d retries=%d dead=%d", len(results), len(retries), len(dead))
	}
	for _, record := range dead {
		if record.TaskID == "" || record.EffectID == "" {
			t.Fatalf("dead record lacks stable identity: %+v", record)
		}
	}
}

func TestRecoveryEffectsIncrementStableTaskIDs(t *testing.T) {
	snapshot := validSnapshot(2)
	retries, dead := buildRecoveryEffects(snapshot)
	if len(retries) != 2 || len(dead) != 0 {
		t.Fatalf("effects = retries=%d dead=%d", len(retries), len(dead))
	}
	if retries[0].TaskID != "task-1" || retries[0].RetryCount != 1 || retries[1].TaskID != "task-2" || retries[1].RetryCount != 2 {
		t.Fatalf("retry tasks = %+v", retries)
	}
}

func TestRecoverHashIsDeterministic(t *testing.T) {
	if recoverCommandHash("batch-1") != recoverCommandHash("batch-1") {
		t.Fatal("recover hash is not stable")
	}
	if recoverCommandHash("batch-1") == recoverCommandHash("batch-2") {
		t.Fatal("different batches share recover hash")
	}
}
