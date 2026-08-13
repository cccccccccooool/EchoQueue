package echoqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

func TestOutcomeEffectsCoverResultRetryAndDead(t *testing.T) {
	snapshot := pendingSnapshot{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		State:         pendingState,
		BatchID:       "batch-effects",
		Queue:         QueueConfig{TaskName: "email", Source: "source"},
		MaxRetry:      2,
		CreatedAt:     1000,
		DeadlineAt:    2000,
		Tasks: []Task{
			{TaskID: "task-result", RetryCount: 0, Payload: json.RawMessage(`{"value":1}`)},
			{TaskID: "task-retry", RetryCount: 1, Payload: json.RawMessage(`{"value":2}`)},
			{TaskID: "task-dead", RetryCount: 2, Payload: json.RawMessage(`{"value":3}`)},
		},
	}
	outcome := Outcome{
		RequestID: "request-effects",
		Results:   []Result{{TaskID: "task-result", Data: json.RawMessage(`{"ok":true}`)}},
		Failures: []Failure{
			{TaskID: "task-retry", Reason: "temporary", Retryable: true},
			{TaskID: "task-dead", Reason: "permanent", Retryable: false},
		},
	}
	results, retries, dead, err := buildOutcomeEffects(snapshot, outcome)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || len(retries) != 1 || len(dead) != 1 {
		t.Fatalf("effects = results=%d retries=%d dead=%d", len(results), len(retries), len(dead))
	}
	if results[0].TaskID != "task-result" || results[0].BatchID != snapshot.BatchID || results[0].EffectID == "" {
		t.Fatalf("result identity = %+v", results[0])
	}
	if retries[0].TaskID != "task-retry" || retries[0].RetryCount != 2 {
		t.Fatalf("retry effect = %+v", retries[0])
	}
	if dead[0].TaskID != "task-dead" || dead[0].RetryCount != 2 || dead[0].BatchID != snapshot.BatchID || dead[0].EffectID == "" {
		t.Fatalf("dead effect = %+v", dead[0])
	}
}

func TestOutcomeEffectsRejectUnknownAndIncompleteTasks(t *testing.T) {
	snapshot := validSnapshot(1)
	cases := []struct {
		name    string
		outcome Outcome
	}{
		{name: "unknown result", outcome: Outcome{Results: []Result{{TaskID: "unknown", Data: json.RawMessage(`true`)}}}},
		{name: "unknown failure", outcome: Outcome{Failures: []Failure{{TaskID: "unknown", Reason: "bad"}}}},
		{name: "incomplete", outcome: Outcome{Results: []Result{{TaskID: "task-1", Data: json.RawMessage(`true`)}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := buildOutcomeEffects(snapshot, tc.outcome); err == nil {
				t.Fatal("invalid outcome was accepted")
			}
		})
	}
}

func TestTaskValidationMatrix(t *testing.T) {
	tests := []struct {
		name  string
		task  Task
		valid bool
	}{
		{name: "object payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`{"a":1}`)}, valid: true},
		{name: "array payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`[1,2]`)}, valid: true},
		{name: "number payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`42`)}, valid: true},
		{name: "string payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`"hello"`)}, valid: true},
		{name: "boolean payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`true`)}, valid: true},
		{name: "null payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`null`)}, valid: true},
		{name: "empty payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(``)}, valid: false},
		{name: "whitespace payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`  `)}, valid: false},
		{name: "invalid json payload", task: Task{TaskID: "task-1", Payload: json.RawMessage(`{"a":`)}, valid: false},
		{name: "negative retry count", task: Task{TaskID: "task-1", RetryCount: -1, Payload: json.RawMessage(`{}`)}, valid: false},
		{name: "empty task id", task: Task{TaskID: "", Payload: json.RawMessage(`{}`)}, valid: false},
		{name: "blank task id", task: Task{TaskID: "   ", Payload: json.RawMessage(`{}`)}, valid: false},
		{name: "control task id", task: Task{TaskID: "task\x03", Payload: json.RawMessage(`{}`)}, valid: false},
		{name: "unicode task id", task: Task{TaskID: "任务-1", Payload: json.RawMessage(`{}`)}, valid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.task.validate()
			if tc.valid && err != nil {
				t.Fatalf("valid task rejected: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatal("invalid task accepted")
			}
		})
	}
}

func TestOutcomeValidationMatrix(t *testing.T) {
	invalid := []struct {
		name    string
		outcome Outcome
	}{
		{name: "empty request id", outcome: Outcome{RequestID: "", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{}`)}}}},
		{name: "control request id", outcome: Outcome{RequestID: "r\x04", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{}`)}}}},
		{name: "no results or failures", outcome: Outcome{RequestID: "r"}},
		{name: "empty result data", outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(``)}}}},
		{name: "invalid result data", outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{`)}}}},
		{name: "duplicate result task", outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{}`)}, {TaskID: "t", Data: json.RawMessage(`{}`)}}}},
		{name: "duplicate failure task", outcome: Outcome{RequestID: "r", Failures: []Failure{{TaskID: "t", Reason: "a"}, {TaskID: "t", Reason: "b"}}}},
		{name: "result and failure same task", outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{}`)}}, Failures: []Failure{{TaskID: "t", Reason: "b"}}}},
		{name: "control failure task id", outcome: Outcome{RequestID: "r", Failures: []Failure{{TaskID: "t\x05"}}}},
		{name: "control failure reason", outcome: Outcome{RequestID: "r", Failures: []Failure{{TaskID: "t", Reason: "bad\x06"}}}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.outcome.validate(); err == nil {
				t.Fatal("invalid outcome accepted")
			}
		})
	}
	valid := []Outcome{
		{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`null`)}}},
		{RequestID: "r", Failures: []Failure{{TaskID: "t"}}},
		{RequestID: "r", Failures: []Failure{{TaskID: "t", Reason: "unicode-理由"}}},
		{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{"ok":true}`)}}, Failures: []Failure{{TaskID: "u", Reason: "x"}}},
	}
	for i, outcome := range valid {
		if err := outcome.validate(); err != nil {
			t.Fatalf("valid outcome %d rejected: %v", i, err)
		}
	}
}

func TestPendingSnapshotValidationMatrix(t *testing.T) {
	base := validSnapshot(2)
	invalid := []struct {
		name   string
		mutate func(*pendingSnapshot)
	}{
		{name: "schema version", mutate: func(p *pendingSnapshot) { p.SchemaVersion = 999 }},
		{name: "protocol version", mutate: func(p *pendingSnapshot) { p.Protocol = 999 }},
		{name: "state", mutate: func(p *pendingSnapshot) { p.State = "CLOSED" }},
		{name: "empty state", mutate: func(p *pendingSnapshot) { p.State = "" }},
		{name: "batch id", mutate: func(p *pendingSnapshot) { p.BatchID = "" }},
		{name: "queue task name", mutate: func(p *pendingSnapshot) { p.Queue.TaskName = "" }},
		{name: "queue source", mutate: func(p *pendingSnapshot) { p.Queue.Source = "" }},
		{name: "negative max retry", mutate: func(p *pendingSnapshot) { p.MaxRetry = -1 }},
		{name: "zero created at", mutate: func(p *pendingSnapshot) { p.CreatedAt = 0 }},
		{name: "negative created at", mutate: func(p *pendingSnapshot) { p.CreatedAt = -5 }},
		{name: "deadline equals created", mutate: func(p *pendingSnapshot) { p.DeadlineAt = p.CreatedAt }},
		{name: "deadline before created", mutate: func(p *pendingSnapshot) { p.DeadlineAt = p.CreatedAt - 1 }},
		{name: "zero deadline", mutate: func(p *pendingSnapshot) { p.DeadlineAt = 0 }},
		{name: "empty tasks", mutate: func(p *pendingSnapshot) { p.Tasks = nil }},
		{name: "duplicate task ids", mutate: func(p *pendingSnapshot) {
			p.Tasks = []Task{{TaskID: "dup", Payload: json.RawMessage(`{}`)}, {TaskID: "dup", Payload: json.RawMessage(`{}`)}}
		}},
		{name: "corrupt task", mutate: func(p *pendingSnapshot) { p.Tasks[0].Payload = json.RawMessage(`not json`) }},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := base
			snapshot.Tasks = append([]Task(nil), base.Tasks...)
			tc.mutate(&snapshot)
			if err := snapshot.validate(); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
	if err := base.validate(); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	single := validSnapshot(0)
	single.Tasks = single.Tasks[:1]
	if err := single.validate(); err != nil {
		t.Fatalf("single task snapshot rejected: %v", err)
	}
}

func TestDecodeStoredReceiptMatrix(t *testing.T) {
	valid := storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       "b",
		RequestID:     "r",
		CommandHash:   "h",
		Winner:        "settle",
		ClosedAt:      100,
		ResultCount:   1,
		RetryCount:    2,
		DeadCount:     3,
	}
	raw, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStoredReceipt(string(raw))
	if err != nil {
		t.Fatalf("valid receipt rejected: %v", err)
	}
	if decoded.ResultCount != 1 || decoded.RetryCount != 2 || decoded.DeadCount != 3 {
		t.Fatalf("counts = %+v", decoded)
	}
	invalid := []string{
		"not json",
		`{}`,
		`{"schema_version":2,"protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":100}`,
		`{"schema_version":1,"protocol_version":2,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":100}`,
		`{"schema_version":1,"protocol_version":1,"request_id":"r","command_hash":"h","winner":"settle","closed_at":100}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","command_hash":"h","winner":"settle","closed_at":100}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","request_id":"r","winner":"settle","closed_at":100}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","closed_at":100}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":0}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":-1}`,
		`{"schema_version":"1","protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":100}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":1.5}`,
		`{"schema_version":1,"protocol_version":1,"batch_id":"b","request_id":"r","command_hash":"h","winner":"settle","closed_at":99999999999999999999999999}`,
	}
	for _, tc := range invalid {
		if _, err := decodeStoredReceipt(tc); err == nil {
			t.Fatalf("invalid receipt accepted: %s", tc)
		}
	}
}

func TestPublicReceiptConvertsUTCAndZeroTime(t *testing.T) {
	value := storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       "b",
		RequestID:     "r",
		CommandHash:   "h",
		Winner:        "recover",
		ClosedAt:      1720000000123,
		ResultCount:   4,
		RetryCount:    5,
		DeadCount:     6,
	}
	receipt := publicReceipt(value, ReceiptDuplicate)
	if receipt.Status != ReceiptDuplicate || receipt.BatchID != "b" || receipt.RequestID != "r" || receipt.Winner != "recover" || receipt.ResultCount != 4 || receipt.RetryCount != 5 || receipt.DeadCount != 6 {
		t.Fatalf("receipt = %+v", receipt)
	}
	if receipt.ClosedAt.IsZero() || receipt.ClosedAt.Location() != time.UTC {
		t.Fatalf("closed_at = %v, want UTC", receipt.ClosedAt)
	}
	zero := publicReceipt(storedReceipt{BatchID: "b", ClosedAt: 0}, ReceiptApplied)
	if !zero.ClosedAt.IsZero() {
		t.Fatalf("zero closed_at became %v", zero.ClosedAt)
	}
}

func TestScriptStringAndScriptInt64Types(t *testing.T) {
	text, err := scriptString("value")
	if err != nil || text != "value" {
		t.Fatalf("string value = %q, err=%v", text, err)
	}
	text, err = scriptString([]byte("bytes"))
	if err != nil || text != "bytes" {
		t.Fatalf("bytes value = %q, err=%v", text, err)
	}
	for _, value := range []interface{}{nil, int64(1), 1, 1.5, []interface{}{"x"}} {
		if _, err := scriptString(value); err == nil {
			t.Fatalf("scriptString accepted %T", value)
		}
	}

	number, err := scriptInt64(int64(42))
	if err != nil || number != 42 {
		t.Fatalf("int64 value = %d, err=%v", number, err)
	}
	number, err = scriptInt64("123")
	if err != nil || number != 123 {
		t.Fatalf("decimal string = %d, err=%v", number, err)
	}
	number, err = scriptInt64([]byte("456"))
	if err != nil || number != 456 {
		t.Fatalf("decimal bytes = %d, err=%v", number, err)
	}
	number, err = scriptInt64("-7")
	if err != nil || number != -7 {
		t.Fatalf("negative decimal = %d, err=%v", number, err)
	}
	for _, value := range []interface{}{"", "not a number", int(5), int32(5), 3.5, nil} {
		if _, err := scriptInt64(value); err == nil {
			t.Fatalf("scriptInt64 accepted %#v", value)
		}
	}
	overflow := "9223372036854775808"
	if _, err := scriptInt64(overflow); err == nil {
		t.Fatalf("scriptInt64 accepted overflowing %s", overflow)
	}
}

func TestParseReceiptResponseFullStatusMatrix(t *testing.T) {
	operationErr := errors.New("operation")
	stored := storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       "batch-status",
		RequestID:     "request-status",
		CommandHash:   "hash-status",
		Winner:        "settle",
		ClosedAt:      100,
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	statuses := []ReceiptStatus{ReceiptApplied, ReceiptDuplicate, ReceiptConflict, ReceiptStale}
	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			receipt, err := parseReceiptResponse("batch-status", []interface{}{string(status), string(raw)}, operationErr)
			if err != nil || receipt.Status != status || receipt.BatchID != "batch-status" || receipt.Winner != "settle" {
				t.Fatalf("receipt = %+v, err=%v", receipt, err)
			}
		})
	}
	if receipt, err := parseReceiptResponse("batch-status", []interface{}{"not_due"}, operationErr); err != nil || receipt.Status != ReceiptNotDue {
		t.Fatalf("not_due = %+v, err=%v", receipt, err)
	}
	receipt, err := parseReceiptResponse("batch-status", []interface{}{"invalid", "rejected because x"}, operationErr)
	if err == nil || receipt.Status != ReceiptInvalid {
		t.Fatalf("invalid = %+v, err=%v", receipt, err)
	}
	if !strings.Contains(err.Error(), "rejected because x") {
		t.Fatalf("invalid error = %v", err)
	}
	rejected := []interface{}{
		[]interface{}{},
		[]interface{}{"applied"},
		[]interface{}{"applied", ""},
		[]interface{}{"applied", 123},
		[]interface{}{"applied", "{bad"},
		[]interface{}{"applied", string(raw[:len(raw)-1])},
		[]interface{}{"unknown"},
		[]interface{}{"unknown", "extra"},
		"flat-string",
		"applied",
	}
	for _, value := range rejected {
		if _, err := parseReceiptResponse("batch-status", value, operationErr); err == nil {
			t.Fatalf("malformed response accepted: %#v", value)
		}
	}
}

func TestCanonicalJSONMatrix(t *testing.T) {
	canonical := []struct {
		name string
		in   string
		want string
	}{
		{name: "object key order", in: `{"b":1,"a":2}`, want: `{"a":2,"b":1}`},
		{name: "whitespace", in: ` { "a" : 2 } `, want: `{"a":2}`},
		{name: "array preserved", in: `[3,1,2]`, want: `[3,1,2]`},
		{name: "number", in: `42`, want: `42`},
		{name: "string", in: `"x"`, want: `"x"`},
		{name: "null", in: `null`, want: `null`},
	}
	for _, tc := range canonical {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalJSON([]byte(tc.in))
			if err != nil || string(got) != tc.want {
				t.Fatalf("canonicalJSON(%q) = %q, err=%v", tc.in, got, err)
			}
		})
	}
	rejected := []string{
		``,
		`   `,
		`{`,
		`{"a":}`,
		`{"a":1} {"b":2}`,
		`1 2`,
		`"x" trailing`,
		`{"a":1} garbage`,
	}
	for _, tc := range rejected {
		if _, err := canonicalJSON([]byte(tc)); err == nil {
			t.Fatalf("canonicalJSON accepted %q", tc)
		}
	}
	big, err := canonicalJSON([]byte(`{"n":123456789012345678901234567890}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(big), "123456789012345678901234567890") {
		t.Fatalf("large integer precision lost: %s", big)
	}
}

func TestCanonicalJSONIsStableUnderReserialization(t *testing.T) {
	inputs := []string{
		`{"z":9,"a":{"x":[1,2,{"y":null}]},"n":1.5,"s":"text"}`,
		`[{"b":2,"a":1},3,"x",false]`,
		`{"unicode":"中文🚀","n":123456789012345678901234567890}`,
	}
	for _, input := range inputs {
		first, err := canonicalJSON([]byte(input))
		if err != nil {
			t.Fatalf("first canonical: %v", err)
		}
		second, err := canonicalJSON(first)
		if err != nil {
			t.Fatalf("second canonical: %v", err)
		}
		if string(first) != string(second) {
			t.Fatalf("canonical JSON is not idempotent: %q != %q", first, second)
		}
	}
}

func FuzzCanonicalJSONNeverPanics(f *testing.F) {
	seeds := []string{`{"a":1}`, `[1,2]`, `{}`, `null`, `"x"`, `{"a":1} {"b":2}`, `{`, `  `, `{"n":12345678901234567890}`}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		first, err := canonicalJSON(raw)
		if err != nil {
			return
		}
		if !json.Valid(first) {
			t.Fatalf("canonical output is not valid JSON: %q", first)
		}
		second, err := canonicalJSON(first)
		if err != nil || string(second) != string(first) {
			t.Fatalf("canonical JSON is not stable: %q -> %q, err=%v", first, second, err)
		}
	})
}

func FuzzReceiptDecodeNeverPanics(f *testing.F) {
	valid := storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       "b",
		RequestID:     "r",
		CommandHash:   "h",
		Winner:        "settle",
		ClosedAt:      100,
	}
	raw, _ := json.Marshal(valid)
	for _, seed := range []string{string(raw), `{}`, `not json`, `{"closed_at":1.5}`, `{"closed_at":999999999999999999999}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		// decodeStoredReceipt must never panic on arbitrary input.
		_, _ = decodeStoredReceipt(raw)
	})
}

func TestCommandHashSensitivity(t *testing.T) {
	base := Outcome{
		RequestID: "request-hash",
		Results:   []Result{{TaskID: "task-a", Data: json.RawMessage(`{"n":1}`)}},
		Failures:  []Failure{{TaskID: "task-b", Reason: "timeout", Retryable: true}},
	}
	baseHash, err := commandHash("batch-hash", base)
	if err != nil {
		t.Fatal(err)
	}
	singleVary := []struct {
		name       string
		new        Outcome
		mustDiffer bool
	}{
		{name: "request id", new: Outcome{RequestID: "request-hash-2", Results: base.Results, Failures: base.Failures}, mustDiffer: true},
		{name: "result data", new: Outcome{RequestID: base.RequestID, Results: []Result{{TaskID: "task-a", Data: json.RawMessage(`{"n":2}`)}}, Failures: base.Failures}, mustDiffer: true},
		{name: "result json whitespace only", new: Outcome{RequestID: base.RequestID, Results: []Result{{TaskID: "task-a", Data: json.RawMessage(` { "n" : 1 } `)}}, Failures: base.Failures}, mustDiffer: false},
		{name: "failure reason", new: Outcome{RequestID: base.RequestID, Results: base.Results, Failures: []Failure{{TaskID: "task-b", Reason: "timeout-2", Retryable: true}}}, mustDiffer: true},
		{name: "failure retryable", new: Outcome{RequestID: base.RequestID, Results: base.Results, Failures: []Failure{{TaskID: "task-b", Reason: "timeout", Retryable: false}}}, mustDiffer: true},
	}
	// A variant differing only in batchID must be produced by calling
	// commandHash with a different batch.
	differentBatch, err := commandHash("batch-hash-2", base)
	if err != nil {
		t.Fatal(err)
	}
	if differentBatch == baseHash {
		t.Fatal("batch id change did not change the hash")
	}
	for _, tc := range singleVary {
		t.Run(tc.name, func(t *testing.T) {
			hash, err := commandHash("batch-hash", tc.new)
			if err != nil {
				t.Fatal(err)
			}
			if tc.mustDiffer && hash == baseHash {
				t.Fatalf("%s did not change the hash", tc.name)
			}
			if !tc.mustDiffer && hash != baseHash {
				t.Fatalf("%s changed the hash", tc.name)
			}
		})
	}
	twoTaskBase := Outcome{
		RequestID: "r",
		Results: []Result{
			{TaskID: "b", Data: json.RawMessage(`{"n":2}`)},
			{TaskID: "a", Data: json.RawMessage(`{"n":1}`)},
		},
	}
	reordered := Outcome{
		RequestID: "r",
		Results: []Result{
			{TaskID: "a", Data: json.RawMessage(`{"n":1}`)},
			{TaskID: "b", Data: json.RawMessage(`{"n":2}`)},
		},
	}
	orderA, err := commandHash("b", twoTaskBase)
	if err != nil {
		t.Fatal(err)
	}
	orderB, err := commandHash("b", reordered)
	if err != nil {
		t.Fatal(err)
	}
	if orderA != orderB {
		t.Fatal("result order changed the hash")
	}
	arrayOrderHashA, err := commandHash("b", Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`[1,2]`)}}})
	if err != nil {
		t.Fatal(err)
	}
	arrayOrderHashB, err := commandHash("b", Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`[2,1]`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if arrayOrderHashA == arrayOrderHashB {
		t.Fatal("array order change did not change the hash")
	}
	whitespaceSame, err := commandHash("b", Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{"a":{"x":1},"y":2}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	whitespaceSameB, err := commandHash("b", Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(`{"y":2,"a":{"x":1}}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if whitespaceSame != whitespaceSameB {
		t.Fatal("semantically equal JSON produced different hashes")
	}
}

func TestEffectIDIsolation(t *testing.T) {
	base := effectID("result", "batch-1", "task-1", 0)
	variants := []struct {
		name  string
		op    string
		batch string
		task  string
		retry int
	}{
		{name: "operation", op: "dead", batch: "batch-1", task: "task-1", retry: 0},
		{name: "batch", op: "result", batch: "batch-2", task: "task-1", retry: 0},
		{name: "task", op: "result", batch: "batch-1", task: "task-2", retry: 0},
		{name: "retry count", op: "result", batch: "batch-1", task: "task-1", retry: 1},
	}
	for _, tc := range variants {
		got := effectID(tc.op, tc.batch, tc.task, tc.retry)
		if got == base {
			t.Fatalf("effectID not isolated by %s", tc.name)
		}
	}
	if effectID("result", "batch-1", "task-1", 0) != base {
		t.Fatal("effectID is not deterministic")
	}
	if !strings.HasPrefix(base, "v1:result:sha256:") {
		t.Fatalf("effectID shape = %q", base)
	}
}

func TestKeyspaceEncodingAndIsolation(t *testing.T) {
	ns := newKeyspace("billing:队列")
	if !strings.HasPrefix(ns.prefix, "echoqueue:1:") {
		t.Fatalf("prefix = %q", ns.prefix)
	}
	rawName := base64.RawURLEncoding.EncodeToString([]byte("billing:队列"))
	if ns.prefix != "echoqueue:1:"+rawName {
		t.Fatalf("prefix encoding = %q, want %q", ns.prefix, "echoqueue:1:"+rawName)
	}
	if strings.Contains(ns.prefix[12:], ":") {
		// Only the fixed structure may contain ':'; the encoded part must not.
		t.Fatalf("separator leaked into encoded namespace: %q", ns.prefix)
	}
	deadlineKey := ns.deadline()
	pendingKey := ns.pending("b/1")
	receiptKey := ns.receipt("b/1")
	if !strings.HasSuffix(pendingKey, ":pending:"+base64.RawURLEncoding.EncodeToString([]byte("b/1"))) {
		t.Fatalf("pending key = %q", pendingKey)
	}
	if receiptKey == pendingKey || deadlineKey == pendingKey || receiptKey == deadlineKey {
		t.Fatal("keyspace components collide")
	}
	if ns.result("t") == ns.deadFor("t") {
		t.Fatal("result and dead keys collide")
	}
	if newKeyspace("a").pending("x") == newKeyspace("b").pending("x") {
		t.Fatal("namespaces are not isolated")
	}
	if ns.pending("a") == ns.pending("b") || ns.pending("a") == ns.receipt("a") {
		t.Fatal("key parts are not isolated")
	}
	if ns.result("a:b") == ns.result("a/b") {
		t.Fatal("task name separators are not isolated")
	}
	if newKeyspace("unicode-🚀").result("任务") != newKeyspace("unicode-🚀").result("任务") {
		t.Fatal("unicode keyspace is unstable")
	}
}

func TestRedisInfoParsing(t *testing.T) {
	version, _, err := redisVersion("# Server\r\nredis_version:6.1.0\r\nos:Linux\r\n")
	if err != nil || version != 6 {
		t.Fatalf("6.1 major = %d, err=%v", version, err)
	}
	version, _, err = redisVersion("redis_version:6.2.7\n")
	if err != nil || version != 6 {
		t.Fatalf("6.2.7 major = %d, err=%v", version, err)
	}
	version, _, err = redisVersion("redis_version:7.4.1\n")
	if err != nil || version != 7 {
		t.Fatalf("7.4.1 major = %d, err=%v", version, err)
	}
	version, _, err = redisVersion("redis_version:8.0.0\n")
	if err != nil || version != 8 {
		t.Fatalf("8.0.0 major = %d, err=%v", version, err)
	}
	for _, bad := range []string{"", "# Server\n", "redis_version:\n", "redis_version:abc.1\n", "redis_version:6\n", "redis_version:6.x\n"} {
		if _, _, err := redisVersion(bad); err == nil {
			t.Fatalf("redisVersion accepted %q", bad)
		}
	}
	if major, minor, err := redisVersion("redis_version:6.2.7-rc1\n"); err != nil || major != 6 || minor != 2 {
		t.Fatalf("rc suffix = %d.%d, err=%v", major, minor, err)
	}

	if got := infoValue("# Server\nredis_version:6.2.7\nos:Linux\n", "redis_version"); got != "6.2.7" {
		t.Fatalf("infoValue = %q", got)
	}
	if got := infoValue("redis_version:6.2.7\r\nredis_mode:standalone\r\n", "redis_mode"); got != "standalone" {
		t.Fatalf("infoValue crlf = %q", got)
	}
	if got := infoValue("  redis_version : 6.2.7  \n", "redis_version"); got != "" {
		t.Fatalf("spaced key parsed as %q", got)
	}
	if got := infoValue("redis_version:6.2.7\n", "missing_key"); got != "" {
		t.Fatalf("missing key parsed as %q", got)
	}
	if got := infoValue("", "redis_version"); got != "" {
		t.Fatalf("empty info parsed as %q", got)
	}

	disabled := []string{
		"ERR This instance has cluster support disabled",
		"ERR This instance has cluster support is disabled",
		"ERR Cluster is not enabled",
		"ERR cluster support disabled",
	}
	for _, message := range disabled {
		if !isClusterDisabled(errors.New(message)) {
			t.Fatalf("isClusterDisabled missed %q", message)
		}
	}
	for _, message := range []string{"ERR unknown command", "network timeout", "connection refused"} {
		if isClusterDisabled(errors.New(message)) {
			t.Fatalf("isClusterDisabled matched %q", message)
		}
	}
	if !isClusterDisabled(errors.New("ERR CLUSTER SUPPORT DISABLED")) {
		t.Fatal("isClusterDisabled is not case insensitive")
	}
}

func TestCheckRedisNilArguments(t *testing.T) {
	if err := checkRedis(context.Background(), nil); err == nil {
		t.Fatal("nil client accepted")
	}
	if err := checkRedis(nil, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})); err == nil {
		t.Fatal("nil context accepted")
	}
}

func TestParseReceiptResponseRejectsMalformedShapes(t *testing.T) {
	operationErr := errors.New("operation")
	stored := storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       "batch-response",
		RequestID:     "request-response",
		CommandHash:   "hash-response",
		Winner:        "settle",
		ClosedAt:      100,
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := parseReceiptResponse("batch-response", []interface{}{"applied", string(raw)}, operationErr)
	if err != nil || valid.Status != ReceiptApplied || valid.RequestID != stored.RequestID {
		t.Fatalf("valid response = %+v, err=%v", valid, err)
	}
	if notFound, err := parseReceiptResponse("batch-response", []interface{}{"not_found"}, operationErr); err != nil || notFound.Status != ReceiptNotFound {
		t.Fatalf("not_found response = %+v, err=%v", notFound, err)
	}
	for _, value := range []interface{}{
		[]interface{}{"applied"},
		[]interface{}{"applied", 123},
		[]interface{}{"applied", "{bad"},
		[]interface{}{"unknown"},
	} {
		if _, err := parseReceiptResponse("batch-response", value, operationErr); err == nil {
			t.Fatalf("malformed response accepted: %#v", value)
		}
	}
}

func TestOutcomeSizeValidationMatrix(t *testing.T) {
	// Use real byte payloads so the count is exact.
	exactPayload := func(n int) json.RawMessage {
		raw := json.RawMessage(`"`)
		for i := 0; i < n; i++ {
			raw = append(raw, 'a')
		}
		raw = append(raw, '"')
		return raw
	}
	utf8Payload := func(n int) json.RawMessage {
		// n multi-byte runes, each three bytes when quoted.
		raw := json.RawMessage(`"`)
		for i := 0; i < n; i++ {
			raw = append(raw, 0xe4, 0xb8, 0xad)
		}
		raw = append(raw, '"')
		return raw
	}
	tests := []struct {
		name      string
		maxPay    int
		maxBatch  int
		outcome   Outcome
		accept    bool
		wantError string
	}{
		{name: "single exactly max payload", maxPay: 5, maxBatch: 100,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: exactPayload(3)}}}, accept: true},
		{name: "single one below max payload", maxPay: 5, maxBatch: 100,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: exactPayload(2)}}}, accept: true},
		{name: "single one above max payload", maxPay: 5, maxBatch: 100,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: exactPayload(4)}}}, accept: false, wantError: "max_payload_bytes"},
		{name: "batch exactly max batch", maxPay: 10, maxBatch: 12,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "a", Data: exactPayload(4)}, {TaskID: "b", Data: exactPayload(4)}}}, accept: true},
		{name: "batch one below max batch", maxPay: 10, maxBatch: 12,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "a", Data: exactPayload(4)}, {TaskID: "b", Data: exactPayload(3)}}}, accept: true},
		{name: "batch one above max batch", maxPay: 10, maxBatch: 12,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "a", Data: exactPayload(4)}, {TaskID: "b", Data: exactPayload(5)}}}, accept: false, wantError: "max_batch_bytes"},
		{name: "each legal total too big", maxPay: 100, maxBatch: 20,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "a", Data: exactPayload(12)}, {TaskID: "b", Data: exactPayload(12)}}}, accept: false, wantError: "max_batch_bytes"},
		{name: "utf8 counted by bytes", maxPay: 10, maxBatch: 100,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: utf8Payload(3)}}}, accept: false, wantError: "max_payload_bytes"},
		{name: "json whitespace counted", maxPay: 4, maxBatch: 100,
			outcome: Outcome{RequestID: "r", Results: []Result{{TaskID: "t", Data: json.RawMessage(` { } `)}}}, accept: false, wantError: "max_payload_bytes"},
		{name: "failure only not charged", maxPay: 1, maxBatch: 1,
			outcome: Outcome{RequestID: "r", Failures: []Failure{{TaskID: "t", Reason: "any reason at all"}}}, accept: true},
		{name: "empty results accepted", maxPay: 1, maxBatch: 1,
			outcome: Outcome{RequestID: "r", Failures: []Failure{{TaskID: "t"}}}, accept: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOutcomeResultSizes(tc.maxPay, tc.maxBatch, tc.outcome)
			if tc.accept && err != nil {
				t.Fatalf("valid outcome rejected: %v", err)
			}
			if !tc.accept {
				if err == nil {
					t.Fatal("oversized outcome accepted")
				}
				if !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error = %q, want %q", err, tc.wantError)
				}
				if !strings.Contains(err.Error(), "t") && strings.Contains(err.Error(), "payload") {
					t.Fatalf("error lacks task context: %q", err)
				}
			}
		})
	}
}

func TestOutcomeSizeErrorDoesNotLeakBusinessData(t *testing.T) {
	secret := "secret-business-payload-" + strings.Repeat("x", 40)
	outcome := Outcome{
		RequestID: "r",
		Results:   []Result{{TaskID: "task-1", Data: json.RawMessage(`{"secret":"` + secret + `"}`)}},
	}
	err := validateOutcomeResultSizes(5, 10, outcome)
	if err == nil {
		t.Fatal("oversized outcome accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked business data: %q", err)
	}
	if !strings.Contains(err.Error(), "task-1") {
		t.Fatalf("error lacks task_id context: %q", err)
	}
	if !errors.Is(err, errResultTooLarge) {
		t.Fatalf("error %q is not the size sentinel", err)
	}
}

type countingHook struct {
	commands atomic.Int64
}

func (h *countingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		return next(ctx, cmd)
	}
}

func (h *countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.commands.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

func TestOutcomeSizeRejectsBeforeAnyRedisCommand(t *testing.T) {
	hook := &countingHook{}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	rdb.AddHook(hook)
	defer rdb.Close()
	scheduler, err := New(rdb, Config{Namespace: "size-no-redis", MaxPayloadBytes: 4, MaxBatchBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	outcome := Outcome{
		RequestID: "request-1",
		Results:   []Result{{TaskID: "task-1", Data: json.RawMessage(`{"too":"big"}`)}},
	}
	receipt, err := scheduler.Settle(context.Background(), "batch-1", outcome)
	if err == nil {
		t.Fatal("oversized settle accepted")
	}
	if !strings.Contains(err.Error(), "result exceeds configured size limit") {
		t.Fatalf("error = %v, want size sentinel text", err)
	}
	if receipt.Status != ReceiptInvalid {
		t.Fatalf("receipt status = %q", receipt.Status)
	}
	if got := hook.commands.Load(); got != 0 {
		t.Fatalf("Redis command count = %d, want 0 (size rejection must precede every Redis call)", got)
	}
}

func TestOutcomeSizeWithinLimitsPassesToRedis(t *testing.T) {
	hook := &countingHook{}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	rdb.AddHook(hook)
	defer rdb.Close()
	scheduler, err := New(rdb, Config{Namespace: "size-to-redis", MaxPayloadBytes: 100, MaxBatchBytes: 200})
	if err != nil {
		t.Fatal(err)
	}
	outcome := Outcome{
		RequestID: "request-1",
		Results:   []Result{{TaskID: "task-1", Data: json.RawMessage(`{"ok":true}`)}},
	}
	if _, err := scheduler.Settle(context.Background(), "batch-1", outcome); err == nil {
		t.Fatal("settle unexpectedly succeeded against unreachable Redis")
	}
	if got := hook.commands.Load(); got == 0 {
		t.Fatal("within-limit settle never reached Redis")
	}
}
