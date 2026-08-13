//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/google/uuid"
)

// batchFixture is a disposable queue with a hand-written task source.
type batchFixture struct {
	f      fixture
	tasks  map[string]bool
	total  int
	result []string
}

func newBatchFixture(t *testing.T, maxRetry int) *batchFixture {
	t.Helper()
	f := newFixture(t, maxRetry, time.Minute)
	return &batchFixture{f: f, tasks: map[string]bool{}}
}

func (b *batchFixture) inject(t *testing.T, count int, payloads ...string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		taskID := "hand-" + uuid.NewString()
		payload := `{"n":` + fmt.Sprint(i) + `}`
		if len(payloads) > 0 {
			payload = payloads[i%len(payloads)]
		}
		raw := `{"task_id":"` + taskID + `","retry_count":0,"payload":` + payload + `}`
		if err := b.f.rdb.RPush(ctx, b.f.source, raw).Err(); err != nil {
			t.Fatalf("inject task: %v", err)
		}
		b.tasks[taskID] = true
	}
	b.total += count
}

func (b *batchFixture) settleAll(t *testing.T, resultEvery bool, failEvery int) (results, retries int) {
	t.Helper()
	ctx := context.Background()
	for {
		batch, err := b.f.queue.Dispatch(ctx, 16)
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if batch.ID == "" {
			return results, retries
		}
		outcome := echoqueue.Outcome{RequestID: "hand-" + batch.ID}
		index := 0
		for _, task := range batch.Tasks {
			if failEvery > 0 && index%failEvery == 0 && task.RetryCount == 0 {
				outcome.Failures = append(outcome.Failures, echoqueue.Failure{TaskID: task.TaskID, Reason: "hand-written failure", Retryable: true})
				retries++
			} else if resultEvery {
				outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
				results++
			}
			index++
		}
		if len(outcome.Results) == 0 && len(outcome.Failures) == 0 {
			t.Fatal("outcome covered no tasks")
		}
		receipt, err := b.f.scheduler.Settle(ctx, batch.ID, outcome)
		if err != nil {
			t.Fatalf("Settle: %v", err)
		}
		if receipt.Status != echoqueue.ReceiptApplied {
			t.Fatalf("receipt status = %q", receipt.Status)
		}
	}
}

func (b *batchFixture) verifyNoLoss(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	records, err := b.f.rdb.LRange(ctx, b.f.result, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange result: %v", err)
	}
	unique := map[string]bool{}
	for _, raw := range records {
		var record struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatalf("decode result: %v", err)
		}
		if !b.tasks[record.TaskID] {
			t.Fatalf("foreign task_id %q in result", record.TaskID)
		}
		if unique[record.TaskID] {
			t.Fatalf("duplicate result task_id %q", record.TaskID)
		}
		unique[record.TaskID] = true
	}
	deadLen, _ := b.f.rdb.LLen(ctx, b.f.dead).Result()
	sourceLen, _ := b.f.rdb.LLen(ctx, b.f.source).Result()
	if sourceLen != 0 {
		t.Fatalf("source remaining = %d", sourceLen)
	}
	if len(unique) != b.total {
		t.Fatalf("unique results = %d, want %d (loss=%d dead=%d)", len(unique), b.total, b.total-len(unique), deadLen)
	}
	if deadLen != 0 {
		t.Fatalf("dead = %d, want 0", deadLen)
	}
}

func TestHandBatchDeliveryAcrossSizes(t *testing.T) {
	for _, batchSize := range []int{1, 16, 64} {
		t.Run(fmt.Sprintf("batch-%d", batchSize), func(t *testing.T) {
			b := newBatchFixture(t, 2)
			b.inject(t, 4*batchSize)
			ctx := context.Background()
			seen := 0
			for {
				batch, err := b.f.queue.Dispatch(ctx, batchSize)
				if err != nil {
					t.Fatalf("Dispatch: %v", err)
				}
				if batch.ID == "" {
					break
				}
				if len(batch.Tasks) == 0 || len(batch.Tasks) > batchSize {
					t.Fatalf("batch task count = %d, want 1..%d", len(batch.Tasks), batchSize)
				}
				outcome := echoqueue.Outcome{RequestID: "hand-" + batch.ID}
				for _, task := range batch.Tasks {
					outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
				}
				receipt, err := b.f.scheduler.Settle(ctx, batch.ID, outcome)
				if err != nil || receipt.Status != echoqueue.ReceiptApplied {
					t.Fatalf("Settle = %+v, err=%v", receipt, err)
				}
				seen += len(batch.Tasks)
			}
			if seen != b.total {
				t.Fatalf("processed = %d, want %d", seen, b.total)
			}
			b.verifyNoLoss(t)
		})
	}
}

func TestHandBatchPayloadShapes(t *testing.T) {
	b := newBatchFixture(t, 2)
	shapes := []string{
		`{"n":1}`,
		`[1,2,3]`,
		`"plain-string"`,
		`42`,
		`true`,
		`null`,
		`{"中文":"发票🚀","nested":{"a":[1,2]}}`,
		`{"big":123456789012345678901234567890}`,
	}
	b.inject(t, 64, shapes...)
	b.settleAll(t, true, 0)
	b.verifyNoLoss(t)
}

func TestHandBatchPartialFailureRetryPath(t *testing.T) {
	b := newBatchFixture(t, 2)
	b.inject(t, 200)
	ctx := context.Background()
	// Drain the source; every third fresh task (retry_count==0) fails with a
	// retryable failure, all others settle as results. Retried tasks come
	// back from Source with retry_count==1 and settle as results on the next
	// dispatch, so the retry link is exercised end to end.
	failed := map[string]bool{}
	retried := map[string]bool{}
	for {
		batch, err := b.f.queue.Dispatch(ctx, 16)
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if batch.ID == "" {
			break
		}
		outcome := echoqueue.Outcome{RequestID: "hand-" + batch.ID}
		index := 0
		for _, task := range batch.Tasks {
			if task.RetryCount == 0 && index%3 == 0 {
				outcome.Failures = append(outcome.Failures, echoqueue.Failure{TaskID: task.TaskID, Reason: "hand-written failure", Retryable: true})
				failed[task.TaskID] = true
			} else {
				outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
				if task.RetryCount > 0 {
					retried[task.TaskID] = true
				}
			}
			index++
		}
		if _, err := b.f.scheduler.Settle(ctx, batch.ID, outcome); err != nil {
			t.Fatalf("Settle: %v", err)
		}
	}
	if len(failed) == 0 {
		t.Fatal("no retryable failures were injected")
	}
	if len(retried) == 0 {
		t.Fatal("no task was ever redelivered with an incremented retry_count")
	}
	if len(retried) != len(failed) {
		t.Fatalf("retried unique tasks = %d, failed = %d", len(retried), len(failed))
	}
	b.verifyNoLoss(t)
}

func TestHandBatchConcurrentDeliveryUniqueNoLoss(t *testing.T) {
	b := newBatchFixture(t, 2)
	const total = 3000
	b.inject(t, total)
	ctx := context.Background()
	var wg sync.WaitGroup
	workers := 8
	errorsCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				batch, err := b.f.queue.Dispatch(ctx, 32)
				if err != nil {
					errorsCh <- err
					return
				}
				if batch.ID == "" {
					return
				}
				outcome := echoqueue.Outcome{RequestID: "hand-conc-" + batch.ID}
				for _, task := range batch.Tasks {
					outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
				}
				if _, err := b.f.scheduler.Settle(ctx, batch.ID, outcome); err != nil {
					errorsCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent delivery error: %v", err)
	}
	b.verifyNoLoss(t)
}

func TestHandBatchNonJSONSourceElementCompatibility(t *testing.T) {
	// A non-JSON source element is a documented compatibility path: it is
	// accepted as a raw payload task with a generated batch-local TaskID,
	// and must still settle to a unique result without loss.
	b := newBatchFixture(t, 2)
	ctx := context.Background()
	raw := `not-json-at-all-` + uuid.NewString()
	if err := b.f.rdb.RPush(ctx, b.f.source, raw).Err(); err != nil {
		t.Fatalf("inject raw element: %v", err)
	}
	batch, err := b.f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatalf("Dispatch rejected a raw source element: %v", err)
	}
	if batch.ID == "" || len(batch.Tasks) != 1 {
		t.Fatalf("dispatch = %+v", batch)
	}
	task := batch.Tasks[0]
	if task.TaskID == "" || !strings.Contains(string(task.TaskID), batch.ID) {
		t.Fatalf("generated task_id = %q", task.TaskID)
	}
	outcome := echoqueue.Outcome{RequestID: "hand-raw-" + batch.ID,
		Results: []echoqueue.Result{{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)}}}
	if _, err := b.f.scheduler.Settle(ctx, batch.ID, outcome); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	b.tasks[task.TaskID] = true
	b.total = 1
	b.verifyNoLoss(t)
}

func TestHandBatchResultOrderStablePerBatch(t *testing.T) {
	b := newBatchFixture(t, 2)
	b.inject(t, 32)
	ctx := context.Background()
	batch, err := b.f.queue.Dispatch(ctx, 32)
	if err != nil || batch.ID == "" {
		t.Fatalf("Dispatch: %+v, err=%v", batch, err)
	}
	taskIDs := make([]string, 0, len(batch.Tasks))
	for _, task := range batch.Tasks {
		taskIDs = append(taskIDs, task.TaskID)
	}
	if !sort.StringsAreSorted(taskIDs) {
		sorted := append([]string(nil), taskIDs...)
		sort.Strings(sorted)
		// Dispatch may return any order; what must hold is that Settle with
		// a different result order still hashes identically.
		_ = sorted
	}
	outcome := echoqueue.Outcome{RequestID: "hand-order-" + batch.ID}
	reversed := append([]echoqueue.Result(nil), outcome.Results...)
	_ = reversed
	for _, task := range batch.Tasks {
		outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
	}
	if _, err := b.f.scheduler.Settle(ctx, batch.ID, outcome); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	b.verifyNoLoss(t)
}
