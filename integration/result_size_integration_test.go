//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/cccccccccooool/EchoQueue/internal/testutil"
	"github.com/google/uuid"
)

func newSizeFixture(t *testing.T, maxPayload, maxBatch int) fixture {
	t.Helper()
	rdb := testutil.MustRedis(t)
	f := fixture{
		rdb:       rdb,
		namespace: "eq-size-" + uuid.NewString(),
		source:    "eq-size-source-" + uuid.NewString(),
		result:    "eq-size-result-" + uuid.NewString(),
		dead:      "eq-size-dead-" + uuid.NewString(),
	}
	cfg := echoqueue.Config{
		Namespace:         f.namespace,
		VisibilityTimeout: time.Minute,
		ReceiptTTL:        time.Hour,
		MaxRetry:          1,
		MaxRetrySet:       true,
		RunInterval:       5 * time.Millisecond,
		RunBatchSize:      16,
		MaxPayloadBytes:   maxPayload,
		MaxBatchBytes:     maxBatch,
	}
	var err error
	f.scheduler, err = echoqueue.New(rdb, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.queue, err = f.scheduler.Bind(echoqueue.QueueConfig{
		TaskName: "invoice",
		Source:   f.source,
		Result:   f.result,
		Dead:     f.dead,
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		keys, _ := rdb.Keys(ctx, internalPrefix(f.namespace)+"*").Result()
		keys = append(keys, f.source, f.result, f.dead)
		_, _ = rdb.Del(ctx, keys...).Result()
		_ = rdb.Close()
	})
	return f
}

func quotedPayload(n int) []byte {
	raw := []byte(`"`)
	for i := 0; i < n; i++ {
		raw = append(raw, 'a')
	}
	raw = append(raw, '"')
	return raw
}

func snapshotRedisState(t *testing.T, f fixture, batchID string) (pendingRaw string, score string, sourceLen, resultLen, deadLen int64, receiptExists bool) {
	t.Helper()
	ctx := context.Background()
	pendingRaw, _ = f.rdb.Get(ctx, pendingKey(f.namespace, batchID)).Result()
	deadline, _ := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result()
	score = time.UnixMilli(int64(deadline)).UTC().Format(time.RFC3339Nano)
	sourceLen, _ = f.rdb.LLen(ctx, f.source).Result()
	resultLen, _ = f.rdb.LLen(ctx, f.result).Result()
	deadLen, _ = f.rdb.LLen(ctx, f.dead).Result()
	receiptExistsValue, _ := f.rdb.Exists(ctx, receiptKey(f.namespace, batchID)).Result()
	receiptExists = receiptExistsValue == 1
	return
}

func TestOversizedSettleLeavesRedisUntouched(t *testing.T) {
	f := newSizeFixture(t, 64, 128)
	ctx := context.Background()
	seedTask(t, f, "task-1")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if batch.ID == "" {
		t.Fatal("empty dispatch")
	}
	pendingBefore, scoreBefore, sourceBefore, resultBefore, deadBefore, receiptBefore := snapshotRedisState(t, f, batch.ID)
	if receiptBefore {
		t.Fatal("receipt exists before settle")
	}

	oversized := echoqueue.Outcome{
		RequestID: "oversized-attempt",
		Results: []echoqueue.Result{
			{TaskID: "task-1", Data: quotedPayload(70)},
		},
	}
	_, err = f.scheduler.Settle(ctx, batch.ID, oversized)
	if err == nil {
		t.Fatal("oversized settle succeeded")
	}
	if !strings.Contains(err.Error(), "result exceeds configured size limit") {
		t.Fatalf("settle error = %v", err)
	}

	pendingAfter, scoreAfter, sourceAfter, resultAfter, deadAfter, receiptAfter := snapshotRedisState(t, f, batch.ID)
	if pendingAfter != pendingBefore {
		t.Fatal("pending snapshot was modified by oversized settle")
	}
	if scoreAfter != scoreBefore {
		t.Fatalf("deadline score changed: %s -> %s", scoreBefore, scoreAfter)
	}
	if sourceAfter != sourceBefore || resultAfter != resultBefore || deadAfter != deadBefore {
		t.Fatalf("list lengths changed: source %d->%d result %d->%d dead %d->%d",
			sourceBefore, sourceAfter, resultBefore, resultAfter, deadBefore, deadAfter)
	}
	if receiptAfter {
		t.Fatal("receipt was created by oversized settle")
	}
}

func TestExternalReferenceSettlesAfterOversizedRejection(t *testing.T) {
	f := newSizeFixture(t, 64, 128)
	ctx := context.Background()
	seedTask(t, f, "task-1")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	_, err = f.scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "oversized-attempt",
		Results:   []echoqueue.Result{{TaskID: "task-1", Data: quotedPayload(70)}},
	})
	if err == nil {
		t.Fatal("oversized settle succeeded")
	}

	receipt, err := f.scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "external-ref-attempt",
		Results: []echoqueue.Result{
			{TaskID: "task-1", Data: []byte(`{"ref":"bucket/key-1","size":7,"sha256":"abc"}`)},
		},
	})
	if err != nil {
		t.Fatalf("external-reference settle failed: %v", err)
	}
	if receipt.Status != echoqueue.ReceiptApplied {
		t.Fatalf("receipt status = %q, want applied", receipt.Status)
	}
	resultLen, _ := f.rdb.LLen(ctx, f.result).Result()
	if resultLen != 1 {
		t.Fatalf("result list length = %d, want 1", resultLen)
	}
	exists, _ := f.rdb.Exists(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if exists == 1 {
		t.Fatal("pending still exists after applied settle")
	}
}

func TestSizeLimitsBoundaryAtMaxPayloadAndMaxBatch(t *testing.T) {
	f := newSizeFixture(t, 64, 128)
	ctx := context.Background()
	seedTask(t, f, "task-1")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	// Exactly max payload (10 bytes: 8 chars + 2 quotes) is allowed.
	receipt, err := f.scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "exact-max",
		Results:   []echoqueue.Result{{TaskID: "task-1", Data: quotedPayload(62)}},
	})
	if err != nil {
		t.Fatalf("exact-max settle failed: %v", err)
	}
	if receipt.Status != echoqueue.ReceiptApplied {
		t.Fatalf("receipt status = %q", receipt.Status)
	}
}

func TestSizeLimitsRejectOneByteOverMaxPayload(t *testing.T) {
	f := newSizeFixture(t, 64, 128)
	ctx := context.Background()
	seedTask(t, f, "task-1")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	_, err = f.scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "over-max",
		Results:   []echoqueue.Result{{TaskID: "task-1", Data: quotedPayload(63)}},
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds max_payload_bytes") {
		t.Fatalf("one-byte-over settle error = %v", err)
	}
	exists, _ := f.rdb.Exists(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if exists == 0 {
		t.Fatal("pending was deleted by rejected settle")
	}
}

func TestSizeLimitsRejectBatchOverMaxBatchBytes(t *testing.T) {
	f := newSizeFixture(t, 100, 180)
	ctx := context.Background()
	seedTask(t, f, "task-1")
	seedTask(t, f, "task-2")
	batch, err := f.queue.Dispatch(ctx, 2)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(batch.Tasks) != 2 {
		t.Fatalf("dispatched %d tasks, want 2", len(batch.Tasks))
	}
	// Each result is exactly 100 bytes (within max_payload), but the pair
	// totals 200 bytes which exceeds max_batch_bytes 180.
	_, err = f.scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "over-batch",
		Results: []echoqueue.Result{
			{TaskID: "task-1", Data: quotedPayload(98)},
			{TaskID: "task-2", Data: quotedPayload(98)},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds max_batch_bytes") {
		t.Fatalf("batch-over settle error = %v", err)
	}
	exists, _ := f.rdb.Exists(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if exists == 0 {
		t.Fatal("pending was deleted by rejected settle")
	}
}

func TestFailureOnlyOutcomeNotRejectedBySizeBudget(t *testing.T) {
	f := newSizeFixture(t, 32, 64)
	ctx := context.Background()
	seedTask(t, f, "task-1")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	receipt, err := f.scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "failure-only",
		Failures:  []echoqueue.Failure{{TaskID: "task-1", Reason: "permanent", Retryable: false}},
	})
	if err != nil {
		t.Fatalf("failure-only settle rejected: %v", err)
	}
	if receipt.Status != echoqueue.ReceiptApplied {
		t.Fatalf("receipt status = %q", receipt.Status)
	}
	deadLen, _ := f.rdb.LLen(ctx, f.dead).Result()
	if deadLen != 1 {
		t.Fatalf("dead length = %d, want 1", deadLen)
	}
}
