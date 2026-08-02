//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	echoqueue "example.com/m"
	"github.com/redis/go-redis/v9"
)

func TestRecoverCleansOrphanDeadline(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	ctx := context.Background()
	batchID := "orphan-batch"
	if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 1, Member: batchID}).Err(); err != nil {
		t.Fatal(err)
	}

	runErr := runUntilState(t, f, func() bool {
		_, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result()
		return errors.Is(err, redis.Nil)
	})
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run error = %v", runErr)
	}
}

func TestRecoverCleansDeadlineForLegalReceipt(t *testing.T) {
	f := newFixture(t, 0, time.Hour)
	seedTask(t, f, "r1-settled")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := f.scheduler.Settle(context.Background(), batch.ID, echoqueue.Outcome{
		RequestID: "settle-first",
		Results:   []echoqueue.Result{{TaskID: "r1-settled", Data: json.RawMessage("{\"ok\":true}")}},
	})
	if err != nil || applied.Status != echoqueue.ReceiptApplied {
		t.Fatalf("Settle = %+v, err=%v", applied, err)
	}
	ctx := context.Background()
	receiptKeyValue := receiptKey(f.namespace, batch.ID)
	before, err := f.rdb.Get(ctx, receiptKeyValue).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 1, Member: batch.ID}).Err(); err != nil {
		t.Fatal(err)
	}

	runErr := runUntilState(t, f, func() bool {
		_, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
		return errors.Is(scoreErr, redis.Nil)
	})
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run error = %v", runErr)
	}
	after, err := f.rdb.Get(ctx, receiptKeyValue).Result()
	if err != nil || after != before {
		t.Fatalf("legal receipt changed: before=%q after=%q err=%v", before, after, err)
	}
}

func TestRecoverRepairsEarlyDeadlineFromPending(t *testing.T) {
	f := newFixture(t, 0, time.Hour)
	seedTask(t, f, "r1-early-deadline")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 0, Member: batch.ID}).Err(); err != nil {
		t.Fatal(err)
	}

	runErr := runUntilState(t, f, func() bool {
		score, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
		return scoreErr == nil && score == float64(batch.DeadlineAt.UnixMilli())
	})
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run error = %v", runErr)
	}
	score, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if score != float64(batch.DeadlineAt.UnixMilli()) {
		t.Fatalf("deadline score = %v, want immutable deadline_at %v", score, batch.DeadlineAt.UnixMilli())
	}
	after, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil || after != pendingBefore {
		t.Fatalf("pending changed during deadline repair: before=%q after=%q err=%v", pendingBefore, after, err)
	}
}

func TestRecoverCorruptReceiptDoesNotDeleteEvidence(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "r1-corrupt-receipt")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	deadlineBefore, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, receiptKey(f.namespace, batch.ID), "{corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}

	runErr := runExpectError(t, f)
	if !strings.Contains(runErr.Error(), batch.ID) {
		t.Fatalf("Run error = %v, want batch id", runErr)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after corrupt receipt: %q, err=%v", got, getErr)
	}
	if got, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); scoreErr != nil || got <= deadlineBefore {
		t.Fatalf("deadline was not deferred after corrupt receipt: %v, before=%v, err=%v", got, deadlineBefore, scoreErr)
	}
}

func TestRecoverWrongTypeReceiptDoesNotDeleteEvidence(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "r1-wrong-receipt-type")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	deadlineBefore, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.RPush(ctx, receiptKey(f.namespace, batch.ID), "wrong-type").Err(); err != nil {
		t.Fatal(err)
	}

	runErr := runExpectError(t, f)
	if !strings.Contains(runErr.Error(), batch.ID) {
		t.Fatalf("Run error = %v, want batch id", runErr)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after wrong receipt type: %q, err=%v", got, getErr)
	}
	if got, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); scoreErr != nil || got <= deadlineBefore {
		t.Fatalf("deadline was not deferred after wrong receipt type: %v, before=%v, err=%v", got, deadlineBefore, scoreErr)
	}
}

func TestRecoverWrongTypeDeadlineDoesNotDeletePending(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "r1-wrong-deadline-type")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Del(ctx, deadlineKey(f.namespace)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, deadlineKey(f.namespace), "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}

	if runErr := runExpectError(t, f); runErr == nil {
		t.Fatal("Run unexpectedly succeeded with wrong deadline type")
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after wrong deadline type: %q, err=%v", got, getErr)
	}
	if got, getErr := f.rdb.Get(ctx, deadlineKey(f.namespace)).Result(); getErr != nil || got != "wrong-type" {
		t.Fatalf("deadline key changed after wrong type: %q, err=%v", got, getErr)
	}
}
