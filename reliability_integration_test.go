//go:build integration

package echoqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cccccccccooool/EchoQueue/internal/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestReceiptBaseValidationPrecedesEffects(t *testing.T) {
	t.Run("settle", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		ctx := context.Background()
		seedTask(t, f, "r0-settle-task")
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		value, err := f.rdb.Eval(ctx, integrationLua(t, "settle.lua"), []string{
			pendingKey(f.namespace, batch.ID),
			receiptKey(f.namespace, batch.ID),
			deadlineKey(f.namespace),
			f.result,
			f.source,
			f.dead,
		}, batch.ID, "r0-settle-request", "r0-settle-hash", "{bad", "[]", "[]", "[]", int64(time.Hour/time.Millisecond)).Result()
		if err != nil || scriptStatus(value) != "invalid" {
			t.Fatalf("settle response = %v, err=%v", value, err)
		}
		assertR0NoTerminalWrites(t, f, batch.ID)
	})

	t.Run("settle fractional ttl", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		ctx := context.Background()
		seedTask(t, f, "r0-settle-fractional-ttl")
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		value, err := f.rdb.Eval(ctx, integrationLua(t, "settle.lua"), []string{
			pendingKey(f.namespace, batch.ID),
			receiptKey(f.namespace, batch.ID),
			deadlineKey(f.namespace),
			f.result,
			f.source,
			f.dead,
		}, batch.ID, "r0-settle-fractional-request", "r0-settle-fractional-hash", "{}", "[]", "[]", "[]", "1.5").Result()
		if err != nil || scriptStatus(value) != "invalid" {
			t.Fatalf("settle fractional ttl response = %v, err=%v", value, err)
		}
		assertR0NoTerminalWrites(t, f, batch.ID)
	})

	t.Run("recover", func(t *testing.T) {
		f := newFixture(t, 0, time.Millisecond)
		ctx := context.Background()
		seedTask(t, f, "r0-recover-task")
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		value, err := f.rdb.Eval(ctx, integrationLua(t, "recover.lua"), []string{
			pendingKey(f.namespace, batch.ID),
			receiptKey(f.namespace, batch.ID),
			deadlineKey(f.namespace),
			f.source,
			f.dead,
		}, batch.ID, "recover:"+batch.ID, "r0-recover-hash", "{bad", "[]", "[]", int64(time.Hour/time.Millisecond)).Result()
		if err != nil || scriptStatus(value) != "invalid" {
			t.Fatalf("recover response = %v, err=%v", value, err)
		}
		assertR0NoTerminalWrites(t, f, batch.ID)
	})
}

func TestSettleDoesNotTreatFractionalReceiptAsTerminal(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	ctx := context.Background()
	seedTask(t, f, "fractional-receipt-task")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	deadlineBefore, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := json.Marshal(map[string]interface{}{
		"schema_version":   1,
		"protocol_version": 1,
		"batch_id":         batch.ID,
		"request_id":       "fractional-receipt-request",
		"command_hash":     "fractional-receipt-hash",
		"winner":           "settle",
		"closed_at":        1.5,
		"result_count":     1,
		"retry_count":      0,
		"dead_count":       0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, receiptKey(f.namespace, batch.ID), receiptRaw, 0).Err(); err != nil {
		t.Fatal(err)
	}

	_, settleErr := f.scheduler.Settle(ctx, batch.ID, Outcome{
		RequestID: "fractional-receipt-request",
		Results:   []Result{{TaskID: "fractional-receipt-task", Data: json.RawMessage(`{"ok":true}`)}},
	})
	if settleErr == nil {
		t.Fatal("fractional receipt was accepted as terminal")
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after fractional receipt: %q, err=%v", got, getErr)
	}
	if got, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); scoreErr != nil || got != deadlineBefore {
		t.Fatalf("deadline changed after fractional receipt: %v, before=%v, err=%v", got, deadlineBefore, scoreErr)
	}
	if got, lengthErr := f.rdb.LLen(ctx, f.result).Result(); lengthErr != nil || got != 0 {
		t.Fatalf("result effects after fractional receipt: %d, err=%v", got, lengthErr)
	}
}

func assertR0NoTerminalWrites(t *testing.T, f fixture, batchID string) {
	t.Helper()
	ctx := context.Background()
	for name, key := range map[string]string{
		"source": f.source,
		"result": f.result,
		"dead":   f.dead,
	} {
		if length, err := f.rdb.LLen(ctx, key).Result(); err != nil || length != 0 {
			t.Fatalf("%s effects = %d, err=%v", name, length, err)
		}
	}
	if _, err := f.rdb.Get(ctx, receiptKey(f.namespace, batchID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("receipt after invalid base = %v", err)
	}
	if _, err := f.rdb.Get(ctx, pendingKey(f.namespace, batchID)).Result(); err != nil {
		t.Fatalf("pending after invalid base: %v", err)
	}
	if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result(); err != nil {
		t.Fatalf("deadline after invalid base: %v", err)
	}
}

func evalDeferStatus(t *testing.T, f fixture, batchID string, delayMillis int64) (string, []interface{}, error) {
	t.Helper()
	return evalStatus(t, f, "defer_recover.lua", []string{
		pendingKey(f.namespace, batchID),
		receiptKey(f.namespace, batchID),
		deadlineKey(f.namespace),
	}, batchID, delayMillis)
}

func TestDeferRecoverMovesDeadlineWithoutChangingPending(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "defer-pending")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	scoreBefore, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}

	status, _, err := evalDeferStatus(t, f, batch.ID, 1000)
	if err != nil || status != "deferred" {
		t.Fatalf("defer = %q, err=%v", status, err)
	}
	scoreAfter, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if scoreAfter <= scoreBefore {
		t.Fatalf("deadline did not move: before=%v after=%v", scoreBefore, scoreAfter)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed during defer: before=%q after=%q err=%v", pendingBefore, got, getErr)
	}
	if _, err := f.rdb.Get(ctx, receiptKey(f.namespace, batch.ID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("defer created receipt, err=%v", err)
	}
}

func TestDeferRecoverTerminalAndOrphanCleanup(t *testing.T) {
	t.Run("terminal", func(t *testing.T) {
		f := newFixture(t, 0, time.Hour)
		seedTask(t, f, "defer-terminal")
		batch, err := f.queue.Dispatch(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		applied, err := f.scheduler.Settle(context.Background(), batch.ID, Outcome{
			RequestID: "defer-terminal-settle",
			Results:   []Result{{TaskID: "defer-terminal", Data: json.RawMessage("{\"ok\":true}")}},
		})
		if err != nil || applied.Status != ReceiptApplied {
			t.Fatalf("Settle = %+v, err=%v", applied, err)
		}
		ctx := context.Background()
		if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 1, Member: batch.ID}).Err(); err != nil {
			t.Fatal(err)
		}
		status, _, err := evalDeferStatus(t, f, batch.ID, 1000)
		if err != nil || status != "terminal" {
			t.Fatalf("terminal defer = %q, err=%v", status, err)
		}
		if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("terminal deadline remains, err=%v", err)
		}
	})

	t.Run("orphan", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		ctx := context.Background()
		batchID := "defer-orphan"
		if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 1, Member: batchID}).Err(); err != nil {
			t.Fatal(err)
		}
		status, _, err := evalDeferStatus(t, f, batchID, 1000)
		if err != nil || status != "orphan" {
			t.Fatalf("orphan defer = %q, err=%v", status, err)
		}
		if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("orphan deadline remains, err=%v", err)
		}
	})
}

func TestDeferRecoverCorruptReceiptDefersWithPending(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "defer-corrupt-receipt")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, receiptKey(f.namespace, batch.ID), "{corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}

	status, _, err := evalDeferStatus(t, f, batch.ID, 1000)
	if err != nil || status != "deferred" {
		t.Fatalf("corrupt receipt defer = %q, err=%v", status, err)
	}
	after, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil || after <= before {
		t.Fatalf("corrupt receipt deadline = %v, before=%v, err=%v", after, before, err)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after corrupt receipt defer: %q", got)
	}
}

func TestDeferRecoverWrongTypeReceiptDefersWithPending(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "defer-wrong-receipt-type")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.rdb.RPush(ctx, receiptKey(f.namespace, batch.ID), "wrong-type").Err(); err != nil {
		t.Fatal(err)
	}

	status, _, err := evalDeferStatus(t, f, batch.ID, 1000)
	if err != nil || status != "deferred" {
		t.Fatalf("wrong receipt type defer = %q, err=%v", status, err)
	}
	if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); err != nil {
		t.Fatalf("deferred deadline missing: %v", err)
	}
}

func TestDeferRecoverInvalidDeadlinePreservesEvidence(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "defer-wrong-deadline-type")
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

	status, parts, err := evalDeferStatus(t, f, batch.ID, 1000)
	if err != nil || status != "invalid" || len(parts) < 2 {
		t.Fatalf("wrong deadline defer = %q, parts=%v, err=%v", status, parts, err)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after wrong type: %q, err=%v", got, getErr)
	}
	if got, getErr := f.rdb.Get(ctx, deadlineKey(f.namespace)).Result(); getErr != nil || got != "wrong-type" {
		t.Fatalf("deadline key changed after wrong type: %q, err=%v", got, getErr)
	}
}

func TestDeferRecoverCorruptReceiptWithoutPendingIsNotOrphan(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	ctx := context.Background()
	batchID := "defer-corrupt-orphan"
	if err := f.rdb.Set(ctx, receiptKey(f.namespace, batchID), "{corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 1, Member: batchID}).Err(); err != nil {
		t.Fatal(err)
	}

	status, parts, err := evalDeferStatus(t, f, batchID, 1000)
	if err != nil || status != "invalid" || len(parts) < 2 {
		t.Fatalf("corrupt receipt orphan defer = %q, parts=%v, err=%v", status, parts, err)
	}
	if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result(); err != nil {
		t.Fatalf("corrupt receipt orphan deadline was not preserved, err=%v", err)
	}
}

func assertClosedWithoutDeadline(t *testing.T, f fixture, batchID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.rdb.Get(ctx, pendingKey(f.namespace, batchID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("pending remains: %v", err)
	}
	if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("deadline remains: %v", err)
	}
	if _, err := f.rdb.Get(ctx, receiptKey(f.namespace, batchID)).Result(); err != nil {
		t.Fatalf("receipt missing: %v", err)
	}
}

func evalDeferScript(ctx context.Context, f fixture, script, batchID string) (string, error) {
	value, err := f.rdb.Eval(ctx, script, []string{
		pendingKey(f.namespace, batchID),
		receiptKey(f.namespace, batchID),
		deadlineKey(f.namespace),
	}, batchID, int64(1000)).Result()
	if err != nil {
		return "", err
	}
	return scriptStatus(value), nil
}

func TestCanceledRunDoesNotDeferCandidate(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "canceled-run")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.rdb.Set(ctx, pendingKey(f.namespace, batch.ID), "{canceled-corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	before, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := f.scheduler.Run(runCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run error = %v", err)
	}
	after, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil || after != before {
		t.Fatalf("canceled Run moved deadline: before=%v after=%v err=%v", before, after, err)
	}
}

func TestSettleAndDeferBothOrdersDoNotReviveDeadline(t *testing.T) {
	t.Run("settle-first", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		seedTask(t, f, "settle-before-defer")
		batch, err := f.queue.Dispatch(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		applied, err := f.scheduler.Settle(context.Background(), batch.ID, Outcome{
			RequestID: "settle-before-defer-request",
			Results:   []Result{{TaskID: "settle-before-defer", Data: json.RawMessage("{\"ok\":true}")}},
		})
		if err != nil || applied.Status != ReceiptApplied {
			t.Fatalf("Settle = %+v, err=%v", applied, err)
		}
		status, _, err := evalDeferStatus(t, f, batch.ID, 1000)
		if err != nil || status != "terminal" {
			t.Fatalf("defer after Settle = %q, err=%v", status, err)
		}
		assertClosedWithoutDeadline(t, f, batch.ID)
	})

	t.Run("defer-first", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		seedTask(t, f, "defer-before-settle")
		batch, err := f.queue.Dispatch(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		status, _, err := evalDeferStatus(t, f, batch.ID, 1000)
		if err != nil || status != "deferred" {
			t.Fatalf("defer = %q, err=%v", status, err)
		}
		applied, err := f.scheduler.Settle(context.Background(), batch.ID, Outcome{
			RequestID: "defer-before-settle-request",
			Results:   []Result{{TaskID: "defer-before-settle", Data: json.RawMessage("{\"ok\":true}")}},
		})
		if err != nil || applied.Status != ReceiptApplied {
			t.Fatalf("Settle after defer = %+v, err=%v", applied, err)
		}
		assertClosedWithoutDeadline(t, f, batch.ID)
	})
}

func TestSettleAndDeferConcurrentRaceClosesOnce(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "settle-defer-concurrent")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	outcome := Outcome{
		RequestID: "settle-defer-concurrent-request",
		Results:   []Result{{TaskID: "settle-defer-concurrent", Data: json.RawMessage("{\"ok\":true}")}},
	}
	deferScript := integrationLua(t, "defer_recover.lua")
	start := make(chan struct{})
	var wg sync.WaitGroup
	settleCh := make(chan struct {
		receipt Receipt
		err     error
	}, 1)
	deferCh := make(chan struct {
		status string
		err    error
	}, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		receipt, settleErr := f.scheduler.Settle(context.Background(), batch.ID, outcome)
		settleCh <- struct {
			receipt Receipt
			err     error
		}{receipt, settleErr}
	}()
	go func() {
		defer wg.Done()
		<-start
		status, deferErr := evalDeferScript(context.Background(), f, deferScript, batch.ID)
		deferCh <- struct {
			status string
			err    error
		}{status, deferErr}
	}()
	close(start)
	wg.Wait()

	settled := <-settleCh
	deferred := <-deferCh
	if settled.err != nil || settled.receipt.Status != ReceiptApplied {
		t.Fatalf("concurrent Settle = %+v, err=%v", settled.receipt, settled.err)
	}
	if deferred.err != nil || (deferred.status != "terminal" && deferred.status != "deferred") {
		t.Fatalf("concurrent defer = %q, err=%v", deferred.status, deferred.err)
	}
	assertClosedWithoutDeadline(t, f, batch.ID)
}

func TestDeferRepeatedExecutionIsIdempotent(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "defer-idempotent")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	deferScript := integrationLua(t, "defer_recover.lua")
	first, err := evalDeferScript(ctx, f, deferScript, batch.ID)
	if err != nil || first != "deferred" {
		t.Fatalf("first defer = %q, err=%v", first, err)
	}
	scoreFirst, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	second, err := evalDeferScript(ctx, f, deferScript, batch.ID)
	if err != nil || second != "deferred" {
		t.Fatalf("second defer = %q, err=%v", second, err)
	}
	scoreSecond, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
	if err != nil || scoreSecond < scoreFirst {
		t.Fatalf("defer score regressed: first=%v second=%v err=%v", scoreFirst, scoreSecond, err)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
		t.Fatalf("pending changed after repeated defer: %q", got)
	}
	if got, cardErr := f.rdb.ZCard(ctx, deadlineKey(f.namespace)).Result(); cardErr != nil || got != 1 {
		t.Fatalf("deadline cardinality = %d, err=%v", got, cardErr)
	}
}

func TestSettleAndRecoverHaveOneFirstWinner(t *testing.T) {
	f := newFixture(t, 0, 10*time.Millisecond)
	seedTask(t, f, "race-task")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	recoverRequest := "recover:" + batch.ID
	recoverHash := "recover-race-hash"
	receiptBase, err := json.Marshal(map[string]interface{}{
		"schema_version":   1,
		"protocol_version": 1,
		"batch_id":         batch.ID,
		"request_id":       recoverRequest,
		"command_hash":     recoverHash,
		"winner":           "recover",
		"result_count":     0,
		"retry_count":      0,
		"dead_count":       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadEffect, err := json.Marshal([]map[string]interface{}{{
		"schema_version":   1,
		"protocol_version": 1,
		"effect_id":        "raced-effect",
		"task_id":          "race-task",
		"batch_id":         batch.ID,
		"retry_count":      0,
		"reason":           "visibility timeout",
		"payload":          json.RawMessage("true"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	recoverScript := integrationLua(t, "recover.lua")
	keys := []string{
		pendingKey(f.namespace, batch.ID),
		receiptKey(f.namespace, batch.ID),
		deadlineKey(f.namespace),
		f.source,
		f.dead,
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	settleCh := make(chan struct {
		receipt Receipt
		err     error
	}, 1)
	recoverCh := make(chan struct {
		status string
		err    error
	}, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		receipt, settleErr := f.scheduler.Settle(context.Background(), batch.ID, Outcome{
			RequestID: "worker-race",
			Results:   []Result{{TaskID: "race-task", Data: json.RawMessage("true")}},
		})
		settleCh <- struct {
			receipt Receipt
			err     error
		}{receipt, settleErr}
	}()
	go func() {
		defer wg.Done()
		<-start
		value, recoverErr := f.rdb.Eval(context.Background(), recoverScript, keys,
			batch.ID,
			recoverRequest,
			recoverHash,
			string(receiptBase),
			"[]",
			string(deadEffect),
			int64(time.Hour/time.Millisecond),
		).Result()
		recoverCh <- struct {
			status string
			err    error
		}{scriptStatus(value), recoverErr}
	}()
	close(start)
	wg.Wait()

	settled := <-settleCh
	recovered := <-recoverCh
	if settled.err != nil {
		t.Fatalf("settle error: %v", settled.err)
	}
	if recovered.err != nil {
		t.Fatalf("recover error: %v", recovered.err)
	}
	if (settled.receipt.Status == ReceiptApplied) == (recovered.status == "applied") {
		t.Fatalf("winner statuses = settle:%s recover:%s", settled.receipt.Status, recovered.status)
	}
	if settled.receipt.Status != ReceiptStale && recovered.status != "stale" {
		t.Fatalf("loser status missing: settle:%s recover:%s", settled.receipt.Status, recovered.status)
	}
	if _, err := f.rdb.Get(context.Background(), pendingKey(f.namespace, batch.ID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("pending remains after first winner: %v", err)
	}
}

func TestWhiteBoxPrivateRecoveryDataFlow(t *testing.T) {
	t.Run("not due repairs no state", func(t *testing.T) {
		f := newFixture(t, 0, time.Hour)
		ctx := context.Background()
		seedTask(t, f, "whitebox-not-due")
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
		if err != nil {
			t.Fatal(err)
		}

		receipt, err := f.scheduler.recoverBatch(ctx, batch.ID)
		if err != nil || receipt.Status != ReceiptNotDue {
			t.Fatalf("recoverBatch not_due = %+v, err=%v", receipt, err)
		}
		if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
			t.Fatalf("pending changed on not_due: %q, err=%v", got, getErr)
		}
		if _, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); scoreErr != nil {
			t.Fatalf("deadline missing on not_due: %v", scoreErr)
		}
	})

	t.Run("expired retry and duplicate receipt", func(t *testing.T) {
		f := newFixture(t, 1, time.Millisecond)
		ctx := context.Background()
		seedTask(t, f, "whitebox-retry")
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)

		first, err := f.scheduler.recoverBatch(ctx, batch.ID)
		if err != nil || first.Status != ReceiptApplied || first.RetryCount != 1 {
			t.Fatalf("first recover = %+v, err=%v", first, err)
		}
		retryRaw, err := f.rdb.LRange(ctx, f.source, 0, -1).Result()
		if err != nil || len(retryRaw) != 1 {
			t.Fatalf("recovery retry source = %#v, err=%v", retryRaw, err)
		}
		var retry Task
		if err := json.Unmarshal([]byte(retryRaw[0]), &retry); err != nil {
			t.Fatal(err)
		}
		if retry.TaskID != "whitebox-retry" || retry.RetryCount != 1 {
			t.Fatalf("recovery retry task = %+v", retry)
		}

		second, err := f.scheduler.recoverBatch(ctx, batch.ID)
		if err != nil || second.Status != ReceiptDuplicate || second.RequestID != first.RequestID {
			t.Fatalf("duplicate recover = %+v, err=%v", second, err)
		}
		if got, err := f.rdb.LLen(ctx, f.source).Result(); err != nil || got != 1 {
			t.Fatalf("duplicate recovery added effects: %d, err=%v", got, err)
		}
	})

	t.Run("orphan clears deadline", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		ctx := context.Background()
		batchID := "whitebox-orphan"
		if err := f.rdb.ZAdd(ctx, deadlineKey(f.namespace), redis.Z{Score: 1, Member: batchID}).Err(); err != nil {
			t.Fatal(err)
		}
		receipt, err := f.scheduler.recoverBatch(ctx, batchID)
		if err != nil || receipt.Status != ReceiptNotFound {
			t.Fatalf("orphan recover = %+v, err=%v", receipt, err)
		}
		if _, scoreErr := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result(); !errors.Is(scoreErr, redis.Nil) {
			t.Fatalf("orphan deadline remains: %v", scoreErr)
		}
	})

	t.Run("defer preserves pending and rotates index", func(t *testing.T) {
		f := newFixture(t, 0, time.Second)
		ctx := context.Background()
		seedTask(t, f, "whitebox-defer")
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		pendingBefore, err := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result()
		if err != nil {
			t.Fatal(err)
		}
		before, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		status, err := f.scheduler.deferRecover(ctx, batch.ID)
		if err != nil || status != "deferred" {
			t.Fatalf("defer = %q, err=%v", status, err)
		}
		after, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
		if err != nil || after <= before {
			t.Fatalf("defer deadline = %v, before=%v, err=%v", after, before, err)
		}
		if got, getErr := f.rdb.Get(ctx, pendingKey(f.namespace, batch.ID)).Result(); getErr != nil || got != pendingBefore {
			t.Fatalf("pending changed by defer: %q, err=%v", got, getErr)
		}
	})
}

func TestWhiteBoxRecoverExpiredContinuesAfterCorruptPending(t *testing.T) {
	f := newFixture(t, 0, time.Millisecond)
	ctx := context.Background()
	seedTask(t, f, "whitebox-corrupt")
	corrupt, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	seedTask(t, f, "whitebox-normal")
	normal, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, pendingKey(f.namespace, corrupt.ID), "{whitebox-corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	before, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), corrupt.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)

	runErr := f.scheduler.recoverExpired(ctx)
	if runErr == nil || !strings.Contains(runErr.Error(), corrupt.ID) {
		t.Fatalf("recoverExpired error = %v, want corrupt batch %s", runErr, corrupt.ID)
	}
	if _, err := f.rdb.Get(ctx, receiptKey(f.namespace, normal.ID)).Result(); err != nil {
		t.Fatalf("normal batch did not recover after corrupt candidate: %v", err)
	}
	if got, err := f.rdb.Get(ctx, pendingKey(f.namespace, corrupt.ID)).Result(); err != nil || got != "{whitebox-corrupt" {
		t.Fatalf("corrupt pending changed: %q, err=%v", got, err)
	}
	after, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), corrupt.ID).Result()
	if err != nil || after <= before {
		t.Fatalf("corrupt deadline was not deferred: after=%v before=%v err=%v", after, before, err)
	}
}

func TestRecoverLuaRejectsClosedPendingBeforeEffects(t *testing.T) {
	f := newFixture(t, 0, time.Millisecond)
	ctx := context.Background()
	seedTask(t, f, "whitebox-closed")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	pendingKeyValue := pendingKey(f.namespace, batch.ID)
	pendingRaw, err := f.rdb.Get(ctx, pendingKeyValue).Result()
	if err != nil {
		t.Fatal(err)
	}
	var pending map[string]interface{}
	if err := json.Unmarshal([]byte(pendingRaw), &pending); err != nil {
		t.Fatal(err)
	}
	pending["state"] = "CLOSED"
	pending["deadline_at"] = int64(1)
	closedRaw, err := json.Marshal(pending)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, pendingKeyValue, closedRaw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	deadRaw, err := json.Marshal([]deadRecord{{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		EffectID:      "whitebox-closed-effect",
		TaskID:        "whitebox-closed",
		BatchID:       batch.ID,
		RetryCount:    0,
		Payload:       json.RawMessage(`{"input":true}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := json.Marshal(storedReceipt{
		SchemaVersion: protocolVersion,
		Protocol:      protocolVersion,
		BatchID:       batch.ID,
		RequestID:     "recover:" + batch.ID,
		CommandHash:   recoverCommandHash(batch.ID),
		Winner:        "recover",
		DeadCount:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := f.rdb.Eval(ctx, recoverScript, []string{
		pendingKeyValue,
		receiptKey(f.namespace, batch.ID),
		deadlineKey(f.namespace),
		f.source,
		f.dead,
	}, batch.ID, "recover:"+batch.ID, recoverCommandHash(batch.ID), string(receiptRaw), "[]", string(deadRaw), int64(time.Hour/time.Millisecond)).Result()
	if err != nil || scriptStatus(value) != "invalid" {
		t.Fatalf("closed pending recover = %v, err=%v", value, err)
	}
	if got, getErr := f.rdb.Get(ctx, pendingKeyValue).Result(); getErr != nil || got != string(closedRaw) {
		t.Fatalf("closed pending changed: %q, err=%v", got, getErr)
	}
	if got, err := f.rdb.LLen(ctx, f.dead).Result(); err != nil || got != 0 {
		t.Fatalf("closed pending produced dead effect: %d, err=%v", got, err)
	}
	if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); err != nil {
		t.Fatalf("closed pending deadline disappeared: %v", err)
	}
}

type fixture struct {
	rdb       *redis.Client
	scheduler *Scheduler
	queue     *Queue
	namespace string
	source    string
	result    string
	dead      string
}

func newFixture(t *testing.T, maxRetry int, visibility time.Duration) fixture {
	t.Helper()
	rdb := testutil.MustRedis(t)
	f := fixture{
		rdb:       rdb,
		namespace: "eq-internal-" + uuid.NewString(),
		source:    "eq-source-" + uuid.NewString(),
		result:    "eq-result-" + uuid.NewString(),
		dead:      "eq-dead-" + uuid.NewString(),
	}
	var err error
	f.scheduler, err = New(rdb, Config{
		Namespace:         f.namespace,
		VisibilityTimeout: visibility,
		ReceiptTTL:        time.Hour,
		MaxRetry:          maxRetry,
		MaxRetrySet:       true,
		RunInterval:       5 * time.Millisecond,
		RunBatchSize:      4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.queue, err = f.scheduler.Bind(QueueConfig{
		TaskName: "internal",
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

func internalPrefix(namespace string) string {
	return "echoqueue:1:" + base64.RawURLEncoding.EncodeToString([]byte(namespace))
}

func pendingKey(namespace, batchID string) string {
	return internalPrefix(namespace) + ":pending:" + base64.RawURLEncoding.EncodeToString([]byte(batchID))
}

func receiptKey(namespace, batchID string) string {
	return internalPrefix(namespace) + ":receipt:" + base64.RawURLEncoding.EncodeToString([]byte(batchID))
}

func deadlineKey(namespace string) string { return internalPrefix(namespace) + ":deadlines" }

func seedTask(t *testing.T, f fixture, taskID string) {
	t.Helper()
	if err := f.rdb.RPush(context.Background(), f.source, `{"task_id":"`+taskID+`","retry_count":0,"payload":{"input":true}}`).Err(); err != nil {
		t.Fatalf("seed task: %v", err)
	}
}

func integrationLua(t *testing.T, name string) string {
	t.Helper()
	switch name {
	case "defer_recover.lua":
		return deferRecoverScript
	case "dispatch.lua":
		return dispatchScript
	case "recover.lua":
		return recoverScript
	case "settle.lua":
		return settleScript
	default:
		t.Fatalf("unknown embedded Lua %s", name)
		return ""
	}
}

func scriptValueString(value interface{}) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return ""
	}
}

func scriptStatus(value interface{}) string {
	parts, ok := value.([]interface{})
	if !ok || len(parts) == 0 {
		return ""
	}
	return scriptValueString(parts[0])
}

func evalStatus(t *testing.T, f fixture, scriptName string, keys []string, args ...interface{}) (string, []interface{}, error) {
	t.Helper()
	value, err := f.rdb.Eval(context.Background(), integrationLua(t, scriptName), keys, args...).Result()
	if err != nil {
		return "", nil, err
	}
	parts, ok := value.([]interface{})
	if !ok {
		return "", nil, errors.New("malformed script response")
	}
	return scriptStatus(value), parts, nil
}
