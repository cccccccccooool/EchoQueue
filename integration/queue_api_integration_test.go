//go:build integration

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	echoqueue "example.com/m"
	"example.com/m/internal/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type fixture struct {
	rdb       *redis.Client
	scheduler *echoqueue.Scheduler
	queue     *echoqueue.Queue
	namespace string
	source    string
	result    string
	dead      string
}

func newFixture(t *testing.T, maxRetry int, visibility time.Duration) fixture {
	return newFixtureWithRunBatchSize(t, maxRetry, visibility, 16)
}

func newFixtureWithRunBatchSize(t *testing.T, maxRetry int, visibility time.Duration, runBatchSize int) fixture {
	t.Helper()
	rdb := testutil.MustRedis(t)
	fixture := fixture{
		rdb:       rdb,
		namespace: "eq-slim-" + uuid.NewString(),
		source:    "eq-source-" + uuid.NewString(),
		result:    "eq-result-" + uuid.NewString(),
		dead:      "eq-dead-" + uuid.NewString(),
	}
	cfg := echoqueue.Config{
		Namespace:         fixture.namespace,
		VisibilityTimeout: visibility,
		ReceiptTTL:        time.Hour,
		MaxRetry:          maxRetry,
		MaxRetrySet:       true,
		RunInterval:       5 * time.Millisecond,
		RunBatchSize:      runBatchSize,
	}
	var err error
	fixture.scheduler, err = echoqueue.New(rdb, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fixture.queue, err = fixture.scheduler.Bind(echoqueue.QueueConfig{
		TaskName: "email",
		Source:   fixture.source,
		Result:   fixture.result,
		Dead:     fixture.dead,
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		keys, _ := rdb.Keys(ctx, internalPrefix(fixture.namespace)+"*").Result()
		keys = append(keys, fixture.source, fixture.result, fixture.dead)
		_, _ = rdb.Del(ctx, keys...).Result()
		_ = rdb.Close()
	})
	return fixture
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
	raw := `{"task_id":"` + taskID + `","retry_count":0,"payload":{"input":true}}`
	if err := f.rdb.RPush(context.Background(), f.source, raw).Err(); err != nil {
		t.Fatalf("seed task: %v", err)
	}
}

func TestDispatchEmptyAndImmutablePending(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	ctx := context.Background()
	empty, err := f.queue.Dispatch(ctx, 1)
	if err != nil || empty.ID != "" {
		t.Fatalf("empty dispatch = %+v, err=%v", empty, err)
	}
	seedTask(t, f, "task-1")
	batch, err := f.queue.Dispatch(ctx, 1)
	if err != nil || batch.ID == "" || len(batch.Tasks) != 1 {
		t.Fatalf("dispatch = %+v, err=%v", batch, err)
	}
	key := pendingKey(f.namespace, batch.ID)
	if ttl, err := f.rdb.PTTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("pending TTL = %v, err=%v", ttl, err)
	}
	var snapshot map[string]interface{}
	raw, err := f.rdb.Get(ctx, key).Result()
	if err != nil || json.Unmarshal([]byte(raw), &snapshot) != nil {
		t.Fatalf("pending snapshot err=%v raw=%q", err, raw)
	}
	if snapshot["max_retry"].(float64) != 0 || snapshot["state"] != "PENDING" {
		t.Fatalf("pending snapshot = %#v", snapshot)
	}
	if _, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result(); err != nil {
		t.Fatalf("deadline index: %v", err)
	}
}

func TestSettleDuplicateConflictAndResultIdentity(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "task-duplicate")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	outcome := echoqueue.Outcome{
		RequestID: "request-1",
		Results:   []echoqueue.Result{{TaskID: "task-duplicate", Data: json.RawMessage(`{"ok":true}`)}},
	}
	applied, err := f.scheduler.Settle(context.Background(), batch.ID, outcome)
	if err != nil || applied.Status != echoqueue.ReceiptApplied {
		t.Fatalf("applied = %+v, err=%v", applied, err)
	}
	if applied.ResultCount != 1 || applied.Winner != "settle" {
		t.Fatalf("receipt = %+v", applied)
	}
	if ttl, ttlErr := f.rdb.PTTL(context.Background(), receiptKey(f.namespace, batch.ID)).Result(); ttlErr != nil || ttl <= 0 || ttl > time.Hour {
		t.Fatalf("settle receipt TTL = %v, err=%v", ttl, ttlErr)
	}
	duplicate, err := f.scheduler.Settle(context.Background(), batch.ID, outcome)
	if err != nil || duplicate.Status != echoqueue.ReceiptDuplicate || duplicate.RequestID != applied.RequestID {
		t.Fatalf("duplicate = %+v, err=%v", duplicate, err)
	}
	conflictOutcome := outcome
	conflictOutcome.Results = []echoqueue.Result{{TaskID: "task-duplicate", Data: json.RawMessage(`{"ok":false}`)}}
	conflict, err := f.scheduler.Settle(context.Background(), batch.ID, conflictOutcome)
	if err != nil || conflict.Status != echoqueue.ReceiptConflict {
		t.Fatalf("conflict = %+v, err=%v", conflict, err)
	}
	if got, err := f.rdb.LLen(context.Background(), f.result).Result(); err != nil || got != 1 {
		t.Fatalf("result length = %d, err=%v", got, err)
	}
	resultRaw, err := f.rdb.LRange(context.Background(), f.result, 0, -1).Result()
	if err != nil || len(resultRaw) != 1 {
		t.Fatalf("result records = %#v, err=%v", resultRaw, err)
	}
	var resultRecord map[string]interface{}
	if err := json.Unmarshal([]byte(resultRaw[0]), &resultRecord); err != nil || resultRecord["task_id"] != "task-duplicate" || resultRecord["effect_id"] == "" {
		t.Fatalf("result record identity = %#v, err=%v", resultRecord, err)
	}
	if _, err := f.rdb.Get(context.Background(), pendingKey(f.namespace, batch.ID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("pending after settle err=%v", err)
	}
}

func TestWrongTypeNeverDeletesPending(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	seedTask(t, f, "task-wrong-type")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(context.Background(), f.result, "wrong", 0).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = f.scheduler.Settle(context.Background(), batch.ID, echoqueue.Outcome{
		RequestID: "request-wrong-type",
		Results:   []echoqueue.Result{{TaskID: "task-wrong-type", Data: json.RawMessage(`true`)}},
	})
	if err == nil {
		t.Fatal("wrong result type was accepted")
	}
	if _, err := f.rdb.Get(context.Background(), pendingKey(f.namespace, batch.ID)).Result(); err != nil {
		t.Fatalf("pending was deleted after Redis error: %v", err)
	}
}

func TestMaxRetryZeroRecoverWritesDeadAndLateSettleIsStale(t *testing.T) {
	f := newFixture(t, 0, 10*time.Millisecond)
	seedTask(t, f, "task-timeout")
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- f.scheduler.Run(ctx) }()
	testutil.WaitFor(t, time.Second, func() bool {
		_, getErr := f.rdb.Get(context.Background(), receiptKey(f.namespace, batch.ID)).Result()
		return getErr == nil
	})
	cancel()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
	late, err := f.scheduler.Settle(context.Background(), batch.ID, echoqueue.Outcome{
		RequestID: "late-worker",
		Results:   []echoqueue.Result{{TaskID: "task-timeout", Data: json.RawMessage(`true`)}},
	})
	if err != nil || late.Status != echoqueue.ReceiptStale {
		t.Fatalf("late settle = %+v, err=%v", late, err)
	}
	if ttl, ttlErr := f.rdb.PTTL(context.Background(), receiptKey(f.namespace, batch.ID)).Result(); ttlErr != nil || ttl <= 0 || ttl > time.Hour {
		t.Fatalf("recover receipt TTL = %v, err=%v", ttl, ttlErr)
	}
	if got, err := f.rdb.LLen(context.Background(), f.dead).Result(); err != nil || got != 1 {
		t.Fatalf("dead length = %d, err=%v", got, err)
	}
}

func TestRecoverReportsCorruptBatchAndContinuesToNext(t *testing.T) {
	f := newFixture(t, 0, 10*time.Millisecond)
	ctx := context.Background()
	seedTask(t, f, "task-corrupt")
	corruptBatch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	seedTask(t, f, "task-normal-after-corrupt")
	normalBatch, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := f.rdb.Set(ctx, pendingKey(f.namespace, corruptBatch.ID), "{corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}

	runErr := f.scheduler.Run(ctx)
	if runErr == nil || !strings.Contains(runErr.Error(), corruptBatch.ID) {
		t.Fatalf("Run error = %v, want corrupt batch id", runErr)
	}
	if _, err := f.rdb.Get(ctx, receiptKey(f.namespace, normalBatch.ID)).Result(); err != nil {
		t.Fatalf("normal batch was not recovered after corrupt batch: %v", err)
	}
	if got, err := f.rdb.LLen(ctx, f.dead).Result(); err != nil || got != 1 {
		t.Fatalf("dead length after normal recovery = %d, err=%v", got, err)
	}
	if raw, err := f.rdb.Get(ctx, pendingKey(f.namespace, corruptBatch.ID)).Result(); err != nil || raw != "{corrupt" {
		t.Fatalf("corrupt pending evidence = %q, err=%v", raw, err)
	}
}

func TestRunCannotStartTwice(t *testing.T) {
	f := newFixture(t, 0, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- f.scheduler.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)
	if secondErr := f.scheduler.Run(context.Background()); !errors.Is(secondErr, echoqueue.ErrRunAlreadyActive) {
		t.Fatalf("second Run error = %v", secondErr)
	}
	cancel()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first Run did not stop")
	}
}
