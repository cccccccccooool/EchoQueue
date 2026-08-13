//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/cccccccccooool/EchoQueue/internal/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type blackboxFixture struct {
	rdb       *redis.Client
	scheduler *echoqueue.Scheduler
	queue     *echoqueue.Queue
	namespace string
	source    string
	result    string
	dead      string
}

func newBlackboxFixture(t *testing.T, maxRetry int, visibility, receiptTTL, runInterval time.Duration, runBatchSize int, customize ...func(*echoqueue.Config)) blackboxFixture {
	t.Helper()
	rdb := testutil.MustRedis(t)
	suffix := uuid.NewString()
	f := blackboxFixture{
		rdb:       rdb,
		namespace: "eq-blackbox-" + suffix,
		source:    "eq-blackbox-source-" + suffix,
		result:    "eq-blackbox-result-" + suffix,
		dead:      "eq-blackbox-dead-" + suffix,
	}
	cfg := echoqueue.DefaultConfig()
	cfg.Namespace = f.namespace
	cfg.VisibilityTimeout = visibility
	cfg.ReceiptTTL = receiptTTL
	cfg.MaxRetry = maxRetry
	cfg.MaxRetrySet = true
	cfg.MaxBatchSize = 4
	cfg.RunInterval = runInterval
	cfg.RunBatchSize = runBatchSize
	for _, change := range customize {
		change(&cfg)
	}
	var err error
	f.scheduler, err = echoqueue.New(rdb, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.queue, err = f.scheduler.Bind(echoqueue.QueueConfig{
		TaskName: "blackbox-task",
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
		_, _ = rdb.Del(ctx, f.source, f.result, f.dead).Result()
	})
	return f
}

func seedBlackboxRaw(t *testing.T, f blackboxFixture, raw ...string) {
	t.Helper()
	values := make([]interface{}, len(raw))
	for i, value := range raw {
		values[i] = value
	}
	if err := f.rdb.RPush(context.Background(), f.source, values...).Err(); err != nil {
		t.Fatalf("seed source: %v", err)
	}
}

func seedBlackboxTask(t *testing.T, f blackboxFixture, taskID string, retryCount int) {
	t.Helper()
	raw, err := json.Marshal(echoqueue.Task{
		TaskID:     taskID,
		RetryCount: retryCount,
		Payload:    json.RawMessage(`{"input":true}`),
	})
	if err != nil {
		t.Fatalf("encode task: %v", err)
	}
	seedBlackboxRaw(t, f, string(raw))
}

func readBlackboxList(t *testing.T, f blackboxFixture, key string) []string {
	t.Helper()
	values, err := f.rdb.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatalf("read list %q: %v", key, err)
	}
	return values
}

func runBlackboxUntil(t *testing.T, f blackboxFixture, condition func() bool) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- f.scheduler.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-result:
			cancel()
			t.Fatalf("Run exited before public state was reached: %v", err)
		default:
		}
		if condition() {
			cancel()
			select {
			case err := <-result:
				return err
			case <-time.After(time.Second):
				t.Fatal("Run did not stop after public state was reached")
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	t.Fatal("public Redis state was not reached before Run timeout")
	return nil
}

func resultOutcome(taskID, requestID, data string) echoqueue.Outcome {
	return echoqueue.Outcome{
		RequestID: requestID,
		Results: []echoqueue.Result{{
			TaskID: taskID,
			Data:   json.RawMessage(data),
		}},
	}
}

func TestBlackBoxPublicValidationAndBinding(t *testing.T) {
	unreachable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer unreachable.Close()
	if _, err := echoqueue.New(nil, echoqueue.Config{}); err == nil {
		t.Fatal("New accepted a nil Redis client")
	}
	if _, err := echoqueue.New(unreachable, echoqueue.Config{}); err == nil {
		t.Fatal("New accepted an empty namespace")
	}
	if _, err := echoqueue.New(unreachable, echoqueue.Config{Namespace: "bad", ReceiptTTL: -time.Second}); err == nil {
		t.Fatal("New accepted a negative ReceiptTTL")
	}

	f := newBlackboxFixture(t, 1, time.Second, time.Hour, 5*time.Millisecond, 2)
	if _, err := f.scheduler.Bind(echoqueue.QueueConfig{TaskName: "blackbox-task", Source: "other-source"}); err == nil {
		t.Fatal("Bind accepted a duplicate TaskName")
	}
	if _, err := f.scheduler.Bind(echoqueue.QueueConfig{TaskName: "self-conflict", Source: "same", Result: "same"}); err == nil {
		t.Fatal("Bind accepted a source/result collision")
	}
	if _, err := f.scheduler.Bind(echoqueue.QueueConfig{TaskName: "source-conflict", Source: f.source, Result: "other-result", Dead: "other-dead"}); err == nil {
		t.Fatal("Bind accepted a cross-queue Source collision")
	}
	if _, err := f.scheduler.Bind(echoqueue.QueueConfig{TaskName: "result-conflict", Source: "other-source-1", Result: f.result, Dead: "other-dead-1"}); err == nil {
		t.Fatal("Bind accepted a cross-queue Result collision")
	}
	if _, err := f.scheduler.Bind(echoqueue.QueueConfig{TaskName: "dead-conflict", Source: "other-source-2", Result: "other-result-2", Dead: f.dead}); err == nil {
		t.Fatal("Bind accepted a cross-queue Dead collision")
	}

	immutableSource := "eq-blackbox-immutable-source-" + uuid.NewString()
	mutatedSource := "eq-blackbox-mutated-source-" + uuid.NewString()
	immutableResult := "eq-blackbox-immutable-result-" + uuid.NewString()
	immutableDead := "eq-blackbox-immutable-dead-" + uuid.NewString()
	t.Cleanup(func() {
		_, _ = f.rdb.Del(context.Background(), immutableSource, mutatedSource, immutableResult, immutableDead).Result()
	})
	config := echoqueue.QueueConfig{
		TaskName: "immutable-binding",
		Source:   immutableSource,
		Result:   immutableResult,
		Dead:     immutableDead,
	}
	queue, err := f.scheduler.Bind(config)
	if err != nil {
		t.Fatal(err)
	}
	config.Source = mutatedSource
	seedBlackboxRaw(t, blackboxFixture{rdb: f.rdb, source: immutableSource}, `{"task_id":"immutable-task","retry_count":0,"payload":{"ok":true}}`)
	batch, err := queue.Dispatch(context.Background(), 1)
	if err != nil || len(batch.Tasks) != 1 || batch.Tasks[0].TaskID != "immutable-task" {
		t.Fatalf("immutable binding dispatch = %+v, err=%v", batch, err)
	}
	if got := readBlackboxList(t, blackboxFixture{rdb: f.rdb}, mutatedSource); len(got) != 0 {
		t.Fatalf("mutated source was used: %#v", got)
	}
}

func TestBlackBoxDispatchAtomicInputAndLimits(t *testing.T) {
	t.Run("empty and generated task id", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		empty, err := f.queue.Dispatch(context.Background(), 1)
		if err != nil || empty.ID != "" || len(empty.Tasks) != 0 {
			t.Fatalf("empty Dispatch = %+v, err=%v", empty, err)
		}
		seedBlackboxRaw(t, f, `{"payload":{"value":1}}`)
		batch, err := f.queue.Dispatch(context.Background(), 1)
		if err != nil || batch.ID == "" || len(batch.Tasks) != 1 || batch.Tasks[0].TaskID == "" {
			t.Fatalf("generated task Dispatch = %+v, err=%v", batch, err)
		}
	})

	t.Run("batch size bounds", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		if _, err := f.queue.Dispatch(context.Background(), 0); err == nil {
			t.Fatal("zero batch size was accepted")
		}
		if _, err := f.queue.Dispatch(context.Background(), 5); err == nil {
			t.Fatal("batch size above MaxBatchSize was accepted")
		}
	})

	t.Run("invalid task is not consumed", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		seedBlackboxRaw(t, f, `{"task_id":"missing-payload"}`)
		if _, err := f.queue.Dispatch(context.Background(), 1); err == nil {
			t.Fatal("task without payload was accepted")
		}
		if got := readBlackboxList(t, f, f.source); len(got) != 1 {
			t.Fatalf("invalid task was consumed: %#v", got)
		}
	})

	t.Run("duplicate task ids are not consumed", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		raw := `{"task_id":"duplicate","retry_count":0,"payload":{"ok":true}}`
		seedBlackboxRaw(t, f, raw, raw)
		if _, err := f.queue.Dispatch(context.Background(), 2); err == nil {
			t.Fatal("duplicate task ids were accepted")
		}
		if got := readBlackboxList(t, f, f.source); len(got) != 2 {
			t.Fatalf("duplicate input was partially consumed: %#v", got)
		}
	})

	t.Run("fractional retry count is not consumed", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		seedBlackboxRaw(t, f, `{"task_id":"fractional-retry","retry_count":1.5,"payload":{"ok":true}}`)
		if _, err := f.queue.Dispatch(context.Background(), 1); err == nil {
			t.Fatal("fractional retry count was accepted")
		}
		if got := readBlackboxList(t, f, f.source); len(got) != 1 {
			t.Fatalf("fractional retry input was consumed: %#v", got)
		}
	})

	t.Run("control task id is not consumed", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		seedBlackboxRaw(t, f, `{"task_id":"bad\u0001id","retry_count":0,"payload":{"ok":true}}`)
		if _, err := f.queue.Dispatch(context.Background(), 1); err == nil {
			t.Fatal("control-character task id was accepted")
		}
		if got := readBlackboxList(t, f, f.source); len(got) != 1 {
			t.Fatalf("control-character task input was consumed: %#v", got)
		}
	})

	t.Run("payload limit is enforced atomically", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2, func(cfg *echoqueue.Config) {
			cfg.MaxPayloadBytes = 8
			cfg.MaxBatchBytes = 128
		})
		seedBlackboxRaw(t, f, `{"task_id":"large","retry_count":0,"payload":"0123456789"}`)
		if _, err := f.queue.Dispatch(context.Background(), 1); err == nil {
			t.Fatal("oversized payload was accepted")
		}
		if got := readBlackboxList(t, f, f.source); len(got) != 1 {
			t.Fatalf("oversized input was consumed: %#v", got)
		}
	})

	t.Run("wrong source type is reported", func(t *testing.T) {
		f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 2)
		if err := f.rdb.Set(context.Background(), f.source, "wrong-type", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := f.queue.Dispatch(context.Background(), 1); err == nil {
			t.Fatal("wrong source type was accepted")
		}
	})
}

func TestBlackBoxSettleReplayAndValidation(t *testing.T) {
	f := newBlackboxFixture(t, 1, time.Second, time.Hour, 5*time.Millisecond, 2)
	seedBlackboxTask(t, f, "settle-task", 0)
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.scheduler.Settle(context.Background(), batch.ID, echoqueue.Outcome{
		RequestID: "unknown-task-request",
		Results:   []echoqueue.Result{{TaskID: "not-in-batch", Data: json.RawMessage(`true`)}},
	}); err == nil {
		t.Fatal("unknown task outcome was accepted")
	}

	first := resultOutcome("settle-task", "settle-request", `{"ok":true}`)
	applied, err := f.scheduler.Settle(context.Background(), batch.ID, first)
	if err != nil || applied.Status != echoqueue.ReceiptApplied {
		t.Fatalf("first Settle = %+v, err=%v", applied, err)
	}
	if applied.ResultCount != 1 || applied.Winner != "settle" {
		t.Fatalf("first Receipt = %+v", applied)
	}

	duplicate := resultOutcome("settle-task", "settle-request", `{ "ok": true }`)
	if got, err := f.scheduler.Settle(context.Background(), batch.ID, duplicate); err != nil || got.Status != echoqueue.ReceiptDuplicate {
		t.Fatalf("duplicate Settle = %+v, err=%v", got, err)
	}
	conflict := resultOutcome("settle-task", "settle-request", `{"ok":false}`)
	if got, err := f.scheduler.Settle(context.Background(), batch.ID, conflict); err != nil || got.Status != echoqueue.ReceiptConflict {
		t.Fatalf("conflict Settle = %+v, err=%v", got, err)
	}
	stale := resultOutcome("settle-task", "different-request", `{"ok":true}`)
	if got, err := f.scheduler.Settle(context.Background(), batch.ID, stale); err != nil || got.Status != echoqueue.ReceiptStale {
		t.Fatalf("stale Settle = %+v, err=%v", got, err)
	}

	records := readBlackboxList(t, f, f.result)
	if len(records) != 1 {
		t.Fatalf("result count = %d, want 1", len(records))
	}
	var record map[string]interface{}
	if err := json.Unmarshal([]byte(records[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record["task_id"] != "settle-task" || record["batch_id"] != batch.ID || record["effect_id"] == nil {
		t.Fatalf("result identity = %#v", record)
	}
}

func TestBlackBoxPendingSnapshotUsesDispatchPolicy(t *testing.T) {
	f := newBlackboxFixture(t, 1, time.Second, time.Hour, 5*time.Millisecond, 2)
	seedBlackboxTask(t, f, "snapshot-task", 0)
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	otherConfig := echoqueue.DefaultConfig()
	otherConfig.Namespace = f.namespace
	otherConfig.MaxRetry = 0
	otherConfig.MaxRetrySet = true
	otherScheduler, err := echoqueue.New(f.rdb, otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := otherScheduler.Settle(context.Background(), batch.ID, echoqueue.Outcome{
		RequestID: "snapshot-policy-request",
		Failures:  []echoqueue.Failure{{TaskID: "snapshot-task", Retryable: true, Reason: "temporary"}},
	})
	if err != nil || receipt.Status != echoqueue.ReceiptApplied || receipt.RetryCount != 1 {
		t.Fatalf("snapshot policy Receipt = %+v, err=%v", receipt, err)
	}
	retried := readBlackboxList(t, f, f.source)
	if len(retried) != 1 {
		t.Fatalf("snapshot policy source = %#v", retried)
	}
	var task echoqueue.Task
	if err := json.Unmarshal([]byte(retried[0]), &task); err != nil {
		t.Fatal(err)
	}
	if task.TaskID != "snapshot-task" || task.RetryCount != 1 {
		t.Fatalf("snapshot retry task = %+v", task)
	}
}

func TestBlackBoxRetryAndDeadLetterLifecycle(t *testing.T) {
	f := newBlackboxFixture(t, 1, 10*time.Millisecond, time.Hour, 5*time.Millisecond, 1)
	seedBlackboxTask(t, f, "lifecycle-task", 0)
	firstBatch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if runErr := runBlackboxUntil(t, f, func() bool { return len(readBlackboxList(t, f, f.source)) == 1 }); runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("first Recover Run = %v", runErr)
	}
	retried := readBlackboxList(t, f, f.source)
	var retryTask echoqueue.Task
	if err := json.Unmarshal([]byte(retried[0]), &retryTask); err != nil {
		t.Fatal(err)
	}
	if retryTask.TaskID != "lifecycle-task" || retryTask.RetryCount != 1 {
		t.Fatalf("recovered retry = %+v", retryTask)
	}

	secondBatch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil || secondBatch.Tasks[0].TaskID != firstBatch.Tasks[0].TaskID || secondBatch.Tasks[0].RetryCount != 1 {
		t.Fatalf("second Dispatch = %+v, err=%v", secondBatch, err)
	}
	time.Sleep(30 * time.Millisecond)
	if runErr := runBlackboxUntil(t, f, func() bool { return len(readBlackboxList(t, f, f.dead)) == 1 }); runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("second Recover Run = %v", runErr)
	}
	deadRecords := readBlackboxList(t, f, f.dead)
	var dead map[string]interface{}
	if err := json.Unmarshal([]byte(deadRecords[0]), &dead); err != nil {
		t.Fatal(err)
	}
	if dead["task_id"] != "lifecycle-task" || dead["batch_id"] != secondBatch.ID || dead["effect_id"] == nil {
		t.Fatalf("dead identity = %#v", dead)
	}
}

func TestBlackBoxReceiptTTLAndLateResponse(t *testing.T) {
	f := newBlackboxFixture(t, 0, 10*time.Millisecond, 60*time.Millisecond, 5*time.Millisecond, 1)
	seedBlackboxTask(t, f, "ttl-task", 0)
	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if runErr := runBlackboxUntil(t, f, func() bool { return len(readBlackboxList(t, f, f.dead)) == 1 }); runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Recover Run = %v", runErr)
	}

	outcome := resultOutcome("ttl-task", "late-worker", `{"ok":true}`)
	late, err := f.scheduler.Settle(context.Background(), batch.ID, outcome)
	if err != nil || late.Status != echoqueue.ReceiptStale {
		t.Fatalf("late Settle = %+v, err=%v", late, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		expired, settleErr := f.scheduler.Settle(context.Background(), batch.ID, outcome)
		if settleErr != nil {
			t.Fatal(settleErr)
		}
		if expired.Status == echoqueue.ReceiptNotFound {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Receipt did not expire through the public API")
}

func TestBlackBoxRedisTargetErrorsRetainBatch(t *testing.T) {
	cases := []struct {
		name      string
		maxRetry  int
		target    func(blackboxFixture) string
		failure   bool
		retryable bool
	}{
		{name: "result", maxRetry: 0, target: func(f blackboxFixture) string { return f.result }},
		{name: "source", maxRetry: 1, target: func(f blackboxFixture) string { return f.source }, failure: true, retryable: true},
		{name: "dead", maxRetry: 0, target: func(f blackboxFixture) string { return f.dead }, failure: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlackboxFixture(t, tc.maxRetry, time.Second, time.Hour, 5*time.Millisecond, 2)
			taskID := "target-error-" + tc.name
			seedBlackboxTask(t, f, taskID, 0)
			batch, err := f.queue.Dispatch(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.rdb.Set(context.Background(), tc.target(f), "wrong-type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			var outcome echoqueue.Outcome
			if tc.failure {
				outcome = echoqueue.Outcome{
					RequestID: "target-error-request-" + tc.name,
					Failures:  []echoqueue.Failure{{TaskID: taskID, Retryable: tc.retryable, Reason: "target failure"}},
				}
			} else {
				outcome = resultOutcome(taskID, "target-error-request-"+tc.name, `{"ok":true}`)
			}
			if _, err := f.scheduler.Settle(context.Background(), batch.ID, outcome); err == nil {
				t.Fatal("wrong target type was accepted")
			}
			if err := f.rdb.Del(context.Background(), tc.target(f)).Err(); err != nil {
				t.Fatal(err)
			}
			receipt, err := f.scheduler.Settle(context.Background(), batch.ID, outcome)
			if err != nil || receipt.Status != echoqueue.ReceiptApplied {
				t.Fatalf("retry after target repair = %+v, err=%v", receipt, err)
			}
		})
	}
}

func TestBlackBoxContextCancellationAndRunLifecycle(t *testing.T) {
	f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 1)
	seedBlackboxTask(t, f, "cancel-task", 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.queue.Dispatch(ctx, 1); err == nil {
		t.Fatal("cancelled Dispatch unexpectedly succeeded")
	}
	if len(readBlackboxList(t, f, f.source)) != 1 {
		t.Fatal("cancelled Dispatch consumed source task")
	}

	batch, err := f.queue.Dispatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	settleCtx, settleCancel := context.WithCancel(context.Background())
	settleCancel()
	if _, err := f.scheduler.Settle(settleCtx, batch.ID, resultOutcome("cancel-task", "cancelled-settle", `true`)); err == nil {
		t.Fatal("cancelled Settle unexpectedly succeeded")
	}
	if receipt, err := f.scheduler.Settle(context.Background(), batch.ID, resultOutcome("cancel-task", "after-cancel", `true`)); err != nil || receipt.Status != echoqueue.ReceiptApplied {
		t.Fatalf("Settle after cancellation = %+v, err=%v", receipt, err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	firstRun := make(chan error, 1)
	go func() { firstRun <- f.scheduler.Run(runCtx) }()
	time.Sleep(50 * time.Millisecond)
	if err := f.scheduler.Run(context.Background()); !errors.Is(err, echoqueue.ErrRunAlreadyActive) {
		t.Fatalf("duplicate Run = %v", err)
	}
	runCancel()
	select {
	case err := <-firstRun:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first Run after cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Run did not stop")
	}
	alreadyCancelled, stop := context.WithCancel(context.Background())
	stop()
	if err := f.scheduler.Run(alreadyCancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-cancelled Run = %v", err)
	}
}

func TestBlackBoxSettleAndRecoverFirstWinner(t *testing.T) {
	f := newBlackboxFixture(t, 0, 10*time.Millisecond, time.Hour, 2*time.Millisecond, 1)
	for i := 0; i < 8; i++ {
		taskID := "race-task-" + uuid.NewString()
		seedBlackboxTask(t, f, taskID, 0)
		batch, err := f.queue.Dispatch(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)

		runCtx, runCancel := context.WithCancel(context.Background())
		runResult := make(chan error, 1)
		settleResult := make(chan struct {
			receipt echoqueue.Receipt
			err     error
		}, 1)
		start := make(chan struct{})
		go func() {
			<-start
			runResult <- f.scheduler.Run(runCtx)
		}()
		go func() {
			<-start
			receipt, settleErr := f.scheduler.Settle(context.Background(), batch.ID, resultOutcome(taskID, "race-request-"+taskID, `true`))
			settleResult <- struct {
				receipt echoqueue.Receipt
				err     error
			}{receipt, settleErr}
		}()
		close(start)

		var settled struct {
			receipt echoqueue.Receipt
			err     error
		}
		select {
		case settled = <-settleResult:
		case <-time.After(2 * time.Second):
			runCancel()
			t.Fatal("Settle did not finish in race test")
		}
		if settled.err != nil {
			runCancel()
			t.Fatalf("race Settle error: %v", settled.err)
		}
		if settled.receipt.Status != echoqueue.ReceiptApplied && settled.receipt.Status != echoqueue.ReceiptStale {
			runCancel()
			t.Fatalf("race Settle status = %s", settled.receipt.Status)
		}
		runCancel()
		select {
		case runErr := <-runResult:
			if !errors.Is(runErr, context.Canceled) {
				t.Fatalf("race Run error: %v", runErr)
			}
		case <-time.After(time.Second):
			t.Fatal("race Run did not stop")
		}
	}
	if got := len(readBlackboxList(t, f, f.result)) + len(readBlackboxList(t, f, f.dead)); got != 8 {
		t.Fatalf("race effects = %d, want 8", got)
	}
}

func TestBlackBoxPublicErrorsRemainObservable(t *testing.T) {
	f := newBlackboxFixture(t, 0, time.Second, time.Hour, 5*time.Millisecond, 1)
	if _, err := f.scheduler.Settle(context.Background(), "", resultOutcome("task", "request", `true`)); err == nil {
		t.Fatal("empty batch ID was accepted")
	}
	if _, err := f.scheduler.Settle(context.Background(), "missing-batch", echoqueue.Outcome{RequestID: "request", Results: []echoqueue.Result{{TaskID: "task", Data: json.RawMessage(`true`)}}}); err != nil {
		t.Fatalf("missing batch returned an unexpected error: %v", err)
	}
	if receipt, err := f.scheduler.Settle(context.Background(), "missing-batch", echoqueue.Outcome{RequestID: "request", Results: []echoqueue.Result{{TaskID: "task", Data: json.RawMessage(`true`)}}}); err != nil || receipt.Status != echoqueue.ReceiptNotFound {
		t.Fatalf("missing batch response = %+v, err=%v", receipt, err)
	}
}

func TestBlackBoxRedisAddressIsNotRequiredForUnitPackage(t *testing.T) {
	// This test is intentionally not a Redis probe: it documents that New only
	// validates the public constructor inputs. All Redis behavior is covered by
	// the integration-tagged tests above against a real server.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	scheduler, err := echoqueue.New(client, echoqueue.Config{Namespace: "blackbox-constructor"})
	if err != nil || scheduler == nil {
		t.Fatalf("constructor = %v, scheduler=%v", err, scheduler)
	}
}
