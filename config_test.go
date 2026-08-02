package echoqueue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
}

func TestConfigDefaultsAndExplicitZero(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "unit"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if scheduler.config.MaxRetry != 3 || scheduler.config.VisibilityTimeout != 30*time.Second || scheduler.config.ReceiptTTL != 24*time.Hour {
		t.Fatalf("defaults = %+v", scheduler.config)
	}
	noRetry, err := New(testClient(), Config{Namespace: "unit-no-retry", MaxRetrySet: true, MaxRetry: 0})
	if err != nil {
		t.Fatalf("New explicit zero: %v", err)
	}
	if noRetry.config.MaxRetry != 0 {
		t.Fatalf("explicit zero became %d", noRetry.config.MaxRetry)
	}
	if _, err := New(testClient(), Config{Namespace: "invalid-receipt-ttl", ReceiptTTL: -time.Second}); err == nil {
		t.Fatal("negative receipt ttl was accepted")
	}
}

func TestBindCopiesQueueConfigAndRejectsDuplicates(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "bind"})
	if err != nil {
		t.Fatal(err)
	}
	input := QueueConfig{TaskName: "email", Source: "source", Result: "result"}
	queue, err := scheduler.Bind(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Source = "mutated"
	if queue.config.Source != "source" {
		t.Fatalf("queue was not immutable: %+v", queue.config)
	}
	if _, err := scheduler.Bind(QueueConfig{TaskName: "email", Source: "other"}); err == nil {
		t.Fatal("duplicate task binding was accepted")
	}
}

func TestBindRejectsPhysicalRouteConflicts(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "physical-routes"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Bind(QueueConfig{TaskName: "first", Source: "shared-source", Result: "first-result", Dead: "first-dead"}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		config QueueConfig
		want   string
	}{
		{name: "source", config: QueueConfig{TaskName: "second", Source: "shared-source", Result: "second-result", Dead: "second-dead"}, want: "source"},
		{name: "result", config: QueueConfig{TaskName: "third", Source: "third-source", Result: "first-result", Dead: "third-dead"}, want: "result"},
		{name: "dead", config: QueueConfig{TaskName: "fourth", Source: "fourth-source", Result: "fourth-result", Dead: "first-dead"}, want: "dead"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scheduler.Bind(tc.config)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Bind error = %v, want %q conflict", err, tc.want)
			}
		})
	}
	if _, err := scheduler.Bind(QueueConfig{TaskName: "self-conflict", Source: scheduler.keys.result("self-conflict")}); err == nil {
		t.Fatal("source was allowed to reuse its implicit result key")
	}
	if _, err := scheduler.Bind(QueueConfig{TaskName: "valid", Source: "valid-source"}); err != nil {
		t.Fatalf("valid independent route was rejected: %v", err)
	}
}

func TestBindPhysicalConflictCheckIsConcurrent(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "physical-routes-concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	var wg sync.WaitGroup
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, bindErr := scheduler.Bind(QueueConfig{
				TaskName: "task-" + string(rune('a'+i)),
				Source:   "one-physical-source",
			})
			results <- bindErr
		}(i)
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent Bind successes = %d, want 1", successes)
	}
}

func TestTaskAndOutcomeValidation(t *testing.T) {
	task := Task{TaskID: "task-1", Payload: []byte(`{"value":1}`)}
	if err := task.validate(); err != nil {
		t.Fatal(err)
	}
	outcome := Outcome{
		RequestID: "request-1",
		Results:   []Result{{TaskID: "task-1", Data: []byte(`{"ok":true}`)}},
	}
	if err := outcome.validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Outcome{}).validate(); err == nil {
		t.Fatal("empty outcome was accepted")
	}
}

func TestCommandHashIsStableAcrossOrderingAndJSONWhitespace(t *testing.T) {
	first := Outcome{
		RequestID: "request-1",
		Results: []Result{
			{TaskID: "b", Data: []byte(` { "n": 2 } `)},
			{TaskID: "a", Data: []byte(`{"n":1}`)},
		},
		Failures: []Failure{{TaskID: "c", Reason: "timeout", Retryable: true}},
	}
	second := Outcome{
		RequestID: "request-1",
		Results: []Result{
			{TaskID: "a", Data: []byte(`{"n":1}`)},
			{TaskID: "b", Data: []byte(`{"n":2}`)},
		},
		Failures: []Failure{{TaskID: "c", Reason: "timeout", Retryable: true}},
	}
	left, err := commandHash("batch-1", first)
	if err != nil {
		t.Fatal(err)
	}
	right, err := commandHash("batch-1", second)
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("hashes differ: %s != %s", left, right)
	}
}

func TestRunCancellationAndDuplicateGuard(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "run"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scheduler.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation error = %v", err)
	}
	if !scheduler.beginRun() {
		t.Fatal("failed to reserve run state")
	}
	if err := scheduler.Run(context.Background()); !errors.Is(err, ErrRunAlreadyActive) {
		t.Fatalf("duplicate Run error = %v", err)
	}
	scheduler.endRun()
}

func TestDispatchContextCancellationFailsBeforeRedisWrite(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "cancel"})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := scheduler.Bind(QueueConfig{TaskName: "task", Source: "source"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queue.Dispatch(ctx, 1); err == nil {
		t.Fatal("cancelled Dispatch unexpectedly succeeded")
	}
}

func TestRedisCapabilityCheckIsCachedAndRetryable(t *testing.T) {
	scheduler, err := New(testClient(), Config{Namespace: "redis-cache"})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	scheduler.redisProbe = func(context.Context, *redis.Client) error {
		calls.Add(1)
		return nil
	}
	const workers = 32
	var wg sync.WaitGroup
	errorsCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errorsCh <- scheduler.ensureRedis(context.Background())
		}()
	}
	wg.Wait()
	close(errorsCh)
	for probeErr := range errorsCh {
		if probeErr != nil {
			t.Fatalf("cached probe error = %v", probeErr)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("successful probe calls = %d, want 1", got)
	}

	retryScheduler, err := New(testClient(), Config{Namespace: "redis-cache-retry"})
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	retryScheduler.redisProbe = func(context.Context, *redis.Client) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary probe failure")
		}
		return nil
	}
	if err := retryScheduler.ensureRedis(context.Background()); err == nil {
		t.Fatal("first failed probe returned nil")
	}
	if err := retryScheduler.ensureRedis(context.Background()); err != nil {
		t.Fatalf("retry probe failed: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("retry probe calls = %d, want 2", got)
	}
}
