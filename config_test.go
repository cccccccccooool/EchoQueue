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

func TestSettleTransientErrorClassification(t *testing.T) {
	// A Settle against an unreachable Redis must classify as a transient
	// Redis interaction failure so hosts can trip a circuit breaker, while
	// validation rejections must not.
	scheduler, err := New(testClient(), Config{Namespace: "transient"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = scheduler.Settle(ctx, "transient-batch", Outcome{
		RequestID: "transient-request",
		Results:   []Result{{TaskID: "t", Data: []byte(`{"ok":true}`)}},
	})
	if err == nil {
		t.Fatal("Settle against an unreachable Redis unexpectedly succeeded")
	}
	if !errors.Is(err, ErrTransientRedis) {
		t.Fatalf("unreachable-Redis Settle error = %v, want ErrTransientRedis", err)
	}
	if !errors.Is(err, errSettle) {
		t.Fatalf("unreachable-Redis Settle error = %v, want errSettle preserved", err)
	}
}

func TestConfigNormalizationBoundaries(t *testing.T) {
	badUTF8 := string([]byte{0xff, 0xfe})
	tests := []struct {
		name   string
		config Config
		want   func(Config) bool
	}{
		{name: "all zero uses defaults", config: Config{Namespace: "all-zero"},
			want: func(c Config) bool {
				return c.MaxRetry == 3 && c.MaxRetrySet && c.VisibilityTimeout == 30*time.Second && c.ReceiptTTL == 24*time.Hour && c.MaxBatchSize == 1000 && c.MaxPayloadBytes == 1<<20 && c.MaxBatchBytes == 64<<20 && c.RunInterval == 500*time.Millisecond && c.RunBatchSize == 32
			}},
		{name: "unset max retry uses default", config: Config{Namespace: "unset-retry"},
			want: func(c Config) bool { return c.MaxRetry == 3 }},
		{name: "explicit zero max retry stays zero", config: Config{Namespace: "zero-retry", MaxRetrySet: true, MaxRetry: 0},
			want: func(c Config) bool { return c.MaxRetry == 0 && c.MaxRetrySet }},
		{name: "one millisecond visibility", config: Config{Namespace: "vis-1ms", VisibilityTimeout: time.Millisecond},
			want: func(c Config) bool { return c.VisibilityTimeout == time.Millisecond }},
		{name: "one millisecond receipt ttl", config: Config{Namespace: "ttl-1ms", ReceiptTTL: time.Millisecond},
			want: func(c Config) bool { return c.ReceiptTTL == time.Millisecond }},
		{name: "payload below batch bytes", config: Config{Namespace: "payload", MaxPayloadBytes: 1000, MaxBatchBytes: 2000},
			want: func(c Config) bool { return c.MaxPayloadBytes == 1000 && c.MaxBatchBytes == 2000 }},
		{name: "explicit batch limits", config: Config{Namespace: "limits", MaxBatchSize: 10, MaxPayloadBytes: 500, MaxBatchBytes: 900, RunBatchSize: 7, RunInterval: 250 * time.Millisecond},
			want: func(c Config) bool {
				return c.MaxBatchSize == 10 && c.MaxPayloadBytes == 500 && c.MaxBatchBytes == 900 && c.RunBatchSize == 7 && c.RunInterval == 250*time.Millisecond
			}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheduler, err := New(testClient(), tc.config)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if !tc.want(scheduler.config) {
				t.Fatalf("normalized config = %+v", scheduler.config)
			}
		})
	}
	rejected := []struct {
		name   string
		config Config
	}{
		{name: "negative max retry", config: Config{Namespace: "neg-retry", MaxRetry: -1}},
		{name: "visibility below one millisecond", config: Config{Namespace: "vis-subms", VisibilityTimeout: time.Microsecond}},
		{name: "negative visibility", config: Config{Namespace: "vis-neg", VisibilityTimeout: -time.Second}},
		{name: "receipt ttl below one millisecond", config: Config{Namespace: "ttl-subms", ReceiptTTL: 999 * time.Microsecond}},
		{name: "negative receipt ttl", config: Config{Namespace: "ttl-neg", ReceiptTTL: -time.Second}},
		{name: "negative max batch size", config: Config{Namespace: "neg-batch", MaxBatchSize: -1}},
		{name: "negative payload bytes", config: Config{Namespace: "neg-payload", MaxPayloadBytes: -1}},
		{name: "negative batch bytes", config: Config{Namespace: "neg-batch-bytes", MaxBatchBytes: -1}},
		{name: "batch bytes below payload bytes", config: Config{Namespace: "crossed", MaxPayloadBytes: 2048, MaxBatchBytes: 1024}},
		{name: "negative run batch size", config: Config{Namespace: "neg-run-batch", RunBatchSize: -2}},
		{name: "negative run interval", config: Config{Namespace: "neg-run-interval", RunInterval: -time.Second}},
		{name: "empty namespace", config: Config{Namespace: ""}},
		{name: "blank namespace", config: Config{Namespace: "   "}},
		{name: "control character namespace", config: Config{Namespace: "ns\x01"}},
		{name: "nul namespace", config: Config{Namespace: "ns\x00ns"}},
		{name: "invalid utf8 namespace", config: Config{Namespace: badUTF8}},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(testClient(), tc.config); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestQueueConfigTextValidation(t *testing.T) {
	badUTF8 := string([]byte{0xff, 0xfe})
	tests := []struct {
		name   string
		config QueueConfig
	}{
		{name: "empty task name", config: QueueConfig{TaskName: "", Source: "src"}},
		{name: "blank task name", config: QueueConfig{TaskName: "  ", Source: "src"}},
		{name: "control task name", config: QueueConfig{TaskName: "task\x02", Source: "src"}},
		{name: "nul task name", config: QueueConfig{TaskName: "task\x00x", Source: "src"}},
		{name: "invalid utf8 task name", config: QueueConfig{TaskName: badUTF8, Source: "src"}},
		{name: "empty source", config: QueueConfig{TaskName: "task", Source: ""}},
		{name: "blank source", config: QueueConfig{TaskName: "task", Source: " "}},
		{name: "control source", config: QueueConfig{TaskName: "task", Source: "src\x1f"}},
		{name: "nul source", config: QueueConfig{TaskName: "task", Source: "src\x00"}},
		{name: "invalid utf8 source", config: QueueConfig{TaskName: "task", Source: badUTF8}},
		{name: "control result", config: QueueConfig{TaskName: "task", Source: "src", Result: "res\x07"}},
		{name: "invalid utf8 result", config: QueueConfig{TaskName: "task", Source: "src", Result: badUTF8}},
		{name: "control dead", config: QueueConfig{TaskName: "task", Source: "src", Dead: "dead\x08"}},
		{name: "invalid utf8 dead", config: QueueConfig{TaskName: "task", Source: "src", Dead: badUTF8}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.config.normalized(); err == nil {
				t.Fatal("invalid queue config was accepted")
			}
		})
	}
	valid := []struct {
		name   string
		config QueueConfig
	}{
		{name: "minimal", config: QueueConfig{TaskName: "task", Source: "src"}},
		{name: "unicode keys", config: QueueConfig{TaskName: "发票任务", Source: "发票:来源", Result: "发票:结果", Dead: "发票:死信"}},
		{name: "cjk and emoji", config: QueueConfig{TaskName: "task-队列", Source: "source-队列-🚀", Result: "", Dead: ""}},
		{name: "empty result and dead", config: QueueConfig{TaskName: "task", Source: "src", Result: "", Dead: ""}},
		{name: "non empty result and dead", config: QueueConfig{TaskName: "task", Source: "src", Result: "res", Dead: "dead"}},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.config.normalized(); err != nil {
				t.Fatalf("valid queue config rejected: %v", err)
			}
		})
	}
}

func TestQueueConfigRouteConflicts(t *testing.T) {
	rejected := []struct {
		name   string
		config QueueConfig
	}{
		{name: "result equals source", config: QueueConfig{TaskName: "task", Source: "same", Result: "same"}},
		{name: "dead equals source", config: QueueConfig{TaskName: "task", Source: "same", Dead: "same"}},
		{name: "result equals dead", config: QueueConfig{TaskName: "task", Source: "src", Result: "both", Dead: "both"}},
		{name: "result equals dead with distinct source", config: QueueConfig{TaskName: "task", Source: "both", Result: "other", Dead: "other"}},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.config.normalized(); err == nil {
				t.Fatal("conflicting routes were accepted")
			}
		})
	}
	if _, err := (QueueConfig{TaskName: "task", Source: "src", Result: "res", Dead: "dead"}).normalized(); err != nil {
		t.Fatalf("distinct routes rejected: %v", err)
	}
	if _, err := (QueueConfig{TaskName: "task", Source: "src", Result: "res"}).normalized(); err != nil {
		t.Fatalf("distinct routes with empty dead rejected: %v", err)
	}
}

func TestValidateTextRejectsControlAndInvalidUTF8(t *testing.T) {
	badUTF8 := string([]byte{0xc3, 0x28})
	rejected := []string{"\x00", "\x01", "\x1f", "\x7f", badUTF8}
	for _, value := range rejected {
		if err := validateText("field", value, false); err == nil {
			t.Fatalf("text %q was accepted", value)
		}
	}
	accepted := []string{"", "normal", "中文", "emoji-🚀"}
	for _, value := range accepted {
		if err := validateText("field", value, false); err != nil {
			t.Fatalf("text %q rejected: %v", value, err)
		}
	}
	if err := validateText("field", "", true); err == nil {
		t.Fatal("empty required text was accepted")
	}
	if err := validateText("field", "  ", true); err == nil {
		t.Fatal("blank required text was accepted")
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
