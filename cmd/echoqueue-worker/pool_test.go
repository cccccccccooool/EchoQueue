package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
)

func pollUntil(t *testing.T, timeout time.Duration, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !condition() {
		t.Fatalf("%s did not become true within %s", label, timeout)
	}
}

func quickPool(t *testing.T) *BoundedPool {
	t.Helper()
	pool, err := NewPool(PoolConfig{
		Workers:       2,
		MaxInFlight:   3,
		Buffer:        3,
		BatchSize:     1,
		PollInterval:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return pool
}

type dispatchRecorder struct {
	mu       sync.Mutex
	calls    atomic.Int64
	inflight int64
	peak     int64
	err      error
	errEvery int64
	batch    echoqueue.Batch
}

func (r *dispatchRecorder) Dispatch(ctx context.Context, batchSize int) (echoqueue.Batch, error) {
	r.calls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil && r.calls.Load()%r.errEvery == 0 {
		return echoqueue.Batch{}, r.err
	}
	r.inflight++
	if r.inflight > r.peak {
		r.peak = r.inflight
	}
	return r.batch, nil
}

type settleRecorder struct {
	mu       sync.Mutex
	calls    atomic.Int64
	peak     int64
	inflight int64
	err      error
	outcomes []echoqueue.Outcome
}

func (r *settleRecorder) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	r.calls.Add(1)
	r.mu.Lock()
	r.inflight++
	if r.inflight > r.peak {
		r.peak = r.inflight
	}
	r.outcomes = append(r.outcomes, outcome)
	err := r.err
	r.mu.Unlock()
	time.Sleep(time.Millisecond)
	r.mu.Lock()
	r.inflight--
	r.mu.Unlock()
	if err != nil {
		return echoqueue.Receipt{Status: echoqueue.ReceiptInvalid, BatchID: batchID}, err
	}
	return echoqueue.Receipt{Status: echoqueue.ReceiptApplied, BatchID: batchID}, nil
}

func okOutcome(batch echoqueue.Batch) echoqueue.Outcome {
	outcome := echoqueue.Outcome{RequestID: "w-" + batch.ID}
	for _, task := range batch.Tasks {
		outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
	}
	return outcome
}

func TestPoolNewPoolPrefillsAllTokens(t *testing.T) {
	pool := quickPool(t)
	if got := len(pool.permits); got != 3 {
		t.Fatalf("permits prefilled = %d, want 3", got)
	}
	if got := len(pool.slots); got != 3 {
		t.Fatalf("slots prefilled = %d, want 3", got)
	}
	if got := cap(pool.batches); got != 3 {
		t.Fatalf("batches capacity = %d, want 3", got)
	}
}

func TestPoolDispatchedButUnsettledNeverExceedsMaxInFlight(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	hold := make(chan struct{})
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-hold
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "pool saturation", func() bool { return dispatch.calls.Load() >= 3 })
	time.Sleep(50 * time.Millisecond)
	var peakUnsettled int64
	for i := 0; i < 20; i++ {
		dispatched := dispatch.calls.Load()
		done := settle.calls.Load()
		unsettled := dispatched - done
		if unsettled > peakUnsettled {
			peakUnsettled = unsettled
		}
		time.Sleep(5 * time.Millisecond)
	}
	if peakUnsettled > 3 {
		t.Fatalf("dispatched-but-unsettled peak = %d, exceeds max_in_flight 3", peakUnsettled)
	}
	close(hold)
	pollUntil(t, 2*time.Second, "settle completion", func() bool { return settle.calls.Load() >= 6 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPoolBackpressureFrozensDispatchCount(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	hold := make(chan struct{})
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-hold
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "pool saturation", func() bool { return dispatch.calls.Load() >= 3 })
	time.Sleep(100 * time.Millisecond)
	frozen := dispatch.calls.Load()
	time.Sleep(100 * time.Millisecond)
	if got := dispatch.calls.Load(); got != frozen {
		t.Fatalf("dispatch kept growing while pool was full: %d -> %d", frozen, got)
	}
	if frozen != 3 {
		t.Fatalf("dispatch froze at %d, want 3 (= max_in_flight)", frozen)
	}
	close(hold)
	pollUntil(t, 2*time.Second, "dispatch resumption", func() bool { return dispatch.calls.Load() > 10 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPoolEmptyQueueKeepsPollingWithoutLosingCapacity(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "empty polling", func() bool { return dispatch.calls.Load() >= 20 })
	if settle.calls.Load() != 0 {
		t.Fatalf("settle called %d times for an empty source", settle.calls.Load())
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	if got := len(pool.permits); got != 3 {
		t.Fatalf("permits after shutdown = %d, want 3", got)
	}
}

func TestPoolDispatchErrorReportsAndContinues(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{err: errors.New("boom"), errEvery: 2}
	settle := &settleRecorder{}
	var reported atomic.Int64
	var lastErr atomic.Value
	report := func(err error) {
		reported.Add(1)
		lastErr.Store(fmt.Sprint(err))
	}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, report) }()
	pollUntil(t, 2*time.Second, "dispatch errors reported", func() bool { return reported.Load() >= 2 })
	if text := fmt.Sprint(lastErr.Load()); !strings.Contains(text, "boom") {
		t.Fatalf("reported error = %q", text)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPoolSettleErrorReportedAndBatchNotTreatedAsSuccess(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{err: errors.New("settle-failed")}
	var settleErrors atomic.Int64
	report := func(err error) {
		if strings.Contains(fmt.Sprint(err), "settle-failed") {
			settleErrors.Add(1)
		}
	}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, report) }()
	pollUntil(t, 2*time.Second, "settle errors reported", func() bool { return settleErrors.Load() >= 2 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPoolCancelBeforeDispatchNeverDispatches(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	if err := pool.Run(ctx, dispatch, settle, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }, func(error) {}); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if dispatch.calls.Load() != 0 {
		t.Fatalf("dispatch called %d times on a cancelled context", dispatch.calls.Load())
	}
	if settle.calls.Load() != 0 {
		t.Fatalf("settle called %d times on a cancelled context", settle.calls.Load())
	}
}

func TestPoolCancelNeverCreatesExtraSettles(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-ctx.Done()
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "dispatch started", func() bool { return dispatch.calls.Load() >= 1 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	// The pool may finish a best-effort settle for a batch a worker already
	// owns, but it must never settle more batches than it dispatched.
	if settle.calls.Load() > dispatch.calls.Load() {
		t.Fatalf("settle calls = %d exceed dispatched batches = %d", settle.calls.Load(), dispatch.calls.Load())
	}
	pollUntil(t, 2*time.Second, "token restoration", func() bool { return len(pool.permits) == 3 && len(pool.slots) == 3 })
}

func TestPoolDuplicateRunIsRejected(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "first run started", func() bool { return dispatch.calls.Load() >= 1 })
	if err := pool.Run(context.Background(), dispatch, settle, handle, func(error) {}); !errors.Is(err, errPoolAlreadyActive) {
		t.Fatalf("duplicate Run error = %v", err)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	restartCtx, restartCancel := context.WithCancel(context.Background())
	restartDone := make(chan error, 1)
	go func() { restartDone <- pool.Run(restartCtx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "restart dispatch", func() bool { return dispatch.calls.Load() >= 2 })
	restartCancel()
	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatalf("restarted Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("restarted Run did not stop")
	}
}

func TestPoolRepeatedShutdownRestoresFullCapacityWithoutLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 6; i++ {
		pool := quickPool(t)
		ctx, cancel := context.WithCancel(context.Background())
		dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
		settle := &settleRecorder{}
		runDone := make(chan error, 1)
		go func() {
			runDone <- pool.Run(ctx, dispatch, settle, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }, func(error) {})
		}()
		pollUntil(t, 2*time.Second, "settle progress", func() bool { return settle.calls.Load() >= 3 })
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("Run = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not stop")
		}
		pollUntil(t, 2*time.Second, "permit restoration", func() bool { return len(pool.permits) == 3 && len(pool.slots) == 3 })
	}
	pollUntil(t, 3*time.Second, "goroutine drain", func() bool { return runtime.NumGoroutine() <= before+5 })
	if got := runtime.NumGoroutine(); got > before+5 {
		t.Fatalf("goroutines grew from %d to %d", before, got)
	}
}

func TestPoolWorkerCountBoundsActiveHandlers(t *testing.T) {
	pool := quickPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	var activeHandlers atomic.Int64
	var peakHandlers atomic.Int64
	hold := make(chan struct{})
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		value := activeHandlers.Add(1)
		for {
			current := peakHandlers.Load()
			if value <= current || peakHandlers.CompareAndSwap(current, value) {
				break
			}
		}
		<-hold
		activeHandlers.Add(-1)
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "handler saturation", func() bool { return peakHandlers.Load() >= 2 })
	time.Sleep(50 * time.Millisecond)
	if got := peakHandlers.Load(); got > 2 {
		t.Fatalf("peak active handlers = %d, exceeds workers 2", got)
	}
	close(hold)
	pollUntil(t, 2*time.Second, "settle completion", func() bool { return settle.calls.Load() >= 5 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPoolConfigValidation(t *testing.T) {
	if _, err := NewPool(PoolConfig{}); err != nil {
		t.Fatalf("zero config: %v", err)
	}
	for name, cfg := range map[string]PoolConfig{
		"negative workers":    {Workers: -1},
		"negative in flight":  {Workers: 1, MaxInFlight: -1},
		"negative buffer":     {Workers: 1, MaxInFlight: 1, Buffer: -1},
		"negative batch size": {Workers: 1, MaxInFlight: 1, Buffer: 1, BatchSize: -1},
		"negative poll":       {Workers: 1, MaxInFlight: 1, Buffer: 1, BatchSize: 1, PollInterval: -time.Second},
		"negative grace":      {Workers: 1, MaxInFlight: 1, Buffer: 1, BatchSize: 1, PollInterval: time.Millisecond, ShutdownGrace: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPool(cfg); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	if pool, err := NewPool(PoolConfig{Workers: 1, MaxInFlight: 1, Buffer: 1, BatchSize: 1, PollInterval: time.Millisecond, ShutdownGrace: time.Millisecond}); err != nil || len(pool.permits) != 1 || len(pool.slots) != 1 {
		t.Fatalf("explicit one-token pool = %v, err=%v", pool, err)
	}
}
