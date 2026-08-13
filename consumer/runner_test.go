package consumer

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

// currentStageSizes reads the per-Run pool sizes through the runner's
// poolMu, matching the synchronization Resize* and Run use for the same
// fields.
func currentStageSizes(r *Runner) (dispatchers, workers, settlers int) {
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	if r.curDispatch != nil {
		dispatchers = r.curDispatch.size()
	}
	if r.curWorkers != nil {
		workers = r.curWorkers.size()
	}
	if r.curSettlers != nil {
		settlers = r.curSettlers.size()
	}
	return dispatchers, workers, settlers
}

func quickRunner(t *testing.T) *Runner {
	t.Helper()
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   3,
		BatchSize:     1,
		BatchBuffer:   3,
		OutcomeBuffer: 3,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runner
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
	if r.err != nil && r.calls.Load()%r.errEvery == 0 {
		r.mu.Unlock()
		return echoqueue.Batch{}, r.err
	}
	r.inflight++
	if r.inflight > r.peak {
		r.peak = r.inflight
	}
	r.mu.Unlock()
	// Hold the "call" briefly so overlapping dispatcher goroutines are
	// observable through peakConcurrent.
	time.Sleep(2 * time.Millisecond)
	r.mu.Lock()
	r.inflight--
	r.mu.Unlock()
	return r.batch, nil
}

func (r *dispatchRecorder) callCount() int64 {
	return r.calls.Load()
}

func (r *dispatchRecorder) peakConcurrent() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peak
}

func (r *dispatchRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inflight = 0
	r.peak = 0
}

type settleRecorder struct {
	mu       sync.Mutex
	calls    atomic.Int64
	peak     int64
	inflight int64
	err      error
	flaky    int64
	outcomes []echoqueue.Outcome
}

func (r *settleRecorder) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	r.calls.Add(1)
	r.mu.Lock()
	r.inflight++
	if r.inflight > r.peak {
		r.peak = r.inflight
	}
	err := r.err
	if r.flaky > 0 && r.calls.Load() <= r.flaky {
		err = errors.New("settle-transient")
	}
	r.outcomes = append(r.outcomes, outcome)
	r.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	r.mu.Lock()
	r.inflight--
	r.mu.Unlock()
	if err != nil {
		// A status other than ReceiptInvalid keeps transient Redis failures
		// distinct from permanent validation rejections.
		return echoqueue.Receipt{Status: echoqueue.ReceiptStale, BatchID: batchID}, err
	}
	return echoqueue.Receipt{Status: echoqueue.ReceiptApplied, BatchID: batchID}, nil
}

func (r *settleRecorder) peakConcurrent() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peak
}

type recordingMetrics struct {
	mu              sync.Mutex
	dispatchStarted atomic.Int64
	dispatchEmpty   atomic.Int64
	dispatchFailed  atomic.Int64
	batchDispatched atomic.Int64
	handleDone      atomic.Int64
	settleStarted   atomic.Int64
	settleSucceeded atomic.Int64
	settleFailed    atomic.Int64
	breakerOpened   atomic.Int64
	breakerClosed   atomic.Int64
}

func (m *recordingMetrics) DispatchStarted() { m.dispatchStarted.Add(1) }
func (m *recordingMetrics) DispatchEmpty()   { m.dispatchEmpty.Add(1) }
func (m *recordingMetrics) DispatchFailed()  { m.dispatchFailed.Add(1) }
func (m *recordingMetrics) BatchDispatched(int) {
	m.batchDispatched.Add(1)
}
func (m *recordingMetrics) HandleStarted() {}
func (m *recordingMetrics) HandleDone()    { m.handleDone.Add(1) }
func (m *recordingMetrics) SettleStarted() { m.settleStarted.Add(1) }
func (m *recordingMetrics) SettleSucceeded() {
	m.settleSucceeded.Add(1)
}
func (m *recordingMetrics) SettleFailed() { m.settleFailed.Add(1) }
func (m *recordingMetrics) BreakerOpened() {
	m.breakerOpened.Add(1)
}
func (m *recordingMetrics) BreakerClosed() {
	m.breakerClosed.Add(1)
}

func okOutcome(batch echoqueue.Batch) echoqueue.Outcome {
	outcome := echoqueue.Outcome{RequestID: "w-" + batch.ID}
	for _, task := range batch.Tasks {
		outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
	}
	return outcome
}

func TestRunnerNewPrefillsAllTokens(t *testing.T) {
	runner := quickRunner(t)
	if got := len(runner.permits); got != 3 {
		t.Fatalf("permits prefilled = %d, want 3", got)
	}
	if got := len(runner.slots); got != 3 {
		t.Fatalf("slots prefilled = %d, want 3", got)
	}
	if got := cap(runner.batches); got != 3 {
		t.Fatalf("batches capacity = %d, want 3", got)
	}
	if got := cap(runner.outcomes); got != 3 {
		t.Fatalf("outcomes capacity = %d, want 3", got)
	}
}

func TestRunnerDispatchedButUnsettledNeverExceedsMaxInFlight(t *testing.T) {
	runner := quickRunner(t)
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
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "runner saturation", func() bool { return dispatch.calls.Load() >= 3 })
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

func TestRunnerBackpressureFrozensDispatchCount(t *testing.T) {
	runner := quickRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	hold := make(chan struct{})
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-hold
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "runner saturation", func() bool { return dispatch.calls.Load() >= 3 })
	time.Sleep(100 * time.Millisecond)
	frozen := dispatch.calls.Load()
	time.Sleep(100 * time.Millisecond)
	if got := dispatch.calls.Load(); got != frozen {
		t.Fatalf("dispatch kept growing while the pipeline was full: %d -> %d", frozen, got)
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

func TestRunnerEmptyQueueKeepsPollingWithoutLosingCapacity(t *testing.T) {
	runner := quickRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
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
	if got := len(runner.permits); got != 3 {
		t.Fatalf("permits after shutdown = %d, want 3", got)
	}
}

func TestRunnerDispatchErrorReportsAndContinues(t *testing.T) {
	runner := quickRunner(t)
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
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, report) }()
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

func TestRunnerSettleErrorReportedAndBatchNotTreatedAsSuccess(t *testing.T) {
	runner := quickRunner(t)
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
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, report) }()
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

func TestRunnerCancelBeforeDispatchNeverDispatches(t *testing.T) {
	runner := quickRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	if err := runner.Run(ctx, dispatch, settle, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }, func(error) {}); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if dispatch.calls.Load() != 0 {
		t.Fatalf("dispatch called %d times on a cancelled context", dispatch.calls.Load())
	}
	if settle.calls.Load() != 0 {
		t.Fatalf("settle called %d times on a cancelled context", settle.calls.Load())
	}
}

func TestRunnerCancelNeverCreatesExtraSettles(t *testing.T) {
	runner := quickRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-ctx.Done()
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
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
	// The runner may settle a batch a handler already owns during the
	// graceful drain, but it must never settle more batches than it
	// dispatched.
	if settle.calls.Load() > dispatch.calls.Load() {
		t.Fatalf("settle calls = %d exceed dispatched batches = %d", settle.calls.Load(), dispatch.calls.Load())
	}
	pollUntil(t, 2*time.Second, "token restoration", func() bool { return len(runner.permits) == 3 && len(runner.slots) == 3 })
}

func TestRunnerDuplicateRunIsRejected(t *testing.T) {
	runner := quickRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "first run started", func() bool { return dispatch.calls.Load() >= 1 })
	if err := runner.Run(context.Background(), dispatch, settle, handle, func(error) {}); !errors.Is(err, errRunnerAlreadyActive) {
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
	go func() { restartDone <- runner.Run(restartCtx, dispatch, settle, handle, func(error) {}) }()
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

func TestRunnerRepeatedShutdownRestoresFullCapacityWithoutLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 6; i++ {
		runner := quickRunner(t)
		ctx, cancel := context.WithCancel(context.Background())
		dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
		settle := &settleRecorder{}
		runDone := make(chan error, 1)
		go func() {
			runDone <- runner.Run(ctx, dispatch, settle, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }, func(error) {})
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
		pollUntil(t, 2*time.Second, "permit restoration", func() bool { return len(runner.permits) == 3 && len(runner.slots) == 3 })
	}
	pollUntil(t, 3*time.Second, "goroutine drain", func() bool { return runtime.NumGoroutine() <= before+5 })
	if got := runtime.NumGoroutine(); got > before+5 {
		t.Fatalf("goroutines grew from %d to %d", before, got)
	}
}

func TestRunnerWorkerCountBoundsActiveHandlers(t *testing.T) {
	runner := quickRunner(t)
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
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
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

func TestRunnerMultipleDispatchersDispatchConcurrently(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   3,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   8,
		BatchSize:     1,
		BatchBuffer:   8,
		OutcomeBuffer: 8,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	hold := make(chan struct{})
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-hold
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "dispatcher concurrency", func() bool { return dispatch.peakConcurrent() >= 2 })
	close(hold)
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
}

func TestRunnerSettlersBoundConcurrency(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   2,
		Workers:       2,
		Settlers:      2,
		MaxInFlight:   4,
		BatchSize:     1,
		BatchBuffer:   4,
		OutcomeBuffer: 4,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "settler concurrency", func() bool { return settle.peakConcurrent() >= 2 })
	time.Sleep(50 * time.Millisecond)
	if got := settle.peakConcurrent(); got > 2 {
		t.Fatalf("peak concurrent settles = %d, exceeds settlers 2", got)
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

// switchableSettler fails transiently while fail is set, wrapping the same
// sentinel the real Scheduler.Settle wraps on Redis interaction errors.
type switchableSettler struct {
	calls atomic.Int64
	fail  atomic.Bool
}

func (s *switchableSettler) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	s.calls.Add(1)
	if s.fail.Load() {
		return echoqueue.Receipt{Status: echoqueue.ReceiptInvalid, BatchID: batchID}, fmt.Errorf("transient boom: %w", echoqueue.ErrTransientRedis)
	}
	return echoqueue.Receipt{Status: echoqueue.ReceiptApplied, BatchID: batchID}, nil
}

// blockingSettler blocks its first Settle call until the gate opens, then
// settles everything normally.
type blockingSettler struct {
	gate  <-chan struct{}
	once  *sync.Once
	calls *atomic.Int64
}

func (s *blockingSettler) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	s.calls.Add(1)
	s.once.Do(func() { <-s.gate })
	return echoqueue.Receipt{Status: echoqueue.ReceiptApplied, BatchID: batchID}, nil
}

// invalidSettler permanently rejects with a plain error, simulating a
// validation or business rejection that must never trip the breaker.
type invalidSettler struct {
	calls atomic.Int64
}

func (s *invalidSettler) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	s.calls.Add(1)
	return echoqueue.Receipt{Status: echoqueue.ReceiptInvalid, BatchID: batchID}, errors.New("permanent business rejection")
}

func TestRunnerBreakerOpensAndFreezesDispatchThenRecovers(t *testing.T) {
	metrics := &recordingMetrics{}
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   4,
		BatchSize:     1,
		BatchBuffer:   4,
		OutcomeBuffer: 4,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  25 * time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
		Metrics:       metrics,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &switchableSettler{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "healthy settles", func() bool { return settle.calls.Load() >= 3 })

	// Start failing: the third consecutive transient failure opens the breaker.
	settle.fail.Store(true)
	pollUntil(t, 2*time.Second, "breaker opens", func() bool { return metrics.breakerOpened.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)
	before := dispatch.calls.Load()
	// While open, Dispatch is limited to half-open probes (one per jittered
	// ErrorBackoff); a broken breaker would dispatch continuously.
	time.Sleep(100 * time.Millisecond)
	if got := dispatch.calls.Load(); got-before > 8 {
		t.Fatalf("dispatch was not paused while the breaker was open: %d -> %d", before, got)
	}

	// Recovery: the next probe's Settle succeeds and closes the breaker.
	settle.fail.Store(false)
	pollUntil(t, 2*time.Second, "breaker closes", func() bool { return metrics.breakerClosed.Load() >= 1 })
	pollUntil(t, 2*time.Second, "dispatch resumes", func() bool { return dispatch.calls.Load() > before+10 })
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

func TestRunnerBreakerStateResetAcrossRuns(t *testing.T) {
	metrics := &recordingMetrics{}
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       1,
		Settlers:      1,
		MaxInFlight:   3,
		BatchSize:     1,
		BatchBuffer:   3,
		OutcomeBuffer: 3,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  10 * time.Millisecond,
		ShutdownGrace: 100 * time.Millisecond,
		Metrics:       metrics,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &switchableSettler{}
	settle.fail.Store(true)
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "first run opens breaker", func() bool { return metrics.breakerOpened.Load() >= 1 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}

	// Redis is still failing: a restarted Run must start with a closed
	// breaker (its own three failures open it again) instead of inheriting
	// the stale open state and wedging forever.
	ctx2, cancel2 := context.WithCancel(context.Background())
	run2Done := make(chan error, 1)
	go func() { run2Done <- runner.Run(ctx2, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "second run re-opens breaker", func() bool { return metrics.breakerOpened.Load() >= 2 })
	cancel2()
	select {
	case err := <-run2Done:
		if err != nil {
			t.Fatalf("Run2 = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run2 did not stop")
	}
}

func TestRunnerReceiptInvalidNeverTripsBreaker(t *testing.T) {
	metrics := &recordingMetrics{}
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   4,
		BatchSize:     1,
		BatchBuffer:   4,
		OutcomeBuffer: 4,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
		Metrics:       metrics,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &invalidSettler{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "permanent rejections", func() bool { return settle.calls.Load() >= 5 })
	if got := metrics.breakerOpened.Load(); got != 0 {
		t.Fatalf("breaker opened %d times on permanent rejections", got)
	}
	if got := metrics.settleFailed.Load(); got != 0 {
		t.Fatalf("settleFailed counted %d times on permanent rejections", got)
	}
	before := dispatch.calls.Load()
	pollUntil(t, 2*time.Second, "dispatch continues", func() bool { return dispatch.calls.Load() >= before+3 })
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

func TestRunnerGraceExpiryReturnsOnlyBufferedTokens(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       1,
		Settlers:      1,
		MaxInFlight:   3,
		BatchSize:     1,
		BatchBuffer:   3,
		OutcomeBuffer: 3,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	gate := make(chan struct{})
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		<-gate
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	// One batch is inside the blocked handler; the other two wait in the
	// batch channel.
	pollUntil(t, 2*time.Second, "saturation", func() bool { return dispatch.calls.Load() >= 3 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	// The two buffered batches were abandoned and their tokens returned; the
	// batch inside the stuck handler still holds its pair.
	if got := len(runner.permits); got != 2 {
		t.Fatalf("permits after grace = %d, want 2", got)
	}
	if got := len(runner.slots); got != 2 {
		t.Fatalf("slots after grace = %d, want 2", got)
	}
	close(gate)
	pollUntil(t, 2*time.Second, "stuck handler released its tokens", func() bool { return len(runner.permits) == 3 && len(runner.slots) == 3 })
}

func TestRunnerCancelDrainsEveryBufferedBatch(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       1,
		Settlers:      1,
		MaxInFlight:   3,
		BatchSize:     1,
		BatchBuffer:   3,
		OutcomeBuffer: 3,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	gate := make(chan struct{})
	var once sync.Once
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		once.Do(func() { <-gate })
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "saturation", func() bool { return dispatch.calls.Load() >= 3 })
	// Wait until all three batches are handed off (one in the handler, two
	// buffered); cancelling mid-Dispatch would legitimately drop the last
	// batch to Recover instead of draining it.
	pollUntil(t, 2*time.Second, "batches buffered", func() bool { return len(runner.batches) == 2 })
	cancel()
	// The graceful drain settles every dispatched batch exactly once.
	close(gate)
	pollUntil(t, 2*time.Second, "all batches settled", func() bool { return settle.calls.Load() >= 3 })
	if got := settle.calls.Load(); got != 3 {
		t.Fatalf("settle calls = %d, want 3", got)
	}
	if got := dispatch.calls.Load(); got != 3 {
		t.Fatalf("dispatch calls = %d, want 3 (no new dispatch during drain)", got)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	pollUntil(t, 2*time.Second, "token restoration", func() bool { return len(runner.permits) == 3 && len(runner.slots) == 3 })
}

func TestRunnerOutcomeBufferFullBackpressure(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   4,
		BatchSize:     1,
		BatchBuffer:   4,
		OutcomeBuffer: 1,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	settleGate := make(chan struct{})
	var settleOnce sync.Once
	// A settle worker that blocks inside Settle leaves the single outcome
	// slot occupied, so handlers back up on the outcome send.
	blockingSettle := &blockingSettler{gate: settleGate, once: &settleOnce, calls: &settle.calls}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, blockingSettle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "handlers back up on outcome", func() bool {
		return settle.calls.Load() >= 1 && dispatch.calls.Load() >= 4
	})
	time.Sleep(50 * time.Millisecond)
	frozen := dispatch.calls.Load()
	time.Sleep(50 * time.Millisecond)
	if got := dispatch.calls.Load(); got != frozen {
		t.Fatalf("dispatch kept growing while outcomes were full: %d -> %d", frozen, got)
	}
	close(settleGate)
	pollUntil(t, 2*time.Second, "settles complete", func() bool { return settle.calls.Load() >= 4 })
	pollUntil(t, 2*time.Second, "dispatch resumes", func() bool { return dispatch.calls.Load() > frozen+3 })
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

func TestRunnerResizeWorkersGrow(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       1,
		Settlers:      1,
		MaxInFlight:   4,
		BatchSize:     1,
		BatchBuffer:   4,
		OutcomeBuffer: 4,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	gate := make(chan struct{})
	var activeHandlers atomic.Int64
	var peakHandlers atomic.Int64
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		value := activeHandlers.Add(1)
		for {
			current := peakHandlers.Load()
			if value <= current || peakHandlers.CompareAndSwap(current, value) {
				break
			}
		}
		<-gate
		activeHandlers.Add(-1)
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "one worker active", func() bool { return peakHandlers.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if got := peakHandlers.Load(); got != 1 {
		t.Fatalf("peak handlers before grow = %d, want 1", got)
	}
	if err := runner.ResizeWorkers(3); err != nil {
		t.Fatalf("ResizeWorkers: %v", err)
	}
	pollUntil(t, 2*time.Second, "grown handlers active", func() bool { return peakHandlers.Load() >= 3 })
	if _, workers, _ := currentStageSizes(runner); workers != 3 {
		t.Fatalf("worker pool size = %d, want 3", workers)
	}
	close(gate)
	pollUntil(t, 2*time.Second, "settle progress", func() bool { return settle.calls.Load() >= 4 })
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

func TestRunnerResizeWorkersShrink(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       3,
		Settlers:      1,
		MaxInFlight:   4,
		BatchSize:     1,
		BatchBuffer:   4,
		OutcomeBuffer: 4,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Empty dispatches first: the three workers idle on the batch channel.
	dispatch := &dispatchRecorder{}
	settle := &settleRecorder{}
	var activeHandlers atomic.Int64
	var peakHandlers atomic.Int64
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		value := activeHandlers.Add(1)
		for {
			current := peakHandlers.Load()
			if value <= current || peakHandlers.CompareAndSwap(current, value) {
				break
			}
		}
		activeHandlers.Add(-1)
		return okOutcome(batch)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "workers idle", func() bool { _, workers, _ := currentStageSizes(runner); return workers == 3 })
	if err := runner.ResizeWorkers(1); err != nil {
		t.Fatalf("ResizeWorkers: %v", err)
	}
	pollUntil(t, 2*time.Second, "pool shrunk", func() bool { _, workers, _ := currentStageSizes(runner); return workers == 1 })
	// Only one worker remains: dispatch real batches and confirm the peak
	// never exceeds one.
	dispatch.mu.Lock()
	dispatch.batch = echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}
	dispatch.mu.Unlock()
	pollUntil(t, 2*time.Second, "settle progress", func() bool { return settle.calls.Load() >= 3 })
	if got := peakHandlers.Load(); got > 1 {
		t.Fatalf("peak handlers after shrink = %d, want 1", got)
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

func TestRunnerResizeDispatchers(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   8,
		BatchSize:     1,
		BatchBuffer:   8,
		OutcomeBuffer: 8,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "dispatches flowing", func() bool { return dispatch.calls.Load() >= 5 })
	if got := dispatch.peakConcurrent(); got != 1 {
		t.Fatalf("peak concurrent dispatches before grow = %d, want 1", got)
	}
	if err := runner.ResizeDispatchers(4); err != nil {
		t.Fatalf("ResizeDispatchers: %v", err)
	}
	pollUntil(t, 2*time.Second, "dispatcher concurrency", func() bool { return dispatch.peakConcurrent() >= 2 })
	if dispatchers, _, _ := currentStageSizes(runner); dispatchers != 4 {
		t.Fatalf("dispatcher pool size = %d, want 4", dispatchers)
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

func TestRunnerResizeSettlers(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   8,
		BatchSize:     1,
		BatchBuffer:   8,
		OutcomeBuffer: 8,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "settles flowing", func() bool { return settle.calls.Load() >= 5 })
	if got := settle.peakConcurrent(); got != 1 {
		t.Fatalf("peak concurrent settles before grow = %d, want 1", got)
	}
	if err := runner.ResizeSettlers(3); err != nil {
		t.Fatalf("ResizeSettlers: %v", err)
	}
	pollUntil(t, 2*time.Second, "settler concurrency", func() bool { return settle.peakConcurrent() >= 2 })
	if _, _, settlers := currentStageSizes(runner); settlers != 3 {
		t.Fatalf("settler pool size = %d, want 3", settlers)
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

func TestRunnerResizePersistsAcrossRuns(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   8,
		BatchSize:     1,
		BatchBuffer:   8,
		OutcomeBuffer: 8,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }

	// Resize before Run: the first Run starts with four dispatchers.
	if err := runner.ResizeDispatchers(4); err != nil {
		t.Fatalf("ResizeDispatchers before Run: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Done := make(chan error, 1)
	go func() { run1Done <- runner.Run(ctx1, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "first run dispatcher concurrency", func() bool { return dispatch.peakConcurrent() >= 2 })
	cancel1()
	select {
	case err := <-run1Done:
		if err != nil {
			t.Fatalf("Run1 = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run1 did not stop")
	}

	// The remembered size survives the Run; shrink between Runs and the next
	// Run starts with one dispatcher.
	if err := runner.ResizeDispatchers(1); err != nil {
		t.Fatalf("ResizeDispatchers between Runs: %v", err)
	}
	dispatch.reset()
	ctx2, cancel2 := context.WithCancel(context.Background())
	run2Done := make(chan error, 1)
	go func() { run2Done <- runner.Run(ctx2, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "second run dispatches", func() bool { return dispatch.calls.Load() >= 10 })
	if got := dispatch.peakConcurrent(); got != 1 {
		t.Fatalf("peak concurrent dispatches after shrink = %d, want 1", got)
	}
	cancel2()
	select {
	case err := <-run2Done:
		if err != nil {
			t.Fatalf("Run2 = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run2 did not stop")
	}
}

func TestRunnerResizeInvalid(t *testing.T) {
	runner := quickRunner(t)
	for name, resize := range map[string]func(int) error{
		"workers":     runner.ResizeWorkers,
		"dispatchers": runner.ResizeDispatchers,
		"settlers":    runner.ResizeSettlers,
	} {
		for _, n := range []int{0, -1} {
			if err := resize(n); err == nil {
				t.Fatalf("%s(%d) accepted", name, n)
			}
		}
	}
}

func TestRunnerResizeDuringShutdown(t *testing.T) {
	runner := quickRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "settle progress", func() bool { return settle.calls.Load() >= 3 })
	cancel()
	// Resizing during the graceful drain must neither deadlock nor leak.
	if err := runner.ResizeWorkers(1); err != nil {
		t.Fatalf("ResizeWorkers during shutdown: %v", err)
	}
	if err := runner.ResizeSettlers(1); err != nil {
		t.Fatalf("ResizeSettlers during shutdown: %v", err)
	}
	if err := runner.ResizeDispatchers(1); err != nil {
		t.Fatalf("ResizeDispatchers during shutdown: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	pollUntil(t, 2*time.Second, "token restoration", func() bool { return len(runner.permits) == 3 && len(runner.slots) == 3 })
}

func TestRunnerResizeReturnsWhenHandlerBlockedOnOutcome(t *testing.T) {
	// The C-1 regression: a settling worker stuck in the host Settle leaves
	// the single outcome slot occupied, a handler blocks handing its outcome
	// over, and shrinking must still return in bounded time (the retiring
	// handler abandons the batch to Recover instead of blocking forever).
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   2,
		BatchSize:     1,
		BatchBuffer:   2,
		OutcomeBuffer: 1,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	settleGate := make(chan struct{})
	var settleOnce sync.Once
	blockingSettle := &blockingSettler{gate: settleGate, once: &settleOnce, calls: &settle.calls}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, blockingSettle, handle, func(error) {}) }()
	// One settle is stuck inside Settle; the second outcome is computed and
	// its handler blocks on the full outcome channel.
	pollUntil(t, 2*time.Second, "settler stuck and outcome blocked", func() bool {
		return settle.calls.Load() >= 1 && len(runner.outcomes) == 1
	})
	resizeDone := make(chan error, 1)
	go func() { resizeDone <- runner.ResizeWorkers(1) }()
	select {
	case err := <-resizeDone:
		if err != nil {
			t.Fatalf("ResizeWorkers: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ResizeWorkers blocked forever while a handler waited on a full outcome channel")
	}
	close(settleGate)
	pollUntil(t, 2*time.Second, "settles resume", func() bool { return settle.calls.Load() >= 2 })
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	pollUntil(t, 2*time.Second, "token restoration", func() bool { return len(runner.permits) == 2 && len(runner.slots) == 2 })
}

func TestRunnerConcurrentResizeRaces(t *testing.T) {
	runner, err := New(Config{
		Dispatchers:   1,
		Workers:       2,
		Settlers:      1,
		MaxInFlight:   6,
		BatchSize:     1,
		BatchBuffer:   6,
		OutcomeBuffer: 6,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
	settle := &settleRecorder{}
	handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
	pollUntil(t, 2*time.Second, "pipeline flowing", func() bool { return settle.calls.Load() >= 3 })

	var wg sync.WaitGroup
	resizeErr := make(chan error, 16)
	flip := func(resize func(int) error, low, high int, rounds int) {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if err := resize(low); err != nil {
				resizeErr <- err
				return
			}
			if err := resize(high); err != nil {
				resizeErr <- err
				return
			}
		}
	}
	wg.Add(3)
	go flip(runner.ResizeWorkers, 1, 3, 40)
	go flip(runner.ResizeSettlers, 1, 2, 40)
	go flip(runner.ResizeDispatchers, 1, 2, 40)
	wg.Wait()
	close(resizeErr)
	for err := range resizeErr {
		t.Fatalf("concurrent resize: %v", err)
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
	pollUntil(t, 2*time.Second, "token restoration", func() bool { return len(runner.permits) == 6 && len(runner.slots) == 6 })
}

func TestRunnerCancelResizeRaceRestoresAllTokens(t *testing.T) {
	// The M-1 regression: cancel racing a Resize that spawns after the
	// coordinator's snapshot must still end every Run with full tokens, on
	// both the graceful and the grace-expiry path.
	for i := 0; i < 10; i++ {
		runner := quickRunner(t)
		ctx, cancel := context.WithCancel(context.Background())
		dispatch := &dispatchRecorder{batch: echoqueue.Batch{ID: "b", Tasks: []echoqueue.Task{{TaskID: "t", Payload: []byte(`{}`)}}}}
		settle := &settleRecorder{}
		handle := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome { return okOutcome(batch) }
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(ctx, dispatch, settle, handle, func(error) {}) }()
		pollUntil(t, 2*time.Second, "pipeline flowing", func() bool { return settle.calls.Load() >= 2 })
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = runner.ResizeDispatchers(2)
		}()
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()
		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("Run = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not stop")
		}
		if got := len(runner.permits); got != 3 {
			t.Fatalf("iteration %d: permits after shutdown = %d, want 3", i, got)
		}
		if got := len(runner.slots); got != 3 {
			t.Fatalf("iteration %d: slots after shutdown = %d, want 3", i, got)
		}
	}
}

func TestRunnerResizeAndConfigUpperBound(t *testing.T) {
	if _, err := New(Config{Workers: maxStageWorkers + 1}); err == nil {
		t.Fatal("config above the stage upper bound was accepted")
	}
	if _, err := New(Config{Dispatchers: maxStageWorkers + 1}); err == nil {
		t.Fatal("config above the stage upper bound was accepted")
	}
	if _, err := New(Config{Settlers: maxStageWorkers + 1}); err == nil {
		t.Fatal("config above the stage upper bound was accepted")
	}
	runner := quickRunner(t)
	if err := runner.ResizeWorkers(maxStageWorkers + 1); err == nil {
		t.Fatal("resize above the stage upper bound was accepted")
	}
	if err := runner.ResizeDispatchers(maxStageWorkers + 1); err == nil {
		t.Fatal("resize above the stage upper bound was accepted")
	}
	if err := runner.ResizeSettlers(maxStageWorkers + 1); err == nil {
		t.Fatal("resize above the stage upper bound was accepted")
	}
	// The boundary value itself is legal; without an active Run it only
	// updates the remembered size, spawning nothing.
	if err := runner.ResizeWorkers(maxStageWorkers); err != nil {
		t.Fatalf("resize at the upper bound: %v", err)
	}
}

func TestRunnerConfigValidation(t *testing.T) {
	if _, err := New(Config{}); err != nil {
		t.Fatalf("zero config: %v", err)
	}
	for name, cfg := range map[string]Config{
		"negative dispatchers":  {Dispatchers: -1},
		"negative workers":      {Workers: -1},
		"negative settlers":     {Workers: 1, Settlers: -1},
		"negative in flight":    {Workers: 1, MaxInFlight: -1},
		"negative batch buffer": {Workers: 1, MaxInFlight: 1, BatchBuffer: -1},
		"negative outcome":      {Workers: 1, MaxInFlight: 1, BatchBuffer: 1, OutcomeBuffer: -1},
		"negative batch size":   {Workers: 1, MaxInFlight: 1, BatchBuffer: 1, OutcomeBuffer: 1, BatchSize: -1},
		"negative poll":         {Workers: 1, MaxInFlight: 1, BatchBuffer: 1, OutcomeBuffer: 1, BatchSize: 1, PollInterval: -time.Second},
		"negative backoff":      {Workers: 1, MaxInFlight: 1, BatchBuffer: 1, OutcomeBuffer: 1, BatchSize: 1, PollInterval: time.Millisecond, ErrorBackoff: -time.Second},
		"negative grace":        {Workers: 1, MaxInFlight: 1, BatchBuffer: 1, OutcomeBuffer: 1, BatchSize: 1, PollInterval: time.Millisecond, ErrorBackoff: time.Millisecond, ShutdownGrace: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	runner, err := New(Config{Workers: 1, MaxInFlight: 1, BatchBuffer: 1, OutcomeBuffer: 1, BatchSize: 1, PollInterval: time.Millisecond, ErrorBackoff: time.Millisecond, ShutdownGrace: time.Millisecond})
	if err != nil || len(runner.permits) != 1 || len(runner.slots) != 1 {
		t.Fatalf("explicit one-token runner = %v, err=%v", runner, err)
	}
}
