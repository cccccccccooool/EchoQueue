// Package consumer hosts the staged consumption pipeline for EchoQueue:
// bounded Dispatcher -> batch channel -> Handler -> outcome channel ->
// Settler. All stages share a single MaxInFlight quota that is acquired
// before Dispatch, so a full pipeline stops calling Dispatch and leaves
// tasks in the Source list.
//
// The required per-batch order is:
//
//	Acquire global permit (and batch slot)
//	  -> Dispatch
//	  -> Handle
//	  -> Settle
//	  -> Release permit (and slot)
//
// A permit is always acquired before Dispatch so a dispatched batch always
// has a bounded buffer slot waiting for it; a full pipeline stops calling
// Dispatch and leaves tasks in the Source list.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
)

// errRunnerAlreadyActive mirrors Scheduler's ErrRunAlreadyActive: a runner
// may only have one active Run call.
var errRunnerAlreadyActive = errors.New("echoqueue consumer: runner is already running")

// BatchDispatcher is the host-local dispatch boundary. echoqueue.*Queue
// satisfies it.
type BatchDispatcher interface {
	Dispatch(ctx context.Context, batchSize int) (echoqueue.Batch, error)
}

// BatchSettler is the host-local settle boundary. echoqueue.*Scheduler
// satisfies it.
type BatchSettler interface {
	Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error)
}

// BatchHandler runs business logic for one dispatched batch. Business
// failures must be expressed as Outcome.Failures so Settle can atomically
// write Retry or Dead effects; the pipeline never fabricates outcomes.
type BatchHandler func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome

// ErrorSink receives host-visible errors (Dispatch errors, Settle errors).
// It MUST be safe for concurrent use: dispatchers and settlers call it from
// multiple goroutines. The reference logs them; a production host would
// alert.
type ErrorSink func(err error)

// outcomeItem carries a computed outcome from a handler to a settler. The
// generation is checked by the settler so a handler abandoned by an earlier
// Run can never settle a batch after a newer Run has started.
type outcomeItem struct {
	batch   echoqueue.Batch
	outcome echoqueue.Outcome
	gen     int64
}

// breakerThreshold is the number of consecutive transient Settle failures
// after which Dispatch is paused until a Settle succeeds again.
const breakerThreshold = 3

// Runner keeps Dispatcher, Handler, Settler, and in-flight counts under hard
// caps.
//
// permits, slots, batches, and outcomes are created by New and reused across
// Runs. permits holds MaxInFlight tokens, slots holds BatchBuffer tokens,
// batches has BatchBuffer capacity, outcomes has OutcomeBuffer capacity.
// Acquiring a token is a receive (blocking when the stage is exhausted);
// releasing is a send. Channels are never closed except by the unique
// coordinator after every producer goroutine of that stage has exited, and
// every acquire is paired with exactly one release on the normal path, so
// token counts return to full after a clean shutdown.
type Runner struct {
	cfg     Config
	startMu sync.Mutex
	active  bool
	// runGen increments with every Run start. Settlers check the generation
	// before Settle: an outcome produced by an earlier Run that ignored
	// context cancellation must never settle after a newer Run has started,
	// so two generations can never close the same batch.
	runGen atomic.Int64

	// desiredDispatchers/desiredWorkers/desiredSettlers are the stage sizes
	// used by the next Run start and updated by Resize*. They persist across
	// Runs, so resizing between Runs changes the next Run's concurrency.
	desiredDispatchers atomic.Int64
	desiredWorkers     atomic.Int64
	desiredSettlers    atomic.Int64

	// poolMu guards the per-Run stage pools; Resize* manipulates them while
	// Run is active and clears them when Run returns.
	poolMu      sync.Mutex
	curDispatch *stagePool
	curWorkers  *stagePool
	curSettlers *stagePool

	// permits holds MaxInFlight tokens. A token is acquired before Dispatch
	// and released after Settle (or after any early return).
	permits chan struct{}
	// slots holds BatchBuffer reservation tokens. A token is acquired before
	// Dispatch and released after Settle, which guarantees the subsequent
	// hand-off into batches never blocks and dispatched batches never wait
	// in unbounded memory.
	slots chan struct{}
	// batches is the bounded hand-off from the dispatchers to the handlers.
	batches chan echoqueue.Batch
	// outcomes is the bounded hand-off from the handlers to the settlers.
	outcomes chan outcomeItem

	// breakerMu serializes breaker state transitions so "consecutive
	// failures" stays meaningful under concurrent settlers.
	breakerMu       sync.Mutex
	breakerFailures int
	breakerOpen     bool
}

// New validates the host-local configuration and constructs the runner with
// full token counts. It does not contact Redis.
func New(cfg Config) (*Runner, error) {
	validated, err := cfg.validated()
	if err != nil {
		return nil, err
	}
	permits := make(chan struct{}, validated.MaxInFlight)
	for i := 0; i < validated.MaxInFlight; i++ {
		permits <- struct{}{}
	}
	slots := make(chan struct{}, validated.BatchBuffer)
	for i := 0; i < validated.BatchBuffer; i++ {
		slots <- struct{}{}
	}
	runner := &Runner{
		cfg:      validated,
		permits:  permits,
		slots:    slots,
		batches:  make(chan echoqueue.Batch, validated.BatchBuffer),
		outcomes: make(chan outcomeItem, validated.OutcomeBuffer),
	}
	runner.desiredDispatchers.Store(int64(validated.Dispatchers))
	runner.desiredWorkers.Store(int64(validated.Workers))
	runner.desiredSettlers.Store(int64(validated.Settlers))
	return runner, nil
}

// ResizeDispatchers changes the number of parallel Dispatch goroutines. The
// new size applies immediately to the active Run and is remembered for the
// next Run. Shrinking retires workers only after they finish their current
// unit of work, so no dispatched batch is ever abandoned by a resize.
func (r *Runner) ResizeDispatchers(n int) error {
	if n < 1 || n > maxStageWorkers {
		return fmt.Errorf("echoqueue consumer: dispatchers must be between 1 and %d", maxStageWorkers)
	}
	r.desiredDispatchers.Store(int64(n))
	r.poolMu.Lock()
	pool := r.curDispatch
	r.poolMu.Unlock()
	if pool != nil {
		pool.resize(n)
	}
	return nil
}

// ResizeWorkers changes the number of handler goroutines. See
// ResizeDispatchers for the lifetime semantics.
func (r *Runner) ResizeWorkers(n int) error {
	if n < 1 || n > maxStageWorkers {
		return fmt.Errorf("echoqueue consumer: workers must be between 1 and %d", maxStageWorkers)
	}
	r.desiredWorkers.Store(int64(n))
	r.poolMu.Lock()
	pool := r.curWorkers
	r.poolMu.Unlock()
	if pool != nil {
		pool.resize(n)
	}
	return nil
}

// ResizeSettlers changes the number of settle goroutines. See
// ResizeDispatchers for the lifetime semantics.
func (r *Runner) ResizeSettlers(n int) error {
	if n < 1 || n > maxStageWorkers {
		return fmt.Errorf("echoqueue consumer: settlers must be between 1 and %d", maxStageWorkers)
	}
	r.desiredSettlers.Store(int64(n))
	r.poolMu.Lock()
	pool := r.curSettlers
	r.poolMu.Unlock()
	if pool != nil {
		pool.resize(n)
	}
	return nil
}

// Run starts the dispatchers, handlers, and settlers, then blocks until ctx
// is cancelled. While ctx is active it never returns. Shutdown is staged:
//
//  1. Dispatchers stop producing as soon as ctx is cancelled.
//  2. Once every dispatcher has exited, the coordinator signals the
//     handlers, who drain every already-dispatched batch.
//  3. Once every handler has exited, the coordinator signals the settlers,
//     who drain every computed outcome.
//  4. If the pipeline has not drained by ShutdownGrace, Run cancels the
//     run context, returns the permits of whatever is still buffered, and
//     leaves the remainder to EchoQueue's Pending/Recover loop.
//
// The shared batches and outcomes channels are never closed, so a restarted
// Run reuses them safely and an abandoned handler that ignores context
// cancellation can never send on a closed channel. Run always returns by
// ShutdownGrace: if a handler or settle ignores context cancellation, its
// goroutine is left running and this Run returns anyway. Run returns
// errRunnerAlreadyActive if called twice concurrently. Dispatch and Settle
// errors are delivered to the ErrorSink; Run itself reports nothing for
// those so the host can restart Run without losing observability.
func (r *Runner) Run(ctx context.Context, dispatch BatchDispatcher, settle BatchSettler, handle BatchHandler, report ErrorSink) error {
	if ctx == nil {
		return errors.New("echoqueue consumer: context is nil")
	}
	if dispatch == nil || settle == nil || handle == nil || report == nil {
		return errors.New("echoqueue consumer: dispatch, settle, handle, and report are required")
	}
	r.startMu.Lock()
	if r.active {
		r.startMu.Unlock()
		return errRunnerAlreadyActive
	}
	r.active = true
	r.startMu.Unlock()
	defer func() {
		r.startMu.Lock()
		r.active = false
		r.startMu.Unlock()
	}()

	// A restarted Run starts with a closed breaker: the first probes decide
	// whether Redis has recovered, instead of inheriting a stale open state
	// that would wedge the pipeline forever.
	r.breakerMu.Lock()
	r.breakerFailures = 0
	r.breakerOpen = false
	r.breakerMu.Unlock()

	// runCtx is deliberately not derived from ctx: the graceful drain needs
	// handlers and settlers to keep working after ctx is cancelled. It is
	// cancelled only when ShutdownGrace expires.
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	gen := r.runGen.Add(1)

	// batchesClosed and outcomesClosed are per-Run signal channels. The
	// coordinator closes them after the corresponding producer stage has
	// exited; handlers and settlers then drain the shared batches/outcomes
	// channels. The shared channels themselves are never closed, so a
	// restarted Run can reuse them safely.
	batchesClosed := make(chan struct{})
	outcomesClosed := make(chan struct{})

	// The stage pools own the goroutines of this Run; Resize* manipulates
	// them through r.cur* while the Run is active.
	dispatchPool := newStagePool(func(quit <-chan struct{}) {
		r.dispatchLoop(ctx, dispatch, report, quit)
	})
	workerPool := newStagePool(func(quit <-chan struct{}) {
		r.workerLoop(runCtx, gen, handle, batchesClosed, quit)
	})
	settlerPool := newStagePool(func(quit <-chan struct{}) {
		r.settlerLoop(runCtx, settle, report, outcomesClosed, quit)
	})
	dispatchPool.resize(int(r.desiredDispatchers.Load()))
	workerPool.resize(int(r.desiredWorkers.Load()))
	settlerPool.resize(int(r.desiredSettlers.Load()))
	r.poolMu.Lock()
	r.curDispatch = dispatchPool
	r.curWorkers = workerPool
	r.curSettlers = settlerPool
	r.poolMu.Unlock()
	defer func() {
		r.poolMu.Lock()
		r.curDispatch = nil
		r.curWorkers = nil
		r.curSettlers = nil
		r.poolMu.Unlock()
	}()

	// The single coordinator signals each stage only after every producer
	// goroutine of that stage has exited, and only while the drain is still
	// graceful. After grace expiry runCtx is cancelled and no signal fires.
	coordinated := make(chan struct{})
	go func() {
		dispatchPool.wait()
		if runCtx.Err() != nil {
			return
		}
		close(batchesClosed)
		workerPool.wait()
		if runCtx.Err() != nil {
			return
		}
		close(outcomesClosed)
		settlerPool.wait()
		if runCtx.Err() != nil {
			return
		}
		close(coordinated)
	}()

	<-ctx.Done()

	grace := time.NewTimer(r.cfg.ShutdownGrace)
	defer grace.Stop()
	select {
	case <-coordinated:
		// The graceful drain completed. Scavenge once as well: a worker
		// spawned by a Resize that raced the coordinator's snapshot may
		// still have delivered one batch after the last handler drained.
		r.scavenge()
		return nil
	case <-grace.C:
		cancelRun()
	}

	// Return the permits and slots of any batches abandoned to Recover so a
	// restarted Run starts with full capacity.
	r.scavenge()
	return nil
}

// scavenge returns the permits and slots of any batches still buffered in
// the shared channels. The sends below can never block: every buffered
// batch holds exactly one token of each channel, and the buffered batch
// count never exceeds the token capacities. Batches held by handlers or
// settlers that ignore cancellation are not counted and their tokens stay
// with the stuck goroutine.
func (r *Runner) scavenge() {
	for {
		select {
		case <-r.batches:
			r.slots <- struct{}{}
			r.permits <- struct{}{}
		case <-r.outcomes:
			r.slots <- struct{}{}
			r.permits <- struct{}{}
		default:
			return
		}
	}
}

func (r *Runner) wait(ctx context.Context, d time.Duration, quit <-chan struct{}) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-quit:
	case <-timer.C:
	}
}

func (r *Runner) breakerOpenState() bool {
	r.breakerMu.Lock()
	defer r.breakerMu.Unlock()
	return r.breakerOpen
}

// release returns one permit and one batch slot. Every acquire in the
// dispatcher is paired with exactly one release here, on the settler's
// normal path, or on a handler's abandon path.
func (r *Runner) release() {
	r.slots <- struct{}{}
	r.permits <- struct{}{}
}
