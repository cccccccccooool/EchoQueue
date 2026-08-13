// Package main is a host reference implementation for bounded, reliable
// consumption. It owns concurrency limits only: EchoQueue's Pending snapshot,
// Receipt fence, and Recover loop remain the durable facts of record, and this
// pool never acts as a durable retry queue.
//
// The required per-batch order is:
//
//	Acquire worker permit
//	  -> Dispatch
//	  -> Handle
//	  -> Settle
//	  -> Release permit
//
// A permit is always acquired before Dispatch so a dispatched batch always has
// a bounded buffer slot waiting for it; a full pool stops calling Dispatch and
// leaves tasks in the Source list.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
)

// errPoolAlreadyActive mirrors Scheduler's ErrRunAlreadyActive: a pool may
// only have one active Run call.
var errPoolAlreadyActive = errors.New("echoqueue example: pool is already running")

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

// BatchHandler runs business logic for one dispatched batch. Business failures
// must be expressed as Outcome.Failures so Settle can atomically write Retry
// or Dead effects; the pool itself never fabricates outcomes.
type BatchHandler func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome

// ErrorSink receives host-visible errors (Dispatch errors, Settle errors,
// archiver errors). The reference logs them; a production host would alert.
type ErrorSink func(err error)

// PoolConfig is host-local configuration. None of these fields enter the
// EchoQueue Config type.
type PoolConfig struct {
	// Workers is the number of handler goroutines; at most this many Handlers
	// are active at once.
	Workers int
	// MaxInFlight bounds how many batches may be dispatched but not yet
	// settled. Each in-flight batch holds one permit.
	MaxInFlight int
	// Buffer bounds how many dispatched batches may wait in memory for a
	// worker. Dispatch never runs without a reserved buffer slot.
	Buffer int
	// BatchSize is passed to Dispatch for every call.
	BatchSize int
	// PollInterval is the pause after an empty Dispatch or a Dispatch error.
	PollInterval time.Duration
	// ShutdownGrace bounds how long Run may wait for workers after context
	// cancellation. Run always returns by this deadline; abandoned batches
	// are left to EchoQueue's Pending/Recover loop.
	ShutdownGrace time.Duration
}

func (c PoolConfig) validated() (PoolConfig, error) {
	defaults := defaultPoolConfig()
	if c.Workers == 0 {
		c.Workers = defaults.Workers
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = defaults.MaxInFlight
	}
	if c.Buffer == 0 {
		c.Buffer = defaults.Buffer
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaults.BatchSize
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaults.PollInterval
	}
	if c.ShutdownGrace == 0 {
		c.ShutdownGrace = defaults.ShutdownGrace
	}
	if c.Workers <= 0 || c.MaxInFlight <= 0 || c.Buffer <= 0 || c.BatchSize <= 0 {
		return PoolConfig{}, errors.New("echoqueue example: workers, max_in_flight, buffer, and batch_size must be positive")
	}
	if c.PollInterval <= 0 || c.ShutdownGrace <= 0 {
		return PoolConfig{}, errors.New("echoqueue example: poll_interval and shutdown_grace must be positive")
	}
	return c, nil
}

func defaultPoolConfig() PoolConfig {
	return PoolConfig{
		Workers:       4,
		MaxInFlight:   8,
		Buffer:        16,
		BatchSize:     1,
		PollInterval:  time.Second,
		ShutdownGrace: 5 * time.Second,
	}
}

// BoundedPool keeps Worker, Buffer, and in-flight counts under hard caps.
//
// permits and slots are token channels created by NewPool and pre-filled with
// MaxInFlight and Buffer tokens respectively. Acquiring a permit or a slot is
// a receive (blocking when the pool is exhausted); releasing is a send. The
// channels are never closed, and every acquire is paired with exactly one
// release on the normal path, so len(permits) and len(slots) return to their
// full values after a clean shutdown.
type BoundedPool struct {
	cfg     PoolConfig
	startMu sync.Mutex
	active  bool
	// runGen increments with every Run start. Workers check the generation
	// before Settle: a worker abandoned by an earlier Run that ignored
	// context cancellation must never settle a batch after a newer Run has
	// started, so two generations can never close the same batch.
	runGen atomic.Int64

	// permits holds MaxInFlight tokens. A token is acquired before Dispatch
	// and released after Settle (or after any early return).
	permits chan struct{}
	// slots holds Buffer reservation tokens. A token is acquired before
	// Dispatch and released after Settle, which guarantees the subsequent
	// hand-off into batches never blocks and dispatched batches never wait in
	// unbounded memory.
	slots chan struct{}
	// batches is the bounded hand-off from the dispatcher to the workers.
	batches chan echoqueue.Batch
}

// NewPool validates the host-local configuration and constructs the bounded
// pool with full token counts. It does not contact Redis.
func NewPool(cfg PoolConfig) (*BoundedPool, error) {
	validated, err := cfg.validated()
	if err != nil {
		return nil, err
	}
	permits := make(chan struct{}, validated.MaxInFlight)
	for i := 0; i < validated.MaxInFlight; i++ {
		permits <- struct{}{}
	}
	slots := make(chan struct{}, validated.Buffer)
	for i := 0; i < validated.Buffer; i++ {
		slots <- struct{}{}
	}
	return &BoundedPool{
		cfg:     validated,
		permits: permits,
		slots:   slots,
		batches: make(chan echoqueue.Batch, validated.Buffer),
	}, nil
}

// Run starts the dispatcher and workers, then blocks until ctx is cancelled.
// While ctx is active it never returns. After cancellation it stops producing
// new dispatches, lets workers finish whatever batch they are already inside,
// and waits up to ShutdownGrace for them to exit; batches still sitting in the
// buffer at that point are abandoned to EchoQueue's Pending/Recover loop.
// Run always returns by ShutdownGrace: if a handler or settle ignores context
// cancellation, its goroutine is left running and this Run returns anyway, so
// Run never deadlocks. Run returns errPoolAlreadyActive if called twice
// concurrently. Dispatch, Settle, and archiver errors are delivered to the
// ErrorSink; Run itself reports nothing for those so the host can restart Run
// without losing observability.
func (p *BoundedPool) Run(ctx context.Context, dispatch BatchDispatcher, settle BatchSettler, handle BatchHandler, report ErrorSink) error {
	if ctx == nil {
		return errors.New("echoqueue example: context is nil")
	}
	if dispatch == nil || settle == nil || handle == nil || report == nil {
		return errors.New("echoqueue example: dispatch, settle, handle, and report are required")
	}
	p.startMu.Lock()
	if p.active {
		p.startMu.Unlock()
		return errPoolAlreadyActive
	}
	p.active = true
	p.startMu.Unlock()
	defer func() {
		p.startMu.Lock()
		p.active = false
		p.startMu.Unlock()
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	gen := p.runGen.Add(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.dispatchLoop(runCtx, dispatch, report)
	}()
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.workerLoop(runCtx, gen, settle, handle, report)
		}()
	}

	<-runCtx.Done()

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	grace := time.NewTimer(p.cfg.ShutdownGrace)
	defer grace.Stop()
	select {
	case <-drained:
	case <-grace.C:
		// A handler or settle that ignores context cancellation is left
		// running; Run never waits past ShutdownGrace for it.
	}

	// Return the permits and slots of any batches abandoned to Recover so a
	// restarted Run starts with full capacity. The sends below can never
	// block: every abandoned batch holds exactly one token of each channel,
	// and the abandoned batch count never exceeds the channel capacities.
	for {
		select {
		case <-p.batches:
			p.slots <- struct{}{}
			p.permits <- struct{}{}
		default:
			return nil
		}
	}
}

func (p *BoundedPool) dispatchLoop(ctx context.Context, dispatch BatchDispatcher, report ErrorSink) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.permits:
			// Permit acquired before Dispatch. Reserve a buffer slot as well
			// so the dispatched batch can always be handed to a worker
			// immediately and never waits in unbounded memory.
			select {
			case <-ctx.Done():
				p.permits <- struct{}{}
				return
			case <-p.slots:
			}
			batch, err := dispatch.Dispatch(ctx, p.cfg.BatchSize)
			if err != nil {
				report(fmt.Errorf("echoqueue example: dispatch: %w", err))
				p.slots <- struct{}{}
				p.permits <- struct{}{}
				if ctx.Err() != nil {
					return
				}
				p.wait(ctx)
				continue
			}
			if batch.ID == "" {
				// Empty source queue: release the permit and poll again.
				p.slots <- struct{}{}
				p.permits <- struct{}{}
				p.wait(ctx)
				continue
			}
			select {
			case <-ctx.Done():
				// The dispatched batch stays under Pending/Recover control;
				// never fabricate an in-memory ACK.
				p.slots <- struct{}{}
				p.permits <- struct{}{}
				return
			case p.batches <- batch:
			}
		}
	}
}

func (p *BoundedPool) workerLoop(ctx context.Context, gen int64, settle BatchSettler, handle BatchHandler, report ErrorSink) {
	for {
		if err := ctx.Err(); err != nil {
			// Batches still in the buffer are abandoned to Pending/Recover:
			// the pool does not settle them after cancellation.
			return
		}
		select {
		case <-ctx.Done():
			return
		case batch := <-p.batches:
			p.workBatch(ctx, gen, batch, settle, handle, report)
		}
	}
}

func (p *BoundedPool) workBatch(ctx context.Context, gen int64, batch echoqueue.Batch, settle BatchSettler, handle BatchHandler, report ErrorSink) {
	defer func() {
		p.slots <- struct{}{}
		p.permits <- struct{}{}
	}()
	outcome := handle(ctx, batch)
	if ctx.Err() != nil {
		// Handling was interrupted by cancellation: never fabricate a
		// terminal state for a partially handled batch. It stays under
		// Pending/deadline control and Recover closes it.
		return
	}
	if p.runGen.Load() != gen {
		// A newer Run has started while this worker was still handling the
		// batch: an old generation must never settle, so the batch is left
		// to Recover instead of risking two generations closing it.
		return
	}
	receipt, err := settle.Settle(ctx, batch.ID, outcome)
	if err != nil {
		// Settle failed: the batch must not be treated as a success, and no
		// in-memory retry queue is created. Pending/deadline remain in Redis
		// and Recover will eventually close the batch.
		report(fmt.Errorf("echoqueue example: settle batch %q: %w", batch.ID, err))
		return
	}
	if receipt.Status == echoqueue.ReceiptInvalid {
		report(fmt.Errorf("echoqueue example: settle batch %q returned an invalid receipt", batch.ID))
	}
}

func (p *BoundedPool) wait(ctx context.Context) {
	timer := time.NewTimer(p.cfg.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
