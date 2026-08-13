package consumer

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// dispatchLoop runs one Dispatch producer. Multiple dispatchers share the
// same permit and slot pools, so total dispatched-not-settled batches never
// exceed MaxInFlight no matter how many dispatchers run. Retiring a worker
// closes quit; the worker exits at the next loop boundary or blocking point
// without taking a new permit.
//
// While the settle circuit breaker is open, the dispatcher pauses for a
// jittered ErrorBackoff and then performs one half-open probe Dispatch: the
// probe's outcome flows to the settlers, whose success or failure decides
// whether the breaker closes. A Redis outage therefore never turns into a
// dispatch storm, and recovery never needs an external nudge.
func (r *Runner) dispatchLoop(ctx context.Context, dispatch BatchDispatcher, report ErrorSink, quit <-chan struct{}) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		select {
		case <-quit:
			return
		default:
		}
		if r.breakerOpenState() {
			// Half-open probe: after one jittered backoff, fall through and
			// allow a single Dispatch; the probe's Settle decides whether the
			// breaker closes.
			r.wait(ctx, jittered(r.cfg.ErrorBackoff), quit)
		}
		r.dispatchCycle(ctx, dispatch, report, quit)
	}
}

// dispatchCycle performs one permit-guarded Dispatch cycle. The permit and
// batch slot are acquired before Dispatch so a dispatched batch always has a
// bounded slot waiting for it; both are released on every early return. The
// deferred release also covers a panicking host Dispatch or report callback,
// so a panic can never permanently shrink the token pools.
func (r *Runner) dispatchCycle(ctx context.Context, dispatch BatchDispatcher, report ErrorSink, quit <-chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-quit:
		return
	case <-r.permits:
	}
	select {
	case <-ctx.Done():
		r.permits <- struct{}{}
		return
	case <-quit:
		r.permits <- struct{}{}
		return
	case <-r.slots:
	}
	released := false
	defer func() {
		if !released {
			r.release()
		}
	}()
	r.cfg.Metrics.DispatchStarted()
	batch, err := dispatch.Dispatch(ctx, r.cfg.BatchSize)
	if err != nil {
		r.cfg.Metrics.DispatchFailed()
		if ctx.Err() != nil {
			// Cancellation noise during shutdown; not a real failure.
			return
		}
		report(fmt.Errorf("echoqueue consumer: dispatch: %w", err))
		r.wait(ctx, jittered(r.cfg.ErrorBackoff), quit)
		return
	}
	if batch.ID == "" {
		// Empty source queue: release the permit and poll again.
		r.cfg.Metrics.DispatchEmpty()
		r.wait(ctx, r.cfg.PollInterval, quit)
		return
	}
	r.cfg.Metrics.BatchDispatched(len(batch.Tasks))
	select {
	case <-ctx.Done():
		// The dispatched batch stays under Pending/Recover control; never
		// fabricate an in-memory ACK.
		return
	case <-quit:
		// A retiring dispatcher hands the batch to Recover instead of
		// fabricating an in-memory ACK.
		return
	case r.batches <- batch:
		// Handed off; the handler or settler releases the permit and slot.
		released = true
	}
}

// jittered returns d with up to 20% random deviation, so multiple
// dispatchers and multiple processes do not retry in lockstep. For delays
// under 5ms the deviation covers the whole delay.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := d / 5
	if spread <= 0 {
		spread = d
	}
	return d - spread + rand.N(2*spread)
}
