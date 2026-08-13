package consumer

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// dispatchLoop runs one Dispatch producer. Multiple dispatchers share the
// same permit and slot pools, so total dispatched-not-settled batches never
// exceed MaxInFlight no matter how many dispatchers run.
//
// While the settle circuit breaker is open, the dispatcher pauses for a
// jittered ErrorBackoff and then performs one half-open probe Dispatch: the
// probe's outcome flows to the settlers, whose success or failure decides
// whether the breaker closes. A Redis outage therefore never turns into a
// dispatch storm, and recovery never needs an external nudge.
func (r *Runner) dispatchLoop(ctx context.Context, dispatch BatchDispatcher, report ErrorSink) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if r.breakerOpenState() {
			r.wait(ctx, jittered(r.cfg.ErrorBackoff))
		}
		r.dispatchOnce(ctx, dispatch, report)
	}
}

// dispatchOnce performs one permit-guarded Dispatch cycle. The permit and
// batch slot are acquired before Dispatch so a dispatched batch always has a
// bounded slot waiting for it; both are released on every early return. The
// deferred release also covers a panicking host Dispatch or report callback,
// so a panic can never permanently shrink the token pools.
func (r *Runner) dispatchOnce(ctx context.Context, dispatch BatchDispatcher, report ErrorSink) {
	select {
	case <-ctx.Done():
		return
	case <-r.permits:
	}
	select {
	case <-ctx.Done():
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
		report(fmt.Errorf("echoqueue consumer: dispatch: %w", err))
		if ctx.Err() != nil {
			return
		}
		r.wait(ctx, jittered(r.cfg.ErrorBackoff))
		return
	}
	if batch.ID == "" {
		// Empty source queue: release the permit and poll again.
		r.cfg.Metrics.DispatchEmpty()
		r.wait(ctx, r.cfg.PollInterval)
		return
	}
	r.cfg.Metrics.BatchDispatched(len(batch.Tasks))
	select {
	case <-ctx.Done():
		// The dispatched batch stays under Pending/Recover control; never
		// fabricate an in-memory ACK.
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
