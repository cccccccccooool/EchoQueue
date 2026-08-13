package consumer

import (
	"context"
	"errors"
	"fmt"

	echoqueue "github.com/cccccccccooool/EchoQueue"
)

// settlerLoop runs one settle worker. Settlers own every Redis write of the
// pipeline, so Redis settle latency never occupies handler workers. On a
// graceful shutdown the coordinator closes the outcomesClosed signal only
// after every handler has exited, and the settler then drains every computed
// outcome. A retiring worker exits at the next loop boundary after finishing
// its current settle.
func (r *Runner) settlerLoop(runCtx context.Context, settle BatchSettler, report ErrorSink, outcomesClosed <-chan struct{}, quit <-chan struct{}) {
	for {
		select {
		case <-runCtx.Done():
			return
		case <-quit:
			return
		case <-outcomesClosed:
			r.drainOutcomes(runCtx, settle, report, quit)
			return
		case item, ok := <-r.outcomes:
			if !ok {
				return
			}
			r.settleOutcome(runCtx, item, settle, report)
		}
	}
}

// drainOutcomes settles every outcome still buffered after the handlers have
// stopped. The shared outcomes channel is never closed and no producer
// remains, so each settler takes items until the channel is empty.
func (r *Runner) drainOutcomes(runCtx context.Context, settle BatchSettler, report ErrorSink, quit <-chan struct{}) {
	for {
		select {
		case <-runCtx.Done():
			return
		case <-quit:
			return
		case item, ok := <-r.outcomes:
			if !ok {
				return
			}
			r.settleOutcome(runCtx, item, settle, report)
		default:
			return
		}
	}
}

// settleOutcome settles one computed outcome and releases its permit and
// batch slot. The generation check lives here, not in the handler, so an
// outcome produced by an abandoned handler of an earlier Run is never
// settled after a newer Run has started.
func (r *Runner) settleOutcome(runCtx context.Context, item outcomeItem, settle BatchSettler, report ErrorSink) {
	defer func() {
		r.slots <- struct{}{}
		r.permits <- struct{}{}
	}()
	if runCtx.Err() != nil {
		// Grace expired while the outcome waited; Recover closes the batch.
		return
	}
	if r.runGen.Load() != item.gen {
		// A newer Run has started while this outcome was still in flight:
		// an old generation must never settle, so the batch is left to
		// Recover instead of risking two generations closing it.
		return
	}
	r.cfg.Metrics.SettleStarted()
	receipt, err := settle.Settle(runCtx, item.batch.ID, item.outcome)
	if err != nil {
		if runCtx.Err() != nil {
			// Cancellation noise during shutdown; not a real failure.
			return
		}
		report(fmt.Errorf("echoqueue consumer: settle batch %q: %w", item.batch.ID, err))
		if errors.Is(err, echoqueue.ErrTransientRedis) {
			// A transient Redis interaction failure trips the circuit
			// breaker; validation and business rejections do not.
			r.cfg.Metrics.SettleFailed()
			r.recordSettleFailure()
		}
		return
	}
	if r.runGen.Load() != item.gen {
		// A newer Run started while this Settle was in flight: the terminal
		// write already happened, but its bookkeeping must not reset the new
		// generation's breaker counters.
		return
	}
	r.cfg.Metrics.SettleSucceeded()
	r.recordSettleSuccess()
	if receipt.Status == echoqueue.ReceiptInvalid {
		report(fmt.Errorf("echoqueue consumer: settle batch %q returned an invalid receipt", item.batch.ID))
	}
}

// recordSettleFailure opens the circuit breaker after breakerThreshold
// consecutive transient Settle failures. The breaker pauses Dispatch until a
// Settle succeeds again, so a Redis outage never turns into unbounded Source
// prefetch. The mutex keeps "consecutive" meaningful under concurrent
// settlers: a success resets the streak atomically with the open state.
func (r *Runner) recordSettleFailure() {
	r.breakerMu.Lock()
	opened := false
	r.breakerFailures++
	if r.breakerFailures >= breakerThreshold && !r.breakerOpen {
		r.breakerOpen = true
		opened = true
	}
	r.breakerMu.Unlock()
	// Metrics callbacks run outside the lock so a blocking sink cannot stall
	// breaker bookkeeping or the dispatchers' open-state checks.
	if opened {
		r.cfg.Metrics.BreakerOpened()
	}
}

func (r *Runner) recordSettleSuccess() {
	r.breakerMu.Lock()
	closed := false
	r.breakerFailures = 0
	if r.breakerOpen {
		r.breakerOpen = false
		closed = true
	}
	r.breakerMu.Unlock()
	if closed {
		r.cfg.Metrics.BreakerClosed()
	}
}
