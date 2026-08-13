package consumer

import (
	"context"

	echoqueue "github.com/cccccccccooool/EchoQueue"
)

// workerLoop runs one handler. On a graceful shutdown the coordinator closes
// the batchesClosed signal only after every dispatcher has exited, and the
// handler then drains every already-dispatched batch instead of abandoning
// it to Recover.
func (r *Runner) workerLoop(runCtx context.Context, gen int64, handle BatchHandler, batchesClosed <-chan struct{}) {
	for {
		select {
		case <-runCtx.Done():
			// Grace expired: batches still in the buffer are abandoned to
			// Pending/Recover; the handler does not compute or settle them.
			return
		case <-batchesClosed:
			// No dispatcher can still be producing; drain the remainder.
			r.drainBatches(runCtx, gen, handle)
			return
		case batch := <-r.batches:
			r.workBatch(runCtx, gen, batch, handle)
		}
	}
}

// drainBatches finishes every batch still buffered after the dispatchers
// have stopped. The shared batches channel is never closed and no producer
// remains, so each handler takes items until the channel is empty.
func (r *Runner) drainBatches(runCtx context.Context, gen int64, handle BatchHandler) {
	for {
		select {
		case <-runCtx.Done():
			return
		case batch := <-r.batches:
			r.workBatch(runCtx, gen, batch, handle)
		default:
			return
		}
	}
}

// workBatch computes the outcome for one batch and hands it to the outcome
// channel. The handler never touches Redis and never settles; the settler
// owns the generation check and the Settle call.
//
// The permit and batch slot travel with the batch from Dispatch to Settle,
// so whoever closes the batch releases them: the settler after a normal
// settle, this handler when the batch is abandoned to Recover. The deferred
// release also covers a panicking host handle callback, so a panic can never
// permanently shrink the token pools.
func (r *Runner) workBatch(runCtx context.Context, gen int64, batch echoqueue.Batch, handle BatchHandler) {
	released := false
	defer func() {
		if !released {
			r.release()
		}
	}()
	// HandleStarted must run after the release defer is registered so a
	// panicking metrics implementation can never leak the batch's tokens.
	r.cfg.Metrics.HandleStarted()
	defer r.cfg.Metrics.HandleDone()
	outcome := handle(runCtx, batch)
	if runCtx.Err() != nil {
		// Handling was interrupted by cancellation: never fabricate a
		// terminal state for a partially handled batch. It stays under
		// Pending/deadline control and Recover closes it.
		return
	}
	item := outcomeItem{batch: batch, outcome: outcome, gen: gen}
	select {
	case <-runCtx.Done():
		// The outcome was computed but the grace window closed before the
		// hand-off; leave the batch to Recover.
	case r.outcomes <- item:
		// Handed off; the settler releases the permit and slot.
		released = true
	}
}
