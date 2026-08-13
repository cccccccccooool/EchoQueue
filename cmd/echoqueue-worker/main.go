// Command echoqueue-worker is the runnable host reference for EchoQueue
// bounded consumption and dead archiving. It wires the public EchoQueue API
// to the consumer pipeline and the dead archiver, and demonstrates the
// required ordering: acquire permit -> Dispatch -> Handle -> Settle ->
// release permit.
//
// It deliberately contains no concrete external storage: the DeadSink below
// logs only and exists to show the host-local injection point. A production
// host must supply an idempotent-by-effect_id sink backed by its own database,
// object storage, or file system.
//
// Run with:
//
//	go run ./cmd/echoqueue-worker -addr 127.0.0.1:6379
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/cccccccccooool/EchoQueue/consumer"
	"github.com/redis/go-redis/v9"
)

func main() {
	runnerDefaults := consumer.DefaultConfig()
	archiverDefaults := defaultArchiverConfig()
	addr := flag.String("addr", envOr("ECHOQUEUE_REDIS_ADDR", "127.0.0.1:6379"), "Redis address")
	namespace := flag.String("namespace", "echoqueue-worker", "EchoQueue namespace")
	taskName := flag.String("task-name", "invoice", "QueueConfig task name")
	source := flag.String("source", "echoqueue-worker:invoice:source", "Source list key")
	result := flag.String("result", "echoqueue-worker:invoice:result", "Result list key")
	dead := flag.String("dead", "echoqueue-worker:invoice:dead", "Dead list key")
	processing := flag.String("processing", "echoqueue-worker:invoice:dead-processing", "DeadProcessing list key")
	dispatchers := flag.Int("dispatchers", runnerDefaults.Dispatchers, "parallel Dispatch goroutines")
	workers := flag.Int("workers", runnerDefaults.Workers, "handler goroutines")
	settlers := flag.Int("settlers", runnerDefaults.Settlers, "parallel Settle goroutines")
	maxInFlight := flag.Int("max-in-flight", runnerDefaults.MaxInFlight, "max dispatched-not-settled batches")
	batchSize := flag.Int("batch-size", runnerDefaults.BatchSize, "tasks per Dispatch call")
	batchBuffer := flag.Int("batch-buffer", runnerDefaults.BatchBuffer, "buffered dispatched batches waiting for a handler")
	outcomeBuffer := flag.Int("outcome-buffer", runnerDefaults.OutcomeBuffer, "buffered outcomes waiting for a settler")
	pollInterval := flag.Duration("poll-interval", runnerDefaults.PollInterval, "pause after an empty Dispatch")
	errorBackoff := flag.Duration("error-backoff", runnerDefaults.ErrorBackoff, "jittered pause after Dispatch/Settle errors")
	grace := flag.Duration("shutdown-grace", runnerDefaults.ShutdownGrace, "grace for in-flight batches after cancellation")
	archiveBatch := flag.Int("archive-batch", archiverDefaults.BatchSize, "max dead records per persist batch")
	enableArchive := flag.Bool("enable-archive", false, "start the dead archiver; the reference logging sink does not persist, so records are claimed but never acknowledged")
	flag.Parse()

	if *enableArchive && (*dead == "" || *processing == "" || *dead == *processing) {
		log.Fatal("the archiver requires an explicit, distinct dead and processing key")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer func() {
		if err := rdb.Close(); err != nil {
			reportError(fmt.Errorf("close Redis client: %w", err))
		}
	}()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	cfg := echoqueue.DefaultConfig()
	cfg.Namespace = *namespace
	scheduler, err := echoqueue.New(rdb, cfg)
	if err != nil {
		log.Fatalf("echoqueue.New: %v", err)
	}
	queue, err := scheduler.Bind(echoqueue.QueueConfig{
		TaskName: *taskName,
		Source:   *source,
		Result:   *result,
		Dead:     *dead,
	})
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// Recovery belongs to the host lifecycle: Run errors are observed and
	// restarted, never swallowed.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	go runRecoveryLoop(runCtx, scheduler)

	runner, err := consumer.New(consumer.Config{
		Dispatchers:   *dispatchers,
		Workers:       *workers,
		Settlers:      *settlers,
		MaxInFlight:   *maxInFlight,
		BatchSize:     *batchSize,
		BatchBuffer:   *batchBuffer,
		OutcomeBuffer: *outcomeBuffer,
		PollInterval:  *pollInterval,
		ErrorBackoff:  *errorBackoff,
		ShutdownGrace: *grace,
	})
	if err != nil {
		log.Fatalf("consumer.New: %v", err)
	}
	runnerErr := make(chan error, 1)
	go func() {
		runnerErr <- runner.Run(ctx, queue, scheduler, referenceHandler, reportError)
	}()

	var archiveErr <-chan error

	if *enableArchive {
		// The reference sink never persists, so enabling the archiver moves
		// records to DeadProcessing but never acknowledges them. A production
		// host must provide its own idempotent DeadSink before records are
		// ACKed away.
		archiver, err := NewDeadArchiver(rdb, ArchiverConfig{
			DeadKey:       *dead,
			ProcessingKey: *processing,
			BatchSize:     *archiveBatch,
			FlushInterval: archiverDefaults.FlushInterval,
			ClaimTimeout:  archiverDefaults.ClaimTimeout,
			ErrorBackoff:  archiverDefaults.ErrorBackoff,
		}, LoggingDeadSink{})
		if err != nil {
			log.Fatalf("NewDeadArchiver: %v", err)
		}
		archiveDone := make(chan error, 1)
		archiveErr = archiveDone
		go func() {
			archiveDone <- archiver.Run(ctx, reportError)
		}()
		log.Printf("dead archiver enabled with the non-persisting logging sink; dead records will be claimed into %q but never deleted", *processing)
	} else {
		log.Printf("dead archiver disabled; dead records remain in %q until a production sink is wired in", *dead)
	}

	log.Printf("consuming %q with %d dispatchers, %d workers, %d settlers, %d in-flight, %d batch buffer, %d outcome buffer",
		*source, *dispatchers, *workers, *settlers, *maxInFlight, *batchBuffer, *outcomeBuffer)
	select {
	case <-ctx.Done():
	case err := <-runnerErr:
		reportUnexpectedStop("consumer runner", err)
		runnerErr = nil
	case err := <-archiveErr:
		reportUnexpectedStop("dead archiver", err)
		archiveErr = nil
	}
	stop()
	waitForComponent("consumer runner", runnerErr, *grace+time.Second)
	waitForComponent("dead archiver", archiveErr, archiverDefaults.ClaimTimeout+archiverDefaults.ErrorBackoff+time.Second)
	if *enableArchive {
		log.Printf("shutting down; unacked dead records stay in %q", *processing)
	} else {
		log.Printf("shutting down; dead records stay in %q", *dead)
	}
}

// runRecoveryLoop restarts Scheduler.Run until shutdown. Run errors carry the
// first failed batch ID and are logged, never swallowed.
func runRecoveryLoop(ctx context.Context, scheduler *echoqueue.Scheduler) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := scheduler.Run(ctx); err != nil && ctx.Err() == nil {
			reportError(fmt.Errorf("scheduler.Run: %w", err))
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

// referenceHandler is intentionally minimal: it demonstrates how a worker
// covers every task in an Outcome without pretending that a placeholder URL
// or hash was durably produced by real business logic.
func referenceHandler(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
	outcome := echoqueue.Outcome{RequestID: "host-worker-" + batch.ID}
	for _, task := range batch.Tasks {
		if ctx.Err() != nil {
			outcome.Failures = append(outcome.Failures, echoqueue.Failure{
				TaskID:    task.TaskID,
				Reason:    "host shutdown before processing",
				Retryable: true,
			})
			continue
		}
		outcome.Results = append(outcome.Results, echoqueue.Result{
			TaskID: task.TaskID,
			Data:   json.RawMessage(`{"processed":true}`),
		})
	}
	return outcome
}

func reportError(err error) {
	log.Printf("error: %v", err)
}

func reportUnexpectedStop(name string, err error) {
	if err != nil {
		reportError(fmt.Errorf("%s stopped: %w", name, err))
		return
	}
	reportError(fmt.Errorf("%s stopped unexpectedly", name))
}

func waitForComponent(name string, done <-chan error, timeout time.Duration) {
	if done == nil {
		return
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			reportError(fmt.Errorf("%s shutdown: %w", name, err))
		}
	case <-time.After(timeout):
		reportError(fmt.Errorf("%s did not stop within %s", name, timeout))
	}
}

// LoggingDeadSink is a demonstration sink only: it does not persist anything
// and returns an error so the archiver never acknowledges (deletes) a Redis
// record. It exists to show the DeadSink injection point. A real sink must be
// idempotent by EffectID and return nil only after durable persistence.
type LoggingDeadSink struct{}

func (LoggingDeadSink) PersistDead(ctx context.Context, records []DeadRecord) error {
	for _, record := range records {
		log.Printf("reference sink: would persist dead effect_id=%s (not durable, not acknowledged)", record.EffectID)
	}
	return errors.New("echoqueue worker: the logging sink does not persist; nothing was acknowledged")
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
