// Command reliable_consumer is the runnable host reference for EchoQueue
// bounded consumption and dead archiving. It wires the public EchoQueue API to
// the bounded pool and the dead archiver, and demonstrates the required
// ordering: acquire permit -> Dispatch -> Handle -> Settle -> release permit.
//
// It deliberately contains no concrete external storage: the DeadSink below
// logs only and exists to show the host-local injection point. A production
// host must supply an idempotent-by-effect_id sink backed by its own database,
// object storage, or file system.
//
// Run with:
//
//	go run ./examples/reliable_consumer -addr 127.0.0.1:6379
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

	echoqueue "echoqueue"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", envOr("ECHOQUEUE_REDIS_ADDR", "127.0.0.1:6379"), "Redis address")
	namespace := flag.String("namespace", "reliable-consumer-example", "EchoQueue namespace")
	taskName := flag.String("task-name", "invoice", "QueueConfig task name")
	source := flag.String("source", "example:invoice:source", "Source list key")
	result := flag.String("result", "example:invoice:result", "Result list key")
	dead := flag.String("dead", "example:invoice:dead", "Dead list key")
	processing := flag.String("processing", "example:invoice:dead-processing", "DeadProcessing list key")
	workers := flag.Int("workers", 4, "pool worker goroutines")
	maxInFlight := flag.Int("max-in-flight", 8, "max dispatched-not-settled batches")
	buffer := flag.Int("buffer", 16, "pool buffer slots")
	batchSize := flag.Int("batch-size", 1, "tasks per Dispatch call")
	grace := flag.Duration("shutdown-grace", 5*time.Second, "grace for in-flight batches after cancellation")
	archiveBatch := flag.Int("archive-batch", 64, "max dead records per persist batch")
	enableArchive := flag.Bool("enable-archive", false, "start the dead archiver; the reference logging sink does not persist, so records are claimed but never acknowledged")
	flag.Parse()

	if *dead == "" || *processing == "" || *dead == *processing {
		log.Fatal("the archiver requires an explicit, distinct dead and processing key")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *addr})
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

	pool, err := NewPool(PoolConfig{
		Workers:       *workers,
		MaxInFlight:   *maxInFlight,
		Buffer:        *buffer,
		BatchSize:     *batchSize,
		PollInterval:  time.Second,
		ShutdownGrace: *grace,
	})
	if err != nil {
		log.Fatalf("NewPool: %v", err)
	}
	poolErr := make(chan error, 1)
	go func() {
		poolErr <- pool.Run(ctx, queue, scheduler, exampleHandler, reportError)
	}()
	// In a real host, a pool Run error (e.g. misuse) would be observed here
	// and the pool restarted. The reference logs it.

	if *enableArchive {
		// The reference sink never persists, so enabling the archiver moves
		// records to DeadProcessing but never acknowledges them. A production
		// host must provide its own idempotent DeadSink before records are
		// ACKed away.
		archiver, err := NewDeadArchiver(rdb, ArchiverConfig{
			DeadKey:       *dead,
			ProcessingKey: *processing,
			BatchSize:     *archiveBatch,
			FlushInterval: time.Second,
			ClaimTimeout:  time.Second,
			ErrorBackoff:  500 * time.Millisecond,
		}, LoggingDeadSink{})
		if err != nil {
			log.Fatalf("NewDeadArchiver: %v", err)
		}
		archiveErr := make(chan error, 1)
		go func() {
			archiveErr <- archiver.Run(ctx, reportError)
		}()
		log.Printf("dead archiver enabled with the non-persisting logging sink; dead records will be claimed into %q but never deleted", *processing)
	} else {
		log.Printf("dead archiver disabled; dead records remain in %q until a production sink is wired in", *dead)
	}

	log.Printf("consuming %q with %d workers, %d in-flight, %d buffer", *source, *workers, *maxInFlight, *buffer)
	<-ctx.Done()
	stop()
	log.Printf("shutting down; unacked dead records stay in %q", *processing)
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

// exampleHandler shows how a worker turns business results and failures into
// an Outcome. Large business data must live in external storage; the Result
// carries a reference, a size, and a hash instead.
func exampleHandler(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
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
			Data: json.RawMessage(`{
				"ref": "example-bucket/` + task.TaskID + `",
				"size": 42,
				"sha256": "external-content-hash-here"
			}`),
		})
	}
	return outcome
}

func reportError(err error) {
	log.Printf("error: %v", err)
}

// LoggingDeadSink is a demonstration sink only: it does not persist anything
// and returns an error so the archiver never acknowledges (deletes) a Redis
// record. It exists to show the DeadSink injection point. A real sink must be
// idempotent by EffectID and return nil only after durable persistence.
type LoggingDeadSink struct{}

func (LoggingDeadSink) PersistDead(ctx context.Context, records []DeadRecord) error {
	for _, record := range records {
		log.Printf("example sink: would persist dead effect_id=%s (not durable, not acknowledged)", record.EffectID)
	}
	return errors.New("echoqueue example: the logging sink does not persist; nothing was acknowledged")
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
