// Command consumer_matrix measures the consumer pipeline across a
// Dispatchers x Workers x Settlers matrix. Each cell runs a unique Redis
// namespace, drains a fixed task load through consumer.Runner, and reports
// throughput plus Dispatch/Settle latency percentiles so the tuning question
// "does adding concurrency still raise throughput?" can be answered with
// data. No failures are injected and no recovery loop is needed: a cell only
// finishes once every task reached a unique Result.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/cccccccccooool/EchoQueue/consumer"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type cell struct {
	dispatchers int
	workers     int
	settlers    int
}

type cellResult struct {
	throughput float64
	elapsed    time.Duration
	dispatch   map[float64]time.Duration
	settle     map[float64]time.Duration
	unique     int
	dead       int
}

func main() {
	address := flag.String("addr", envString("ECHOQUEUE_REDIS_ADDR", "127.0.0.1:6380"), "Redis address")
	tasks := flag.Int("tasks", envInt("ECHOQUEUE_MATRIX_TASKS", 30000), "tasks per matrix cell")
	batchSize := flag.Int("batch-size", envInt("ECHOQUEUE_MATRIX_BATCH_SIZE", 32), "tasks per Dispatch call")
	maxInFlight := flag.Int("max-in-flight", envInt("ECHOQUEUE_MATRIX_MAX_IN_FLIGHT", 16), "MaxInFlight per cell")
	batchBuffer := flag.Int("batch-buffer", envInt("ECHOQUEUE_MATRIX_BATCH_BUFFER", 8), "BatchBuffer per cell")
	outcomeBuffer := flag.Int("outcome-buffer", envInt("ECHOQUEUE_MATRIX_OUTCOME_BUFFER", 8), "OutcomeBuffer per cell")
	matrix := flag.String("matrix", envString("ECHOQUEUE_MATRIX", "1x1x1,1x2x1,1x4x2,2x4x2,4x8x2,4x8x4,8x16x8,16x16x8"), "comma-separated DispatchersxWorkersxSettlers cells")
	timeout := flag.Duration("timeout", envDuration("ECHOQUEUE_MATRIX_TIMEOUT", 120*time.Second), "per-cell timeout")
	flag.Parse()

	if *tasks <= 0 || *batchSize <= 0 || *maxInFlight <= 0 || *batchBuffer <= 0 || *outcomeBuffer <= 0 || *timeout <= 0 {
		fatalf("tasks, batch-size, max-in-flight, batch-buffer, outcome-buffer and timeout must be positive")
	}
	cells, err := parseMatrix(*matrix)
	if err != nil {
		fatalf("matrix: %v", err)
	}

	fmt.Printf("consumer matrix: redis=%s tasks=%d batch=%d max_in_flight=%d batch_buffer=%d outcome_buffer=%d timeout=%s\n",
		*address, *tasks, *batchSize, *maxInFlight, *batchBuffer, *outcomeBuffer, timeout)
	fmt.Println("every cell uses unique keys and cleans only its own Redis data.")
	fmt.Printf("%-12s %10s %10s %10s %10s %12s %12s\n", "cell", "throughput", "dispatch", "dispatch", "settle", "dispatch", "settle")
	fmt.Printf("%-12s %10s %10s %10s %10s %12s %12s\n", "", "tasks/s", "p50", "p95", "p50", "p99", "p99")

	for _, current := range cells {
		result, err := runCell(context.Background(), *address, *tasks, *batchSize, *maxInFlight, *batchBuffer, *outcomeBuffer, *timeout, current)
		if err != nil {
			fatalf("cell %dx%dx%d failed: %v", current.dispatchers, current.workers, current.settlers, err)
		}
		fmt.Printf("%dx%dx%d %10.0f %10s %10s %10s %12s %12s\n",
			current.dispatchers, current.workers, current.settlers,
			result.throughput,
			result.dispatch[0.5].Round(time.Microsecond),
			result.dispatch[0.95].Round(time.Microsecond),
			result.settle[0.5].Round(time.Microsecond),
			result.dispatch[0.99].Round(time.Microsecond),
			result.settle[0.99].Round(time.Microsecond))
		fmt.Printf("  elapsed=%s unique=%d dead=%d loss=%d\n",
			result.elapsed.Round(time.Millisecond), result.unique, result.dead, *tasks-result.unique-result.dead)
	}
}

func runCell(parent context.Context, address string, tasks, batchSize, maxInFlight, batchBuffer, outcomeBuffer int, timeout time.Duration, current cell) (cellResult, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         address,
		PoolSize:     maxInt(64, maxInFlight*2),
		MinIdleConns: 8,
	})
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	uid := uuid.NewString()
	namespace := "echoqueue-matrix-" + uid
	source := "echoqueue-matrix:source:" + uid
	resultKey := "echoqueue-matrix:result:" + uid
	deadKey := "echoqueue-matrix:dead:" + uid
	defer func() {
		cleanupKeys(context.Background(), client, namespace, source, resultKey, deadKey)
		_ = client.Close()
	}()

	config := echoqueue.DefaultConfig()
	config.Namespace = namespace
	config.VisibilityTimeout = 30 * time.Second
	config.ReceiptTTL = 10 * time.Minute
	config.MaxRetry = 0
	config.MaxRetrySet = true
	config.MaxBatchSize = batchSize
	config.MaxPayloadBytes = 4096
	config.MaxBatchBytes = 64 << 20
	scheduler, err := echoqueue.New(client, config)
	if err != nil {
		return cellResult{}, err
	}
	queue, err := scheduler.Bind(echoqueue.QueueConfig{
		TaskName: "matrix",
		Source:   source,
		Result:   resultKey,
		Dead:     deadKey,
	})
	if err != nil {
		return cellResult{}, err
	}

	if err := pushTasks(ctx, client, source, tasks); err != nil {
		return cellResult{}, err
	}

	dispatchLat := &latencyRecorder{}
	settleLat := &latencyRecorder{}
	runner, err := consumer.New(consumer.Config{
		Dispatchers:   current.dispatchers,
		Workers:       current.workers,
		Settlers:      current.settlers,
		MaxInFlight:   maxInFlight,
		BatchSize:     batchSize,
		BatchBuffer:   batchBuffer,
		OutcomeBuffer: outcomeBuffer,
		PollInterval:  time.Millisecond,
		ErrorBackoff:  10 * time.Millisecond,
		ShutdownGrace: 5 * time.Second,
	})
	if err != nil {
		return cellResult{}, err
	}

	runErr := make(chan error, 1)
	var firstErr error
	var errorMu sync.Mutex
	go func() {
		runErr <- runner.Run(ctx,
			&timedDispatcher{queue: queue, latency: dispatchLat},
			&timedSettler{scheduler: scheduler, latency: settleLat},
			func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
				outcome := echoqueue.Outcome{RequestID: "matrix-" + batch.ID}
				for _, task := range batch.Tasks {
					outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: json.RawMessage(`{"ok":true}`)})
				}
				return outcome
			},
			func(err error) {
				if err == nil {
					return
				}
				fmt.Fprintf(os.Stderr, "cell %dx%dx%d: %v\n", current.dispatchers, current.workers, current.settlers, err)
				errorMu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				errorMu.Unlock()
			})
	}()

	drainStart := time.Now()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			errorMu.Lock()
			err := firstErr
			errorMu.Unlock()
			if err != nil {
				return cellResult{}, fmt.Errorf("runner reported: %w", err)
			}
			break
		}
		resultLen, err := client.LLen(context.Background(), resultKey).Result()
		if err != nil {
			return cellResult{}, fmt.Errorf("poll result length: %w", err)
		}
		sourceLeft, err := client.LLen(context.Background(), source).Result()
		if err != nil {
			return cellResult{}, fmt.Errorf("poll source length: %w", err)
		}
		if resultLen == int64(tasks) && sourceLeft == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(drainStart)

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			return cellResult{}, err
		}
	case <-time.After(10 * time.Second):
		return cellResult{}, fmt.Errorf("runner did not stop after cancellation")
	}

	remaining, err := client.LLen(context.Background(), source).Result()
	if err != nil {
		return cellResult{}, err
	}
	dead, err := client.LLen(context.Background(), deadKey).Result()
	if err != nil {
		return cellResult{}, err
	}
	if remaining != 0 {
		return cellResult{}, fmt.Errorf("source remaining = %d (timeout?)", remaining)
	}
	unique, err := verifyResultIDs(context.Background(), client, resultKey)
	if err != nil {
		return cellResult{}, err
	}
	if unique != tasks {
		return cellResult{}, fmt.Errorf("unique results = %d, want %d", unique, tasks)
	}
	return cellResult{
		throughput: float64(tasks) / elapsed.Seconds(),
		elapsed:    elapsed,
		dispatch:   dispatchLat.percentiles(0.5, 0.95, 0.99),
		settle:     settleLat.percentiles(0.5, 0.95, 0.99),
		unique:     unique,
		dead:       int(dead),
	}, nil
}

func pushTasks(ctx context.Context, client *redis.Client, source string, count int) error {
	const chunk = 512
	for offset := 0; offset < count; offset += chunk {
		end := minInt(offset+chunk, count)
		values := make([]interface{}, 0, end-offset)
		for index := offset; index < end; index++ {
			values = append(values, fmt.Sprintf(`{"task_id":"matrix-%08d","retry_count":0,"payload":{"value":%d}}`, index, index))
		}
		if err := client.RPush(ctx, source, values...).Err(); err != nil {
			return err
		}
	}
	return nil
}

type timedDispatcher struct {
	queue   *echoqueue.Queue
	latency *latencyRecorder
}

func (d *timedDispatcher) Dispatch(ctx context.Context, batchSize int) (echoqueue.Batch, error) {
	start := time.Now()
	batch, err := d.queue.Dispatch(ctx, batchSize)
	d.latency.record(time.Since(start))
	return batch, err
}

type timedSettler struct {
	scheduler *echoqueue.Scheduler
	latency   *latencyRecorder
}

func (s *timedSettler) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	start := time.Now()
	receipt, err := s.scheduler.Settle(ctx, batchID, outcome)
	s.latency.record(time.Since(start))
	return receipt, err
}

type latencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (r *latencyRecorder) record(sample time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, sample)
	r.mu.Unlock()
}

func (r *latencyRecorder) percentiles(ps ...float64) map[float64]time.Duration {
	r.mu.Lock()
	sorted := append([]time.Duration(nil), r.samples...)
	r.mu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	result := map[float64]time.Duration{}
	for _, p := range ps {
		if len(sorted) == 0 {
			result[p] = 0
			continue
		}
		index := int(float64(len(sorted)-1) * p)
		if index < 0 {
			index = 0
		}
		if index >= len(sorted) {
			index = len(sorted) - 1
		}
		result[p] = sorted[index]
	}
	return result
}

func parseMatrix(raw string) ([]cell, error) {
	var cells []cell
	for _, value := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(value), "x")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid cell %q, want DispatchersxWorkersxSettlers", value)
		}
		dispatchers, err := strconv.Atoi(parts[0])
		if err != nil || dispatchers < 1 {
			return nil, fmt.Errorf("invalid cell %q", value)
		}
		workers, err := strconv.Atoi(parts[1])
		if err != nil || workers < 1 {
			return nil, fmt.Errorf("invalid cell %q", value)
		}
		settlers, err := strconv.Atoi(parts[2])
		if err != nil || settlers < 1 {
			return nil, fmt.Errorf("invalid cell %q", value)
		}
		cells = append(cells, cell{dispatchers: dispatchers, workers: workers, settlers: settlers})
	}
	if len(cells) == 0 {
		return nil, fmt.Errorf("at least one matrix cell is required")
	}
	return cells, nil
}

func cleanupKeys(ctx context.Context, client *redis.Client, namespace string, hostKeys ...string) {
	cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	keys := append([]string{}, hostKeys...)
	prefix := "echoqueue:1:" + base64.RawURLEncoding.EncodeToString([]byte(namespace))
	var cursor uint64
	for {
		found, nextCursor, err := client.Scan(cleanupCtx, cursor, prefix+"*", 256).Result()
		if err != nil {
			break
		}
		keys = append(keys, found...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	if len(keys) > 0 {
		_, _ = client.Del(cleanupCtx, keys...).Result()
	}
}

func verifyResultIDs(ctx context.Context, client *redis.Client, key string) (int, error) {
	records, err := client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(records))
	for _, raw := range records {
		var record struct {
			TaskID   string `json:"task_id"`
			EffectID string `json:"effect_id"`
		}
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			return 0, fmt.Errorf("decode result record: %w", err)
		}
		if record.TaskID == "" || record.EffectID == "" {
			return 0, fmt.Errorf("result record is missing task_id or effect_id")
		}
		if _, exists := seen[record.TaskID]; exists {
			return 0, fmt.Errorf("duplicate result task_id %q", record.TaskID)
		}
		seen[record.TaskID] = struct{}{}
	}
	return len(seen), nil
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(envString(name, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(envString(name, fallback.String()))
	if err != nil {
		return fallback
	}
	return value
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
