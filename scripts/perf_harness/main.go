package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type scenario struct {
	name      string
	tasks     int
	producers int
	consumers int
	interval  time.Duration
	preload   bool
}

type scenarioResult struct {
	produced        int64
	processed       int64
	attempts        int64
	retryEffects    int64
	retryDeliveries int64
	peakBacklog     int64
	remaining       int64
	results         int64
	dead            int64
	lost            int64
	produceTime     time.Duration
	drainTime       time.Duration
	totalTime       time.Duration
}

type aggregateResult struct {
	logical         int64
	attempts        int64
	retryEffects    int64
	retryDeliveries int64
	results         int64
	dead            int64
	lost            int64
}

func main() {
	address := flag.String("addr", envString("ECHOQUEUE_REDIS_ADDR", "127.0.0.1:6380"), "Redis address")
	tasks := flag.Int("tasks", envInt("ECHOQUEUE_PERF_TASKS", 12000), "tasks for continuous and interval scenarios")
	pressureTasks := flag.Int("pressure-tasks", envInt("ECHOQUEUE_PERF_PRESSURE_TASKS", 16000), "tasks for each pressure level")
	batchSize := flag.Int("batch-size", envInt("ECHOQUEUE_PERF_BATCH_SIZE", 64), "Dispatch batch size")
	consumerInterval := flag.Duration("consumer-interval", envDuration("ECHOQUEUE_PERF_CONSUMER_INTERVAL", 20*time.Millisecond), "interval between interval-scenario batches")
	retryPercent := flag.Int("retry-percent", envInt("ECHOQUEUE_PERF_RETRY_PERCENT", 2), "deterministic first-attempt retry percentage")
	timeout := flag.Duration("timeout", envDuration("ECHOQUEUE_PERF_TIMEOUT", 180*time.Second), "per-scenario timeout")
	pressureLevels := flag.String("pressure-levels", envString("ECHOQUEUE_PERF_PRESSURE_LEVELS", "1,4,8"), "comma-separated producer/consumer concurrency levels")
	flag.Parse()

	if *tasks <= 0 || *pressureTasks <= 0 || *batchSize <= 0 || *timeout <= 0 || *retryPercent < 0 || *retryPercent > 100 {
		fatalf("tasks, pressure-tasks, batch-size and timeout must be positive; retry-percent must be 0..100")
	}
	levels, err := parseLevels(*pressureLevels)
	if err != nil {
		fatalf("pressure-levels: %v", err)
	}

	fmt.Printf("EchoQueue performance harness: redis=%s batch=%d timeout=%s\n", *address, *batchSize, timeout.String())
	fmt.Println("All scenarios use unique keys and clean only their own Redis data.")

	scenarios := []scenario{
		{name: "continuous", tasks: *tasks, producers: 4, consumers: 4},
		{name: "interval", tasks: *tasks, producers: 4, consumers: 2, interval: *consumerInterval, preload: true},
	}
	for _, level := range levels {
		scenarios = append(scenarios, scenario{
			name:      fmt.Sprintf("pressure-%dx", level),
			tasks:     *pressureTasks,
			producers: level,
			consumers: level,
		})
	}

	var overall aggregateResult
	for _, current := range scenarios {
		result, err := runScenario(context.Background(), *address, *batchSize, *retryPercent, *timeout, current)
		if err != nil {
			fatalf("%s failed: %v", current.name, err)
		}
		printResult(current, result)
		overall.add(result)
	}
	printOverall(overall)
}

func runScenario(parent context.Context, address string, batchSize, retryPercent int, timeout time.Duration, current scenario) (scenarioResult, error) {
	if current.producers < 1 || current.consumers < 1 {
		return scenarioResult{}, errors.New("producer and consumer counts must be positive")
	}
	if current.tasks < current.producers {
		current.producers = current.tasks
	}

	client := redis.NewClient(&redis.Options{
		Addr:         address,
		PoolSize:     maxInt(32, current.producers+current.consumers*2),
		MinIdleConns: maxInt(8, current.consumers),
	})
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	uid := uuid.NewString()
	namespace := "echoqueue-perf-" + current.name + "-" + uid
	source := "echoqueue-perf:source:" + uid
	resultKey := "echoqueue-perf:result:" + uid
	deadKey := "echoqueue-perf:dead:" + uid
	cleanup := func() {
		cleanupKeys(context.Background(), client, namespace, source, resultKey, deadKey)
		_ = client.Close()
	}
	defer cleanup()

	config := echoqueue.DefaultConfig()
	config.Namespace = namespace
	config.VisibilityTimeout = 30 * time.Second
	config.ReceiptTTL = 10 * time.Minute
	config.MaxRetry = 1
	config.MaxRetrySet = true
	config.MaxBatchSize = batchSize
	config.MaxPayloadBytes = 4096
	config.MaxBatchBytes = 64 << 20
	config.RunInterval = 100 * time.Millisecond
	config.RunBatchSize = batchSize
	scheduler, err := echoqueue.New(client, config)
	if err != nil {
		return scenarioResult{}, err
	}
	queue, err := scheduler.Bind(echoqueue.QueueConfig{
		TaskName: current.name,
		Source:   source,
		Result:   resultKey,
		Dead:     deadKey,
	})
	if err != nil {
		return scenarioResult{}, err
	}

	var produced atomic.Int64
	var processed atomic.Int64
	var attempts atomic.Int64
	var retryEffects atomic.Int64
	var retryDeliveries atomic.Int64
	var peakBacklog atomic.Int64
	var firstErr error
	var errorMu sync.Mutex
	recordError := func(runErr error) {
		if runErr == nil || errors.Is(runErr, context.Canceled) {
			return
		}
		errorMu.Lock()
		if firstErr == nil {
			firstErr = runErr
			cancel()
		}
		errorMu.Unlock()
	}

	producerDone := make(chan struct{})
	producerDuration := atomic.Int64{}
	runProducers := func() error {
		start := time.Now()
		var wg sync.WaitGroup
		workerErr := make(chan error, current.producers)
		for worker := 0; worker < current.producers; worker++ {
			startIndex := current.tasks * worker / current.producers
			endIndex := current.tasks * (worker + 1) / current.producers
			wg.Add(1)
			go func(worker, startIndex, endIndex int) {
				defer wg.Done()
				const pushChunk = 256
				for offset := startIndex; offset < endIndex; offset += pushChunk {
					if err := ctx.Err(); err != nil {
						workerErr <- err
						return
					}
					end := minInt(offset+pushChunk, endIndex)
					values := make([]interface{}, 0, end-offset)
					for index := offset; index < end; index++ {
						values = append(values, taskJSON(current.name, index))
					}
					if err := client.RPush(ctx, source, values...).Err(); err != nil {
						workerErr <- err
						return
					}
					produced.Add(int64(end - offset))
				}
			}(worker, startIndex, endIndex)
		}
		wg.Wait()
		close(workerErr)
		for workerErrValue := range workerErr {
			if workerErrValue != nil {
				producerDuration.Store(time.Since(start).Nanoseconds())
				return workerErrValue
			}
		}
		producerDuration.Store(time.Since(start).Nanoseconds())
		return nil
	}

	var consumers sync.WaitGroup
	overallStart := time.Now()
	drainStart := overallStart
	startConsumers := func() {
		for worker := 0; worker < current.consumers; worker++ {
			consumers.Add(1)
			go func() {
				defer consumers.Done()
				for {
					if processed.Load() >= int64(current.tasks) {
						return
					}
					batch, dispatchErr := queue.Dispatch(ctx, batchSize)
					if dispatchErr != nil {
						if !errors.Is(dispatchErr, context.Canceled) {
							recordError(dispatchErr)
						}
						return
					}
					if batch.ID == "" {
						if ctx.Err() != nil {
							return
						}
						time.Sleep(time.Millisecond)
						continue
					}
					if current.interval > 0 {
						timer := time.NewTimer(current.interval)
						select {
						case <-timer.C:
						case <-ctx.Done():
							if !timer.Stop() {
								<-timer.C
							}
							return
						}
					}
					results := make([]echoqueue.Result, 0, len(batch.Tasks))
					failures := make([]echoqueue.Failure, 0, len(batch.Tasks))
					for _, task := range batch.Tasks {
						attempts.Add(1)
						if task.RetryCount > 0 {
							retryDeliveries.Add(1)
						}
						if retryCandidate(task, retryPercent) {
							failures = append(failures, echoqueue.Failure{TaskID: task.TaskID, Reason: "synthetic performance retry", Retryable: true})
							retryEffects.Add(1)
							continue
						}
						results = append(results, echoqueue.Result{TaskID: task.TaskID, Data: json.RawMessage(`{"ok":true}`)})
					}
					receipt, settleErr := scheduler.Settle(ctx, batch.ID, echoqueue.Outcome{
						RequestID: "perf-" + batch.ID,
						Results:   results,
						Failures:  failures,
					})
					if settleErr != nil {
						if !errors.Is(settleErr, context.Canceled) {
							recordError(settleErr)
						}
						return
					}
					if receipt.Status != echoqueue.ReceiptApplied {
						recordError(fmt.Errorf("unexpected settle status %q for batch %s", receipt.Status, batch.ID))
						return
					}
					processed.Add(int64(len(results)))
					backlog, backlogErr := client.LLen(ctx, source).Result()
					if backlogErr != nil {
						recordError(backlogErr)
						return
					}
					updateMax(&peakBacklog, backlog)
				}
			}()
		}
	}

	if current.preload {
		if err := runProducers(); err != nil {
			close(producerDone)
			return scenarioResult{}, err
		}
		close(producerDone)
		backlog, backlogErr := client.LLen(ctx, source).Result()
		if backlogErr != nil {
			return scenarioResult{}, backlogErr
		}
		updateMax(&peakBacklog, backlog)
		drainStart = time.Now()
		startConsumers()
	} else {
		startConsumers()
		go func() {
			if err := runProducers(); err != nil {
				recordError(err)
			}
			close(producerDone)
		}()
	}

	select {
	case <-producerDone:
	case <-ctx.Done():
	}
	consumers.Wait()
	if current.preload {
		// The producer completed before the drain window began.
	} else {
		// Ensure producer goroutines have observed cancellation before reading final state.
		<-producerDone
	}

	errorMu.Lock()
	runErr := firstErr
	errorMu.Unlock()
	if runErr == nil && ctx.Err() != nil {
		runErr = ctx.Err()
	}
	if runErr != nil {
		return scenarioResult{}, runErr
	}
	cancel()

	remaining, err := client.LLen(context.Background(), source).Result()
	if err != nil {
		return scenarioResult{}, err
	}
	results, err := client.LLen(context.Background(), resultKey).Result()
	if err != nil {
		return scenarioResult{}, err
	}
	dead, err := client.LLen(context.Background(), deadKey).Result()
	if err != nil {
		return scenarioResult{}, err
	}
	uniqueResults, err := verifyResultIDs(context.Background(), client, resultKey)
	if err != nil {
		return scenarioResult{}, err
	}
	lost := int64(current.tasks) - uniqueResults - dead
	if lost < 0 {
		return scenarioResult{}, fmt.Errorf("terminal records exceed logical tasks: results=%d dead=%d tasks=%d", uniqueResults, dead, current.tasks)
	}
	if produced.Load() != int64(current.tasks) || processed.Load() != int64(current.tasks) || remaining != 0 || results != int64(current.tasks) || uniqueResults != int64(current.tasks) || dead != 0 || lost != 0 {
		return scenarioResult{}, fmt.Errorf("incomplete drain: produced=%d processed=%d source=%d result=%d dead=%d", produced.Load(), processed.Load(), remaining, results, dead)
	}

	end := time.Now()
	produceTime := time.Duration(producerDuration.Load())
	drainTime := end.Sub(drainStart)
	return scenarioResult{
		produced:        produced.Load(),
		processed:       processed.Load(),
		attempts:        attempts.Load(),
		retryEffects:    retryEffects.Load(),
		retryDeliveries: retryDeliveries.Load(),
		peakBacklog:     peakBacklog.Load(),
		remaining:       remaining,
		results:         uniqueResults,
		dead:            dead,
		lost:            lost,
		produceTime:     produceTime,
		drainTime:       drainTime,
		totalTime:       end.Sub(overallStart),
	}, nil
}

func taskJSON(name string, index int) string {
	return fmt.Sprintf(`{"task_id":"perf-%s-%08d","retry_count":0,"payload":{"value":%d}}`, name, index, index)
}

func cleanupKeys(ctx context.Context, client *redis.Client, namespace string, hostKeys ...string) {
	keys := append([]string{}, hostKeys...)
	prefix := "echoqueue:1:" + base64.RawURLEncoding.EncodeToString([]byte(namespace))
	var cursor uint64
	for {
		found, nextCursor, err := client.Scan(ctx, cursor, prefix+"*", 256).Result()
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
		_, _ = client.Del(ctx, keys...).Result()
	}
}

func updateMax(target *atomic.Int64, value int64) {
	for {
		old := target.Load()
		if value <= old || target.CompareAndSwap(old, value) {
			return
		}
	}
}

func printResult(current scenario, result scenarioResult) {
	produceRate := rate(result.produced, result.produceTime)
	drainRate := rate(result.processed, result.drainTime)
	totalRate := rate(result.processed, result.totalTime)
	fmt.Printf("%-14s tasks=%-6d producers=%-2d consumers=%-2d interval=%-8s peak_backlog=%-6d produce=%8.0f/s drain=%8.0f/s total=%8.0f/s elapsed=%s\n",
		current.name, result.produced, current.producers, current.consumers, current.interval, result.peakBacklog, produceRate, drainRate, totalRate, result.totalTime.Round(time.Millisecond))
	fmt.Printf("  verified: produced=%d processed=%d attempts=%d retries=%d retry_deliveries=%d retry_rate=%.2f%% source_remaining=%d result=%d dead=%d lost=%d loss_rate=%.4f%%\n",
		result.produced, result.processed, result.attempts, result.retryEffects, result.retryDeliveries, percentage(result.retryEffects, result.produced), result.remaining, result.results, result.dead, result.lost, percentage(result.lost, result.produced))
}

func (a *aggregateResult) add(result scenarioResult) {
	a.logical += result.produced
	a.attempts += result.attempts
	a.retryEffects += result.retryEffects
	a.retryDeliveries += result.retryDeliveries
	a.results += result.results
	a.dead += result.dead
	a.lost += result.lost
}

func printOverall(result aggregateResult) {
	fmt.Printf("overall: logical=%d attempts=%d retries=%d retry_deliveries=%d results=%d dead=%d lost=%d retry_rate=%.2f%% delivery_retry_rate=%.2f%% loss_rate=%.4f%%\n",
		result.logical, result.attempts, result.retryEffects, result.retryDeliveries, result.results, result.dead, result.lost,
		percentage(result.retryEffects, result.logical), percentage(result.retryDeliveries, result.attempts), percentage(result.lost, result.logical))
}

func rate(count int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func percentage(value, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(value) * 100 / float64(total)
}

func retryCandidate(task echoqueue.Task, retryPercent int) bool {
	if retryPercent <= 0 || task.RetryCount != 0 {
		return false
	}
	separator := strings.LastIndexByte(task.TaskID, '-')
	if separator < 0 {
		return false
	}
	index, err := strconv.Atoi(task.TaskID[separator+1:])
	return err == nil && index%100 < retryPercent
}

func verifyResultIDs(ctx context.Context, client *redis.Client, key string) (int64, error) {
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
			return 0, errors.New("result record is missing task_id or effect_id")
		}
		if _, exists := seen[record.TaskID]; exists {
			return 0, fmt.Errorf("duplicate result task_id %q", record.TaskID)
		}
		seen[record.TaskID] = struct{}{}
	}
	return int64(len(seen)), nil
}

func parseLevels(raw string) ([]int, error) {
	var levels []int
	for _, value := range strings.Split(raw, ",") {
		level, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || level < 1 {
			return nil, fmt.Errorf("invalid level %q", value)
		}
		levels = append(levels, level)
	}
	if len(levels) == 0 {
		return nil, errors.New("at least one pressure level is required")
	}
	return levels, nil
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

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
