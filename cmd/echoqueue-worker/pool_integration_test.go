//go:build integration

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/cccccccccooool/EchoQueue/internal/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func newBlackBoxPool(t *testing.T) (*redis.Client, *echoqueue.Scheduler, *echoqueue.Queue, string, string, string, string) {
	t.Helper()
	rdb := testutil.MustRedis(t)
	namespace := "eq-pool-" + uuid.NewString()
	source := "eq-pool-source-" + uuid.NewString()
	result := "eq-pool-result-" + uuid.NewString()
	dead := "eq-pool-dead-" + uuid.NewString()
	cfg := echoqueue.Config{
		Namespace:         namespace,
		VisibilityTimeout: 300 * time.Millisecond,
		ReceiptTTL:        time.Hour,
		MaxRetry:          0,
		MaxRetrySet:       true,
		RunInterval:       5 * time.Millisecond,
		RunBatchSize:      16,
	}
	scheduler, err := echoqueue.New(rdb, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	queue, err := scheduler.Bind(echoqueue.QueueConfig{
		TaskName: "pool-task",
		Source:   source,
		Result:   result,
		Dead:     dead,
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		keys, _ := rdb.Keys(ctx, "echoqueue:1:"+base64.RawURLEncoding.EncodeToString([]byte(namespace))+"*").Result()
		keys = append(keys, source, result, dead)
		_, _ = rdb.Del(ctx, keys...).Result()
		_ = rdb.Close()
	})
	return rdb, scheduler, queue, source, result, dead, namespace
}

func pushTasks(t *testing.T, rdb *redis.Client, source string, count int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		raw := `{"task_id":"pool-task-` + uuid.NewString() + `","retry_count":0,"payload":{"n":` + strconv.Itoa(i) + `}}`
		if err := rdb.RPush(ctx, source, raw).Err(); err != nil {
			t.Fatalf("push task: %v", err)
		}
	}
}

func countUniqueResultIDs(t *testing.T, rdb *redis.Client, key string) int {
	t.Helper()
	records, err := rdb.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange result: %v", err)
	}
	seen := map[string]struct{}{}
	for _, raw := range records {
		var record struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatalf("decode result record: %v", err)
		}
		seen[record.TaskID] = struct{}{}
	}
	return len(seen)
}

func TestPoolGenerationGapNeverDoubleSettles(t *testing.T) {
	// A handler that ignores context cancellation outlives its Run. When the
	// host restarts immediately, the old generation must never settle a batch
	// (the generation guard leaves it to Recover), so no batch is ever
	// settled twice across generations.
	rdb, scheduler, queue, source, result, dead, _ := newBlackBoxPool(t)
	pushTasks(t, rdb, source, 4)
	hold := make(chan struct{})
	var handledMu sync.Mutex
	var handledBatchIDs []string
	ignoreCancel := func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
		handledMu.Lock()
		handledBatchIDs = append(handledBatchIDs, batch.ID)
		handledMu.Unlock()
		<-hold
		outcome := echoqueue.Outcome{RequestID: "ignoring-worker-" + batch.ID}
		for _, task := range batch.Tasks {
			outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
		}
		return outcome
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	pool, err := NewPool(PoolConfig{
		Workers:       1,
		MaxInFlight:   1,
		Buffer:        1,
		BatchSize:     1,
		PollInterval:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	var settleMu sync.Mutex
	var settleBatchIDs []string
	settle := &countingSettler{scheduler: scheduler, onCall: func(batchID string) {
		settleMu.Lock()
		settleBatchIDs = append(settleBatchIDs, batchID)
		settleMu.Unlock()
	}}
	run1Done := make(chan error, 1)
	go func() { run1Done <- pool.Run(ctx1, queue, settle, ignoreCancel, func(error) {}) }()
	time.Sleep(300 * time.Millisecond)
	handledMu.Lock()
	firstGenBatch := ""
	if len(handledBatchIDs) > 0 {
		firstGenBatch = handledBatchIDs[0]
	}
	handledMu.Unlock()
	cancel1()
	select {
	case err := <-run1Done:
		if err != nil {
			t.Fatalf("Run1 = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run1 did not stop")
	}
	// The ignoring handler still owns its batch; the host restarts at once.
	ctx2, cancel2 := context.WithCancel(context.Background())
	run2Done := make(chan error, 1)
	go func() { run2Done <- pool.Run(ctx2, queue, settle, ignoreCancel, func(error) {}) }()
	time.Sleep(300 * time.Millisecond)
	// Release the old generation's handler after the new generation is live.
	close(hold)
	// Let the old worker finish; then stop the new generation.
	time.Sleep(200 * time.Millisecond)
	cancel2()
	select {
	case err := <-run2Done:
		if err != nil {
			t.Fatalf("Run2 = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run2 did not stop")
	}
	settleMu.Lock()
	uniqueBatchIDs := map[string]bool{}
	duplicated := ""
	for _, batchID := range settleBatchIDs {
		if uniqueBatchIDs[batchID] {
			duplicated = batchID
			break
		}
		uniqueBatchIDs[batchID] = true
	}
	oldGenSettled := false
	if firstGenBatch != "" {
		for _, batchID := range settleBatchIDs {
			if batchID == firstGenBatch {
				oldGenSettled = true
				break
			}
		}
	}
	settleMu.Unlock()
	if duplicated != "" {
		t.Fatalf("batch %q was settled twice", duplicated)
	}
	if oldGenSettled {
		t.Fatalf("the first generation settled batch %q; the generation guard must leave it to Recover", firstGenBatch)
	}
	// Whatever the pool left un-settled is now owned by Pending/Recover; the
	// recovery loop must close every dispatched batch exactly once.
	recoverCtx, recoverCancel := context.WithCancel(context.Background())
	defer recoverCancel()
	recoverDone := make(chan error, 1)
	go func() { recoverDone <- scheduler.Run(recoverCtx) }()
	testutil.WaitFor(t, 15*time.Second, func() bool {
		resultLen, _ := rdb.LLen(context.Background(), result).Result()
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		sourceLeft, _ := rdb.LLen(context.Background(), source).Result()
		return sourceLeft == 0 && resultLen+deadLen == 4
	})
	recoverCancel()
	select {
	case err := <-recoverDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery loop did not stop")
	}
}

type countingSettler struct {
	scheduler *echoqueue.Scheduler
	onCall    func(batchID string)
}

func (c *countingSettler) Settle(ctx context.Context, batchID string, outcome echoqueue.Outcome) (echoqueue.Receipt, error) {
	if c.onCall != nil {
		c.onCall(batchID)
	}
	return c.scheduler.Settle(ctx, batchID, outcome)
}

func TestPoolBlackBoxDrainsSourceToResult(t *testing.T) {
	rdb, scheduler, queue, source, result, dead, _ := newBlackBoxPool(t)
	const taskCount = 40
	pushTasks(t, rdb, source, taskCount)

	ctx, cancel := context.WithCancel(context.Background())
	pool, err := NewPool(PoolConfig{
		Workers:       4,
		MaxInFlight:   6,
		Buffer:        8,
		BatchSize:     4,
		PollInterval:  time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	var settleErrors atomic.Int64
	runDone := make(chan error, 1)
	go func() {
		runDone <- pool.Run(ctx, queue, scheduler, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
			outcome := echoqueue.Outcome{RequestID: "pool-worker-" + batch.ID}
			for _, task := range batch.Tasks {
				outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
			}
			return outcome
		}, func(err error) {
			if strings.Contains(err.Error(), "settle") {
				settleErrors.Add(1)
			}
		})
	}()
	testutil.WaitFor(t, 10*time.Second, func() bool {
		remaining, _ := rdb.LLen(context.Background(), source).Result()
		settled, _ := rdb.LLen(context.Background(), result).Result()
		return remaining == 0 && settled == taskCount
	})
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	if unique := countUniqueResultIDs(t, rdb, result); unique != taskCount {
		t.Fatalf("unique results = %d, want %d", unique, taskCount)
	}
	deadLen, _ := rdb.LLen(context.Background(), dead).Result()
	if deadLen != 0 {
		t.Fatalf("dead length = %d, want 0", deadLen)
	}
	if settleErrors.Load() != 0 {
		t.Fatalf("settle errors = %d", settleErrors.Load())
	}
}

func TestPoolBlackBoxSaturationKeepsSource(t *testing.T) {
	rdb, scheduler, queue, source, result, _, _ := newBlackBoxPool(t)
	const taskCount = 30
	pushTasks(t, rdb, source, taskCount)

	ctx, cancel := context.WithCancel(context.Background())
	pool, err := NewPool(PoolConfig{
		Workers:       1,
		MaxInFlight:   2,
		Buffer:        2,
		BatchSize:     2,
		PollInterval:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	hold := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- pool.Run(ctx, queue, scheduler, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
			<-hold
			outcome := echoqueue.Outcome{RequestID: "pool-worker-" + batch.ID}
			for _, task := range batch.Tasks {
				outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
			}
			return outcome
		}, func(error) {})
	}()
	time.Sleep(300 * time.Millisecond)
	remaining, _ := rdb.LLen(context.Background(), source).Result()
	if remaining == 0 {
		t.Fatal("source drained completely while the pool was saturated")
	}
	if remaining >= taskCount {
		t.Fatalf("source still holds %d tasks; the pool never dispatched while saturated", remaining)
	}
	close(hold)
	// Wait until every task has been settled (result list reaches the final
	// count) before cancelling, so no in-flight batch is abandoned to Recover
	// without a recovery loop in this test.
	testutil.WaitFor(t, 10*time.Second, func() bool {
		left, _ := rdb.LLen(context.Background(), source).Result()
		settled, _ := rdb.LLen(context.Background(), result).Result()
		return left == 0 && settled == taskCount
	})
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	if unique := countUniqueResultIDs(t, rdb, result); unique != taskCount {
		t.Fatalf("unique results = %d, want %d", unique, taskCount)
	}
}

func TestPoolBlackBoxCancelAfterDispatchIsRecovered(t *testing.T) {
	rdb, scheduler, queue, source, result, dead, namespace := newBlackBoxPool(t)
	pushTasks(t, rdb, source, 4)

	ctx, cancel := context.WithCancel(context.Background())
	pool, err := NewPool(PoolConfig{
		Workers:       1,
		MaxInFlight:   2,
		Buffer:        2,
		BatchSize:     1,
		PollInterval:  time.Millisecond,
		ShutdownGrace: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	hold := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- pool.Run(ctx, queue, scheduler, func(ctx context.Context, batch echoqueue.Batch) echoqueue.Outcome {
			<-hold
			outcome := echoqueue.Outcome{RequestID: "pool-worker-" + batch.ID}
			for _, task := range batch.Tasks {
				outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
			}
			return outcome
		}, func(error) {})
	}()
	time.Sleep(300 * time.Millisecond)
	dispatched, _ := rdb.LLen(context.Background(), result).Result()
	_ = dispatched
	// Cancel while batches are dispatched and handlers are blocked.
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
	close(hold)

	// The recovery loop must close every batch the pool dispatched before
	// cancellation (max_in_flight=2 -> 2 batches), writing each task to Dead
	// (MaxRetry=0). Tasks never dispatched stay in Source for a later run.
	recoverCtx, recoverCancel := context.WithCancel(context.Background())
	defer recoverCancel()
	recoverDone := make(chan error, 1)
	go func() { recoverDone <- scheduler.Run(recoverCtx) }()
	ok := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		pendingLeft, _ := rdb.Keys(context.Background(), "echoqueue:1:"+base64.RawURLEncoding.EncodeToString([]byte(namespace))+":pending:*").Result()
		if deadLen == 2 && len(pendingLeft) == 0 {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		sourceLeft, _ := rdb.LLen(context.Background(), source).Result()
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		t.Fatalf("recovery did not close the dispatched batches: source=%d dead=%d", sourceLeft, deadLen)
	}
	recoverCancel()
	select {
	case err := <-recoverDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery loop did not stop")
	}
	sourceLeft, _ := rdb.LLen(context.Background(), source).Result()
	if sourceLeft != 2 {
		t.Fatalf("source remaining = %d, want 2 (undispatched tasks must stay for a later run)", sourceLeft)
	}
	deadLen, _ := rdb.LLen(context.Background(), dead).Result()
	if deadLen != 2 {
		t.Fatalf("dead length = %d, want 2", deadLen)
	}
	unique := countUniqueResultIDs(t, rdb, result)
	if unique != 0 {
		t.Fatalf("unique results = %d, want 0", unique)
	}
}
