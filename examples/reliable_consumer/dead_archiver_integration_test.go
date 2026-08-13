//go:build integration

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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

type sinkCall struct {
	records []DeadRecord
	err     error
	block   <-chan struct{}
}

type recordingSink struct {
	mu        sync.Mutex
	calls     []sinkCall
	persisted map[string]int
	failures  int
	block     <-chan struct{}
}

func newRecordingSink() *recordingSink {
	return &recordingSink{persisted: map[string]int{}}
}

func (s *recordingSink) PersistDead(ctx context.Context, records []DeadRecord) error {
	s.mu.Lock()
	if len(records) > 0 {
		for _, record := range records {
			s.persisted[record.EffectID]++
		}
	}
	s.calls = append(s.calls, sinkCall{records: append([]DeadRecord(nil), records...)})
	blocked := s.block
	failures := s.failures
	s.mu.Unlock()
	if blocked != nil {
		<-blocked
	}
	if failures > 0 && len(records) > 0 {
		s.mu.Lock()
		s.failures--
		s.mu.Unlock()
		return errors.New("sink failure injected")
	}
	return nil
}

func (s *recordingSink) uniquePersisted() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := map[string]bool{}
	for effectID := range s.persisted {
		result[effectID] = true
	}
	return result
}

func (s *recordingSink) persistCount(effectID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persisted[effectID]
}

func newArchiverFixture(t *testing.T, batchSize int) (*redis.Client, string, string, *DeadArchiver, *recordingSink) {
	t.Helper()
	rdb := testutil.MustRedis(t)
	dead := "eq-arch-dead-" + uuid.NewString()
	processing := "eq-arch-processing-" + uuid.NewString()
	sink := newRecordingSink()
	archiver, err := NewDeadArchiver(rdb, ArchiverConfig{
		DeadKey:       dead,
		ProcessingKey: processing,
		BatchSize:     batchSize,
		FlushInterval: 30 * time.Millisecond,
		ClaimTimeout:  50 * time.Millisecond,
		ErrorBackoff:  10 * time.Millisecond,
	}, sink)
	if err != nil {
		t.Fatalf("NewDeadArchiver: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = rdb.Del(ctx, dead, processing).Result()
		_ = rdb.Close()
	})
	return rdb, dead, processing, archiver, sink
}

func deadRecordJSON(effectID string) string {
	return `{"schema_version":1,"protocol_version":1,"effect_id":"` + effectID + `","task_id":"task","batch_id":"batch","retry_count":3,"reason":"fail","payload":{"x":1}}`
}

func pushDead(t *testing.T, rdb *redis.Client, dead string, effectIDs ...string) {
	t.Helper()
	ctx := context.Background()
	for _, effectID := range effectIDs {
		if err := rdb.RPush(ctx, dead, deadRecordJSON(effectID)).Err(); err != nil {
			t.Fatalf("push dead: %v", err)
		}
	}
}

func runArchiverUntil(t *testing.T, archiver *DeadArchiver, report ErrorSink, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- archiver.Run(ctx, report) }()
	testutil.WaitFor(t, 10*time.Second, condition)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("archiver did not stop")
	}
}

func TestDeadArchiverClaimsPersistsAndAcks(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	pushDead(t, rdb, dead, "effect-1", "effect-2", "effect-3")
	runArchiverUntil(t, archiver, func(error) {}, func() bool {
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		processingLen, _ := rdb.LLen(context.Background(), processing).Result()
		return deadLen == 0 && processingLen == 0
	})
	unique := sink.uniquePersisted()
	if len(unique) != 3 {
		t.Fatalf("unique persisted = %d, want 3", len(unique))
	}
	for _, effectID := range []string{"effect-1", "effect-2", "effect-3"} {
		if !unique[effectID] {
			t.Fatalf("effect_id %q was removed from Redis without a persist record", effectID)
		}
	}
}

func TestDeadArchiverDoesNotAckWhenPersistFails(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	sink.failures = 100
	pushDead(t, rdb, dead, "effect-fail")
	var reported atomic.Int64
	runArchiverUntil(t, archiver, func(error) { reported.Add(1) }, func() bool { return reported.Load() >= 1 })
	deadLen, _ := rdb.LLen(context.Background(), dead).Result()
	processingLen, _ := rdb.LLen(context.Background(), processing).Result()
	if deadLen+processingLen != 1 {
		t.Fatalf("record vanished after failed persist: dead=%d processing=%d", deadLen, processingLen)
	}
	if sink.persistCount("effect-fail") == 0 {
		t.Fatal("sink never saw the record")
	}
}

func TestDeadArchiverAckFailureIsReportedAndRecordKept(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	pushDead(t, rdb, dead, "effect-ackfail")
	// Block the sink right after persist so the test can break the
	// Processing list type before the ACK (LREM) runs.
	block := make(chan struct{})
	sink.mu.Lock()
	sink.block = block
	sink.mu.Unlock()
	var reportedMu sync.Mutex
	var reportedErrors []string
	var reported atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- archiver.Run(ctx, func(err error) {
			reported.Add(1)
			reportedMu.Lock()
			reportedErrors = append(reportedErrors, err.Error())
			reportedMu.Unlock()
		})
	}()
	testutil.WaitFor(t, 10*time.Second, func() bool { return sink.persistCount("effect-ackfail") >= 1 })
	// Persist succeeded and the ACK is blocked: break the list type now.
	if err := rdb.Del(context.Background(), processing).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), processing, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	close(block)
	// At least one ack failure must be reported before the loop starts
	// reporting claim failures on the broken key.
	var sawAck atomic.Bool
	testutil.WaitFor(t, 10*time.Second, func() bool {
		reportedMu.Lock()
		defer reportedMu.Unlock()
		for _, text := range reportedErrors {
			if strings.Contains(text, "ack") {
				sawAck.Store(true)
				return true
			}
		}
		return false
	})
	if !sawAck.Load() {
		t.Fatalf("no ack failure reported; errors = %v", reportedErrors)
	}
	if sink.persistCount("effect-ackfail") == 0 {
		t.Fatal("record was never persisted before the ack failure")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("archiver did not stop")
	}
}

func TestDeadArchiverReplaysProcessingAfterRestart(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	ctx := context.Background()
	// Simulate a previous crash: records are already in DeadProcessing.
	if err := rdb.RPush(ctx, processing, deadRecordJSON("leftover-1"), deadRecordJSON("leftover-2")).Err(); err != nil {
		t.Fatalf("seed processing: %v", err)
	}
	runArchiverUntil(t, archiver, func(error) {}, func() bool {
		processingLen, _ := rdb.LLen(ctx, processing).Result()
		return processingLen == 0
	})
	unique := sink.uniquePersisted()
	if len(unique) != 2 || !unique["leftover-1"] || !unique["leftover-2"] {
		t.Fatalf("replayed persists = %v", unique)
	}
	deadLen, _ := rdb.LLen(ctx, dead).Result()
	if deadLen != 0 {
		t.Fatalf("dead length = %d", deadLen)
	}
}

func TestDeadArchiverDuplicatePersistStaysIdempotentAfterCrashWindow(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	pushDead(t, rdb, dead, "effect-dupe")
	// Block the sink right after persist so the run is cancelled exactly in
	// the window between persist success and the ACK (LREM).
	block := make(chan struct{})
	sink.mu.Lock()
	sink.block = block
	sink.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- archiver.Run(ctx, func(error) {}) }()
	testutil.WaitFor(t, 10*time.Second, func() bool { return sink.persistCount("effect-dupe") >= 1 })
	cancel()
	close(block)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("first run did not stop")
	}
	processingLen, _ := rdb.LLen(context.Background(), processing).Result()
	if processingLen == 0 {
		t.Fatal("record was acked while the run was being cancelled")
	}
	// Restart: the record is replayed and persisted again, but the external
	// sink deduplicates by effect_id.
	runArchiverUntil(t, archiver, func(error) {}, func() bool {
		processingLen, _ := rdb.LLen(context.Background(), processing).Result()
		return processingLen == 0
	})
	if count := sink.persistCount("effect-dupe"); count < 2 {
		t.Fatalf("idempotent duplicate persist count = %d, want >= 2", count)
	}
	if unique := sink.uniquePersisted(); len(unique) != 1 {
		t.Fatalf("unique external records = %d, want 1", len(unique))
	}
}

func TestDeadArchiverConcurrentRPUSHAllArchived(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- archiver.Run(ctx, func(error) {}) }()
	const total = 30
	var produced atomic.Int64
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; i < total; i++ {
			effectID := "concurrent-" + uuid.NewString()
			if err := rdb.RPush(context.Background(), dead, deadRecordJSON(effectID)).Err(); err != nil {
				t.Errorf("push: %v", err)
				return
			}
			produced.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	testutil.WaitFor(t, 10*time.Second, func() bool {
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		processingLen, _ := rdb.LLen(context.Background(), processing).Result()
		return produced.Load() == total && deadLen == 0 && processingLen == 0
	})
	<-producerDone
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("archiver did not stop")
	}
	unique := sink.uniquePersisted()
	if len(unique) != total {
		t.Fatalf("unique persisted = %d, want %d", len(unique), total)
	}
}

func TestDeadArchiverBufferSaturationStopsClaiming(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	block := make(chan struct{})
	sink.mu.Lock()
	sink.block = block
	sink.mu.Unlock()
	for i := 0; i < 10; i++ {
		pushDead(t, rdb, dead, "sat-"+uuid.NewString())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- archiver.Run(ctx, func(error) {}) }()
	// Wait until at least one persist is blocked, then confirm the claimed
	// records stay bounded at the batch capacity plus one in-flight claim.
	testutil.WaitFor(t, 10*time.Second, func() bool { return sink.persistCount("") > 0 || len(sink.uniquePersisted()) > 0 })
	time.Sleep(200 * time.Millisecond)
	processingLen, _ := rdb.LLen(context.Background(), processing).Result()
	deadLen, _ := rdb.LLen(context.Background(), dead).Result()
	if processingLen > 3 {
		t.Fatalf("claimed records = %d exceed batch capacity 2 plus one in-flight", processingLen)
	}
	if deadLen == 0 {
		t.Fatal("all records were claimed while the sink was blocked")
	}
	close(block)
	testutil.WaitFor(t, 10*time.Second, func() bool {
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		processingLen, _ := rdb.LLen(context.Background(), processing).Result()
		return deadLen == 0 && processingLen == 0
	})
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("archiver did not stop")
	}
}

func TestDeadArchiverCancelBeforeClaimLeavesDeadUntouched(t *testing.T) {
	rdb, dead, processing, archiver, _ := newArchiverFixture(t, 2)
	pushDead(t, rdb, dead, "effect-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := archiver.Run(ctx, func(error) {})
	// The cancelled replay may surface context.Canceled; the guarantee under
	// test is that no record is claimed, persisted, or acked.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	deadLen, _ := rdb.LLen(context.Background(), dead).Result()
	processingLen, _ := rdb.LLen(context.Background(), processing).Result()
	if deadLen != 1 || processingLen != 0 {
		t.Fatalf("cancelled archiver touched lists: dead=%d processing=%d", deadLen, processingLen)
	}
}

func TestDeadArchiverRejectsCorruptRecordsAndKeepsEvidence(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	ctx := context.Background()
	_ = rdb.RPush(ctx, dead, "not-json", `{"effect_id":42}`, `{"other":"no-id"}`, deadRecordJSON("good-1"))
	var rejected atomic.Int64
	runArchiverUntil(t, archiver, func(err error) {
		if strings.Contains(err.Error(), "rejected") {
			rejected.Add(1)
		}
	}, func() bool {
		processingLen, _ := rdb.LLen(ctx, processing).Result()
		deadLen, _ := rdb.LLen(ctx, dead).Result()
		return rejected.Load() >= 2 && processingLen == 2 && deadLen == 2
	})
	processingLen, _ := rdb.LLen(ctx, processing).Result()
	if processingLen != 2 {
		t.Fatalf("corrupt records not preserved in processing: %d", processingLen)
	}
	// After BatchSize consecutive corrupt claims, claiming pauses; the
	// remaining corrupt record and the valid record stay in Dead, never
	// silently dropped.
	deadLen, _ := rdb.LLen(ctx, dead).Result()
	if deadLen != 2 {
		t.Fatalf("dead leftover = %d, want 2 (bounded pause keeps evidence)", deadLen)
	}
	if !sink.uniquePersisted()["good-1"] {
		t.Log("good-1 not yet archived: the corrupt head pauses claiming (bounded pause semantics); the record is preserved in Dead")
	}
	raws, _ := rdb.LRange(ctx, dead, 0, -1).Result()
	preserved := false
	for _, raw := range raws {
		var envelope struct {
			EffectID string `json:"effect_id"`
		}
		if err := json.Unmarshal([]byte(raw), &envelope); err == nil && envelope.EffectID == "good-1" {
			preserved = true
		}
	}
	if !preserved {
		t.Fatal("valid record was lost during the corrupt-head pause")
	}
}

func TestDeadArchiverCountingInvariant(t *testing.T) {
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 2)
	// Produce terminal records through the real Settle/Recover path.
	namespace := "eq-arch-count-" + uuid.NewString()
	source := "eq-arch-count-source-" + uuid.NewString()
	result := "eq-arch-count-result-" + uuid.NewString()
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
		TaskName: "counting",
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
		keys = append(keys, source, result)
		_, _ = rdb.Del(ctx, keys...).Result()
	})
	const taskCount = 6
	for i := 0; i < taskCount; i++ {
		raw := `{"task_id":"count-` + uuid.NewString() + `","retry_count":0,"payload":{"x":1}}`
		if err := rdb.RPush(context.Background(), source, raw).Err(); err != nil {
			t.Fatalf("seed source: %v", err)
		}
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	recoverDone := make(chan error, 1)
	go func() { recoverDone <- scheduler.Run(runCtx) }()
	// Drain the source through Dispatch + Settle failure so every task lands
	// in Dead (MaxRetry=0 means a failed task becomes a dead record).
	for {
		batch, dispatchErr := queue.Dispatch(context.Background(), 1)
		if dispatchErr != nil {
			t.Fatalf("Dispatch: %v", dispatchErr)
		}
		if batch.ID == "" {
			break
		}
		outcome := echoqueue.Outcome{RequestID: "count-worker", Failures: []echoqueue.Failure{}}
		for _, task := range batch.Tasks {
			outcome.Failures = append(outcome.Failures, echoqueue.Failure{TaskID: task.TaskID, Reason: "counted failure", Retryable: true})
		}
		if _, settleErr := scheduler.Settle(context.Background(), batch.ID, outcome); settleErr != nil {
			t.Fatalf("Settle: %v", settleErr)
		}
	}
	testutil.WaitFor(t, 10*time.Second, func() bool {
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		return deadLen == taskCount
	})
	runCancel()
	select {
	case <-recoverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not stop")
	}
	// Run the archiver and verify the deduplicated invariant:
	// produced dead == dead unclaimed + processing unacked + external unique.
	archiverCtx, archiverCancel := context.WithCancel(context.Background())
	defer archiverCancel()
	archiverDone := make(chan error, 1)
	go func() { archiverDone <- archiver.Run(archiverCtx, func(error) {}) }()
	testutil.WaitFor(t, 10*time.Second, func() bool {
		deadLen, _ := rdb.LLen(context.Background(), dead).Result()
		processingLen, _ := rdb.LLen(context.Background(), processing).Result()
		return deadLen == 0 && processingLen == 0
	})
	archiverCancel()
	select {
	case <-archiverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("archiver did not stop")
	}
	unique := sink.uniquePersisted()
	if len(unique) != taskCount {
		t.Fatalf("external unique dead = %d, want %d (deduplicated invariant violated)", len(unique), taskCount)
	}
}

func TestDeadArchiverCorruptFloodBounded(t *testing.T) {
	// A flood of corrupt records must not grow DeadProcessing without bound:
	// after BatchSize consecutive unparseable claims, claiming pauses.
	rdb, dead, processing, archiver, _ := newArchiverFixture(t, 2)
	ctx := context.Background()
	const corrupt = 10
	for i := 0; i < corrupt; i++ {
		if err := rdb.RPush(ctx, dead, "corrupt-"+uuid.NewString()).Err(); err != nil {
			t.Fatalf("push corrupt: %v", err)
		}
	}
	var rejected atomic.Int64
	runArchiverUntil(t, archiver, func(err error) {
		if strings.Contains(err.Error(), "rejected") {
			rejected.Add(1)
		}
	}, func() bool { return rejected.Load() >= 2 })
	// Give the loop a moment to over-claim if it were going to.
	time.Sleep(300 * time.Millisecond)
	processingLen, _ := rdb.LLen(ctx, processing).Result()
	if processingLen > 2 {
		t.Fatalf("processing grew to %d records for a corrupt flood, want bounded by batch size 2", processingLen)
	}
	deadLen, _ := rdb.LLen(ctx, dead).Result()
	if deadLen == 0 {
		t.Fatal("all corrupt records were claimed into processing")
	}
}

func TestDeadArchiverReplayLargeProcessingBounded(t *testing.T) {
	// A large leftover DeadProcessing must be drained through fixed windows
	// and finish with correct accounting, including mixed corrupt records.
	rdb, dead, processing, archiver, sink := newArchiverFixture(t, 4)
	ctx := context.Background()
	const valid = 120
	for i := 0; i < valid; i++ {
		if err := rdb.RPush(ctx, processing, deadRecordJSON("replay-"+uuid.NewString())).Err(); err != nil {
			t.Fatalf("seed processing: %v", err)
		}
		if i%10 == 0 {
			if err := rdb.RPush(ctx, processing, "corrupt-window").Err(); err != nil {
				t.Fatalf("seed corrupt: %v", err)
			}
		}
	}
	runArchiverUntil(t, archiver, func(error) {}, func() bool {
		processingLen, _ := rdb.LLen(ctx, processing).Result()
		return processingLen == 12
	})
	if unique := sink.uniquePersisted(); len(unique) != valid {
		t.Fatalf("unique persisted = %d, want %d", len(unique), valid)
	}
	processingLen, _ := rdb.LLen(ctx, processing).Result()
	if processingLen != 12 {
		t.Fatalf("processing leftover = %d, want 12 corrupt records", processingLen)
	}
	deadLen, _ := rdb.LLen(ctx, dead).Result()
	if deadLen != 0 {
		t.Fatalf("dead = %d, want 0", deadLen)
	}
}

func TestDeadArchiverLoggingSinkNeverAcks(t *testing.T) {
	// The reference logging sink returns an error, so the archiver claims
	// records but never acknowledges (deletes) them.
	rdb := testutil.MustRedis(t)
	dead := "eq-arch-log-dead-" + uuid.NewString()
	processing := "eq-arch-log-processing-" + uuid.NewString()
	archiver, err := NewDeadArchiver(rdb, ArchiverConfig{
		DeadKey:       dead,
		ProcessingKey: processing,
		BatchSize:     2,
		FlushInterval: 30 * time.Millisecond,
		ClaimTimeout:  50 * time.Millisecond,
		ErrorBackoff:  10 * time.Millisecond,
	}, LoggingDeadSink{})
	if err != nil {
		t.Fatalf("NewDeadArchiver: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = rdb.Del(ctx, dead, processing).Result()
		_ = rdb.Close()
	})
	ctx := context.Background()
	for _, effectID := range []string{"log-1", "log-2", "log-3"} {
		if err := rdb.RPush(ctx, dead, deadRecordJSON(effectID)).Err(); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	var reported atomic.Int64
	runArchiverUntil(t, archiver, func(error) { reported.Add(1) }, func() bool {
		processingLen, _ := rdb.LLen(ctx, processing).Result()
		return processingLen >= 2 && reported.Load() >= 1
	})
	time.Sleep(200 * time.Millisecond)
	// The flush fails on the non-persisting sink, so claimed records stay in
	// Processing and never more than BatchSize are claimed; the rest stays in
	// Dead. Nothing is ever acknowledged.
	processingLen, _ := rdb.LLen(ctx, processing).Result()
	if processingLen != 2 {
		t.Fatalf("processing = %d, want 2 (bounded by batch size; nothing acknowledged)", processingLen)
	}
	deadLen, _ := rdb.LLen(ctx, dead).Result()
	if deadLen != 1 {
		t.Fatalf("dead = %d, want 1 (third record never claimed while flush fails)", deadLen)
	}
}

func TestDeadArchiverDuplicateRunRejected(t *testing.T) {
	_, _, _, archiver, _ := newArchiverFixture(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- archiver.Run(ctx, func(error) {}) }()
	time.Sleep(100 * time.Millisecond)
	if err := archiver.Run(context.Background(), func(error) {}); !errors.Is(err, errArchiverAlreadyActive) {
		t.Fatalf("duplicate Run error = %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("archiver did not stop")
	}
}
