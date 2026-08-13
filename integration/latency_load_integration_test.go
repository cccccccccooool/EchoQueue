//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
)

func percentiles(samples []time.Duration, ps ...float64) map[float64]time.Duration {
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	result := map[float64]time.Duration{}
	for _, p := range ps {
		if len(sorted) == 0 {
			result[p] = 0
			continue
		}
		index := int(float64(len(sorted)-1) * p)
		result[p] = sorted[index]
	}
	return result
}

func TestLoadLatencyAndThroughputUnderInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("latency load test skipped in -short mode")
	}
	f := newFixture(t, 2, time.Minute)
	ctx := context.Background()
	const total = 20000
	const batchSize = 32
	for i := 0; i < total; i++ {
		raw := fmt.Sprintf(`{"task_id":"load-%s-%08d","retry_count":0,"payload":{"n":%d}}`, f.namespace[7:12], i, i)
		if err := f.rdb.RPush(ctx, f.source, raw).Err(); err != nil {
			t.Fatalf("inject: %v", err)
		}
	}

	workers := 8
	dispatchLat := make([][]time.Duration, workers)
	settleLat := make([][]time.Duration, workers)
	var wg sync.WaitGroup
	errorsCh := make(chan error, workers)
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				dispatchStart := time.Now()
				batch, err := f.queue.Dispatch(ctx, batchSize)
				dispatchLat[w] = append(dispatchLat[w], time.Since(dispatchStart))
				if err != nil {
					errorsCh <- err
					return
				}
				if batch.ID == "" {
					return
				}
				outcome := echoqueue.Outcome{RequestID: "load-" + batch.ID}
				for _, task := range batch.Tasks {
					outcome.Results = append(outcome.Results, echoqueue.Result{TaskID: task.TaskID, Data: []byte(`{"ok":true}`)})
				}
				settleStart := time.Now()
				receipt, err := f.scheduler.Settle(ctx, batch.ID, outcome)
				settleLat[w] = append(settleLat[w], time.Since(settleStart))
				if err != nil {
					errorsCh <- err
					return
				}
				if receipt.Status != echoqueue.ReceiptApplied {
					errorsCh <- fmt.Errorf("unexpected status %q", receipt.Status)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errorsCh)
	elapsed := time.Since(start)
	for err := range errorsCh {
		t.Fatalf("load error: %v", err)
	}

	var allDispatch, allSettle []time.Duration
	for w := 0; w < workers; w++ {
		allDispatch = append(allDispatch, dispatchLat[w]...)
		allSettle = append(allSettle, settleLat[w]...)
	}
	d := percentiles(allDispatch, 0.5, 0.95, 0.99)
	s := percentiles(allSettle, 0.5, 0.95, 0.99)
	t.Logf("load=%d workers=%d batch=%d elapsed=%s throughput=%.0f batches/s",
		total, workers, batchSize, elapsed.Round(time.Millisecond), float64(len(allDispatch))/elapsed.Seconds())
	t.Logf("dispatch latency p50=%s p95=%s p99=%s", d[0.5], d[0.95], d[0.99])
	t.Logf("settle latency  p50=%s p95=%s p99=%s", s[0.5], s[0.95], s[0.99])

	records, err := f.rdb.LRange(ctx, f.result, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	unique := map[string]bool{}
	for _, raw := range records {
		var record struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if unique[record.TaskID] {
			t.Fatalf("duplicate result %q", record.TaskID)
		}
		unique[record.TaskID] = true
	}
	deadLen, _ := f.rdb.LLen(ctx, f.dead).Result()
	sourceLeft, _ := f.rdb.LLen(ctx, f.source).Result()
	if len(unique) != total {
		t.Fatalf("unique results = %d, want %d (loss=%d)", len(unique), total, total-len(unique))
	}
	if sourceLeft != 0 || deadLen != 0 {
		t.Fatalf("leftovers source=%d dead=%d", sourceLeft, deadLen)
	}
	t.Logf("loss=0 dead=%d source=%d unique=%d", deadLen, sourceLeft, len(unique))
}
