//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"example.com/m/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func runUntilReceipt(t *testing.T, f fixture, batchID string) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- f.scheduler.Run(ctx)
	}()
	testutil.WaitFor(t, 2*time.Second, func() bool {
		_, err := f.rdb.Get(context.Background(), receiptKey(f.namespace, batchID)).Result()
		return err == nil
	})
	cancel()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after receipt")
		return nil
	}
}

func TestRecoverCrossesMultipleBoundedWindows(t *testing.T) {
	const runBatchSize = 2
	const badCount = 2*runBatchSize + 2
	f := newFixtureWithRunBatchSize(t, 0, 10*time.Millisecond, runBatchSize)
	ctx := context.Background()
	badIDs := make([]string, 0, badCount)
	badRaw := make(map[string]string, badCount)
	initialScores := make(map[string]float64, badCount)
	for i := 0; i < badCount; i++ {
		taskID := fmt.Sprintf("cross-window-bad-%d", i)
		seedTask(t, f, taskID)
		batch, err := f.queue.Dispatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		badIDs = append(badIDs, batch.ID)
		raw := fmt.Sprintf("{corrupt-%d", i)
		badRaw[batch.ID] = raw
		if err := f.rdb.Set(ctx, pendingKey(f.namespace, batch.ID), raw, 0).Err(); err != nil {
			t.Fatal(err)
		}
		score, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batch.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		initialScores[batch.ID] = score
	}
	seedTask(t, f, "cross-window-normal")
	normal, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	for window := 0; window < badCount/runBatchSize; window++ {
		runErr := f.scheduler.Run(ctx)
		if runErr == nil || !strings.Contains(runErr.Error(), badIDs[window*runBatchSize]) {
			t.Fatalf("window %d Run error = %v, want first bad batch %s", window, runErr, badIDs[window*runBatchSize])
		}
	}
	if _, err := f.rdb.Get(ctx, receiptKey(f.namespace, normal.ID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("normal batch recovered before its window, err=%v", err)
	}
	if runErr := runUntilReceipt(t, f, normal.ID); runErr != nil && !errors.Is(runErr, context.Canceled) && !strings.Contains(runErr.Error(), badIDs[0]) {
		t.Fatalf("final controlled Run error = %v", runErr)
	}

	for _, batchID := range badIDs {
		raw, err := f.rdb.Get(ctx, pendingKey(f.namespace, batchID)).Result()
		if err != nil || raw != badRaw[batchID] {
			t.Fatalf("bad pending %s changed: %q, err=%v", batchID, raw, err)
		}
		score, err := f.rdb.ZScore(ctx, deadlineKey(f.namespace), batchID).Result()
		if err != nil || score <= initialScores[batchID] {
			t.Fatalf("bad deadline %s was not deferred: score=%v initial=%v err=%v", batchID, score, initialScores[batchID], err)
		}
	}
}

func TestRecoverAdvancesAfterWaitingPastDeferDelay(t *testing.T) {
	f := newFixtureWithRunBatchSize(t, 0, 10*time.Millisecond, 1)
	ctx := context.Background()
	seedTask(t, f, "wait-bad")
	bad, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	seedTask(t, f, "wait-normal")
	normal, err := f.queue.Dispatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := "{wait-corrupt"
	if err := f.rdb.Set(ctx, pendingKey(f.namespace, bad.ID), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	firstErr := f.scheduler.Run(ctx)
	if firstErr == nil || !strings.Contains(firstErr.Error(), bad.ID) {
		t.Fatalf("first Run error = %v, want %s", firstErr, bad.ID)
	}
	time.Sleep(1100 * time.Millisecond)
	runErr := runUntilReceipt(t, f, normal.ID)
	if runErr == nil {
		t.Fatal("controlled Run unexpectedly returned nil")
	}
	if got, err := f.rdb.Get(ctx, pendingKey(f.namespace, bad.ID)).Result(); err != nil || got != raw {
		t.Fatalf("bad pending after waiting = %q, err=%v", got, err)
	}
}
