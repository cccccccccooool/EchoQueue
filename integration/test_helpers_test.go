//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/cccccccccooool/EchoQueue/internal/testutil"
)

func runUntilState(t *testing.T, f fixture, condition func() bool) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- f.scheduler.Run(ctx)
	}()
	testutil.WaitFor(t, 2*time.Second, condition)
	cancel()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
		return nil
	}
}

func runExpectError(t *testing.T, f fixture) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- f.scheduler.Run(ctx)
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Run returned nil for expected error")
		}
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return expected error")
		return nil
	}
}
