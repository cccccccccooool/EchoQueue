package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	echoqueue "github.com/cccccccccooool/EchoQueue"
	"github.com/redis/go-redis/v9"
)

type verifyHook struct {
	values []string
	err    error
}

func (h *verifyHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *verifyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *verifyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "lrange" {
			return next(ctx, cmd)
		}
		if h.err != nil {
			cmd.SetErr(h.err)
			return h.err
		}
		stringSliceCmd, ok := cmd.(*redis.StringSliceCmd)
		if !ok {
			panic("unexpected lrange command type")
		}
		stringSliceCmd.SetVal(h.values)
		return nil
	}
}

func newVerifyClient(values []string, hookErr error) *redis.Client {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	client.AddHook(&verifyHook{values: values, err: hookErr})
	return client
}

func TestRate(t *testing.T) {
	tests := []struct {
		name    string
		count   int64
		elapsed time.Duration
		want    float64
	}{
		{name: "zero elapsed", count: 10, elapsed: 0, want: 0},
		{name: "negative elapsed", count: 10, elapsed: -time.Second, want: 0},
		{name: "zero count", count: 0, elapsed: time.Second, want: 0},
		{name: "normal", count: 100, elapsed: 2 * time.Second, want: 50},
		{name: "fractional", count: 1, elapsed: 250 * time.Millisecond, want: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := rate(test.count, test.elapsed); got != test.want {
				t.Errorf("rate(%d, %s) = %v, want %v", test.count, test.elapsed, got, test.want)
			}
		})
	}
}

func TestPercentage(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		total int64
		want  float64
	}{
		{name: "zero total", value: 10, total: 0, want: 0},
		{name: "negative total", value: 10, total: -5, want: 0},
		{name: "zero value", value: 0, total: 10, want: 0},
		{name: "half", value: 50, total: 100, want: 50},
		{name: "exact hundred", value: 3, total: 3, want: 100},
		{name: "fractional", value: 1, total: 3, want: 100.0 / 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := percentage(test.value, test.total); got != test.want {
				t.Errorf("percentage(%d, %d) = %v, want %v", test.value, test.total, got, test.want)
			}
		})
	}
}

func TestRetryCandidate(t *testing.T) {
	tests := []struct {
		name         string
		taskID       string
		retryCount   int
		retryPercent int
		want         bool
	}{
		{name: "zero retry percent", taskID: "perf-1", retryCount: 0, retryPercent: 0, want: false},
		{name: "negative retry percent", taskID: "perf-1", retryCount: 0, retryPercent: -1, want: false},
		{name: "already retried", taskID: "perf-1", retryCount: 1, retryPercent: 2, want: false},
		{name: "no separator", taskID: "plain", retryCount: 0, retryPercent: 2, want: false},
		{name: "non-numeric suffix", taskID: "perf-1-abc", retryCount: 0, retryPercent: 2, want: false},
		{name: "empty suffix", taskID: "perf-1-", retryCount: 0, retryPercent: 2, want: false},
		{name: "index one inside two percent", taskID: "perf-1", retryCount: 0, retryPercent: 2, want: true},
		{name: "index two outside two percent", taskID: "perf-2", retryCount: 0, retryPercent: 2, want: false},
		{name: "index one hundred resets", taskID: "perf-100", retryCount: 0, retryPercent: 2, want: true},
		{name: "index two hundred resets", taskID: "perf-200", retryCount: 0, retryPercent: 2, want: true},
		{name: "index ninety nine full percent", taskID: "perf-99", retryCount: 0, retryPercent: 100, want: true},
		{name: "index fifty outside fifty percent", taskID: "perf-50", retryCount: 0, retryPercent: 50, want: false},
		{name: "index forty nine inside fifty percent", taskID: "perf-49", retryCount: 0, retryPercent: 50, want: true},
		{name: "last dash wins", taskID: "perf-continuous-2", retryCount: 0, retryPercent: 2, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := echoqueue.Task{TaskID: test.taskID, RetryCount: test.retryCount}
			if got := retryCandidate(task, test.retryPercent); got != test.want {
				t.Errorf("retryCandidate(%+v, %d) = %v, want %v", task, test.retryPercent, got, test.want)
			}
		})
	}
}

func TestParseLevels(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tests := []struct {
			name string
			raw  string
			want []int
		}{
			{name: "single", raw: "8", want: []int{8}},
			{name: "multiple", raw: "1,4,8", want: []int{1, 4, 8}},
			{name: "whitespace and order", raw: " 1 , 4 ", want: []int{1, 4}},
			{name: "duplicate values", raw: "2,2", want: []int{2, 2}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				got, err := parseLevels(test.raw)
				if err != nil {
					t.Fatalf("parseLevels(%q) unexpected error: %v", test.raw, err)
				}
				if len(got) != len(test.want) {
					t.Fatalf("parseLevels(%q) = %v, want %v", test.raw, got, test.want)
				}
				for index := range got {
					if got[index] != test.want[index] {
						t.Errorf("parseLevels(%q) = %v, want %v", test.raw, got, test.want)
					}
				}
			})
		}
	})
	t.Run("invalid", func(t *testing.T) {
		invalid := []string{"", ",", " ", "0", "-3", "abc", "1,,2"}
		for _, raw := range invalid {
			t.Run(raw, func(t *testing.T) {
				if got, err := parseLevels(raw); err == nil {
					t.Errorf("parseLevels(%q) = %v, want error", raw, got)
				}
			})
		}
	})
}

func TestVerifyResultIDs(t *testing.T) {
	sentinel := errors.New("lrange failed")
	tests := []struct {
		name    string
		values  []string
		err     error
		want    int64
		wantErr string
	}{
		{
			name:   "unique ids",
			values: []string{`{"task_id":"t-1","effect_id":"e-1"}`, `{"task_id":"t-2","effect_id":"e-2"}`, `{"task_id":"t-3","effect_id":"e-3"}`},
			want:   3,
		},
		{
			name:   "empty list",
			values: nil,
			want:   0,
		},
		{
			name:    "duplicate task id",
			values:  []string{`{"task_id":"t-1","effect_id":"e-1"}`, `{"task_id":"t-1","effect_id":"e-2"}`},
			wantErr: "duplicate",
		},
		{
			name:    "missing task id",
			values:  []string{`{"effect_id":"e-1"}`},
			wantErr: "missing task_id or effect_id",
		},
		{
			name:    "missing effect id",
			values:  []string{`{"task_id":"t-1"}`},
			wantErr: "missing task_id or effect_id",
		},
		{
			name:    "invalid json",
			values:  []string{"not json"},
			wantErr: "decode result record",
		},
		{
			name:    "lrange error",
			values:  []string{`{"task_id":"t-1","effect_id":"e-1"}`},
			err:     sentinel,
			wantErr: "lrange failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newVerifyClient(test.values, test.err)
			defer client.Close()
			got, err := verifyResultIDs(context.Background(), client, "key")
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("verifyResultIDs() = %d, want error containing %q", got, test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("verifyResultIDs() error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("verifyResultIDs() unexpected error: %v", err)
			}
			if got != test.want {
				t.Errorf("verifyResultIDs() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestUpdateMax(t *testing.T) {
	t.Run("sequential", func(t *testing.T) {
		var target atomic.Int64
		updateMax(&target, 5)
		if target.Load() != 5 {
			t.Errorf("target = %d, want 5", target.Load())
		}
		updateMax(&target, 3)
		if target.Load() != 5 {
			t.Errorf("target = %d, want 5", target.Load())
		}
		updateMax(&target, 10)
		if target.Load() != 10 {
			t.Errorf("target = %d, want 10", target.Load())
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		values := []int64{-5, 3, -1, 0, 2, 7, -3, 4, 9, 1, 8, -2}
		var target atomic.Int64
		var wg sync.WaitGroup
		for _, value := range values {
			wg.Add(1)
			go func(value int64) {
				defer wg.Done()
				updateMax(&target, value)
			}(value)
		}
		wg.Wait()
		want := int64(9)
		if target.Load() != want {
			t.Errorf("target = %d, want max %d", target.Load(), want)
		}
	})
}

func TestAggregateAdd(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		var aggregate aggregateResult
		aggregate.add(scenarioResult{
			produced:        10,
			attempts:        12,
			retryEffects:    2,
			retryDeliveries: 3,
			results:         10,
			dead:            1,
			lost:            1,
		})
		if aggregate.logical != 10 || aggregate.attempts != 12 || aggregate.retryEffects != 2 ||
			aggregate.retryDeliveries != 3 || aggregate.results != 10 || aggregate.dead != 1 || aggregate.lost != 1 {
			t.Errorf("aggregate = %+v, want logical=10 attempts=12 retryEffects=2 retryDeliveries=3 results=10 dead=1 lost=1", aggregate)
		}
	})
	t.Run("multiple", func(t *testing.T) {
		var aggregate aggregateResult
		aggregate.add(scenarioResult{produced: 10, attempts: 12, retryEffects: 2, retryDeliveries: 3, results: 10, dead: 1, lost: 1})
		aggregate.add(scenarioResult{produced: 20, attempts: 21, retryEffects: 0, retryDeliveries: 1, results: 20, dead: 0, lost: 0})
		want := aggregateResult{logical: 30, attempts: 33, retryEffects: 2, retryDeliveries: 4, results: 30, dead: 1, lost: 1}
		if aggregate != want {
			t.Errorf("aggregate = %+v, want %+v", aggregate, want)
		}
	})
	t.Run("zero result", func(t *testing.T) {
		var aggregate aggregateResult
		aggregate.add(scenarioResult{produced: 5, attempts: 5, results: 5})
		aggregate.add(scenarioResult{})
		if aggregate.logical != 5 || aggregate.attempts != 5 || aggregate.results != 5 {
			t.Errorf("aggregate = %+v, want logical=5 attempts=5 results=5", aggregate)
		}
	})
}

func TestMaxInt(t *testing.T) {
	tests := []struct {
		name  string
		left  int
		right int
		want  int
	}{
		{name: "left larger", left: 5, right: 3, want: 5},
		{name: "right larger", left: 3, right: 5, want: 5},
		{name: "equal", left: 4, right: 4, want: 4},
		{name: "both negative", left: -1, right: -5, want: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := maxInt(test.left, test.right); got != test.want {
				t.Errorf("maxInt(%d, %d) = %d, want %d", test.left, test.right, got, test.want)
			}
		})
	}
}

func TestMinInt(t *testing.T) {
	tests := []struct {
		name  string
		left  int
		right int
		want  int
	}{
		{name: "left smaller", left: 3, right: 5, want: 3},
		{name: "right smaller", left: 5, right: 3, want: 3},
		{name: "equal", left: 4, right: 4, want: 4},
		{name: "both negative", left: -1, right: -5, want: -5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := minInt(test.left, test.right); got != test.want {
				t.Errorf("minInt(%d, %d) = %d, want %d", test.left, test.right, got, test.want)
			}
		})
	}
}

func TestEnvString(t *testing.T) {
	t.Setenv("EQ_TEST_ENV_STRING", "")
	if got := envString("EQ_TEST_ENV_STRING", "fallback"); got != "fallback" {
		t.Errorf("empty env = %q, want %q", got, "fallback")
	}
	t.Setenv("EQ_TEST_ENV_STRING", "custom")
	if got := envString("EQ_TEST_ENV_STRING", "fallback"); got != "custom" {
		t.Errorf("set env = %q, want %q", got, "custom")
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("EQ_TEST_ENV_INT", "")
	if got := envInt("EQ_TEST_ENV_INT", 42); got != 42 {
		t.Errorf("empty env = %d, want %d", got, 42)
	}
	t.Setenv("EQ_TEST_ENV_INT", "7")
	if got := envInt("EQ_TEST_ENV_INT", 42); got != 7 {
		t.Errorf("valid env = %d, want %d", got, 7)
	}
	t.Setenv("EQ_TEST_ENV_INT", "abc")
	if got := envInt("EQ_TEST_ENV_INT", 42); got != 42 {
		t.Errorf("invalid env = %d, want fallback %d", got, 42)
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("EQ_TEST_ENV_DURATION", "")
	if got := envDuration("EQ_TEST_ENV_DURATION", 5*time.Second); got != 5*time.Second {
		t.Errorf("empty env = %s, want %s", got, 5*time.Second)
	}
	t.Setenv("EQ_TEST_ENV_DURATION", "250ms")
	if got := envDuration("EQ_TEST_ENV_DURATION", 5*time.Second); got != 250*time.Millisecond {
		t.Errorf("valid env = %s, want %s", got, 250*time.Millisecond)
	}
	t.Setenv("EQ_TEST_ENV_DURATION", "junk")
	if got := envDuration("EQ_TEST_ENV_DURATION", 5*time.Second); got != 5*time.Second {
		t.Errorf("invalid env = %s, want fallback %s", got, 5*time.Second)
	}
}
