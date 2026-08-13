package main

import (
	"testing"
	"time"
)

func TestParseMatrix(t *testing.T) {
	cells, err := parseMatrix("1x1x1, 2x4x2 ,4x8x4")
	if err != nil {
		t.Fatalf("parseMatrix: %v", err)
	}
	want := []cell{
		{dispatchers: 1, workers: 1, settlers: 1},
		{dispatchers: 2, workers: 4, settlers: 2},
		{dispatchers: 4, workers: 8, settlers: 4},
	}
	if len(cells) != len(want) {
		t.Fatalf("cells = %v, want %v", cells, want)
	}
	for i := range cells {
		if cells[i] != want[i] {
			t.Fatalf("cell %d = %v, want %v", i, cells[i], want[i])
		}
	}
}

func TestParseMatrixRejectsInvalid(t *testing.T) {
	invalid := []string{"", ",", "1", "1x2", "1x2x", "axbx1", "0x1x1", "1x0x1", "1x1x0", "1,2x3x4"}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			if _, err := parseMatrix(raw); err == nil {
				t.Errorf("parseMatrix(%q) = nil, want error", raw)
			}
		})
	}
}

func TestLatencyRecorderPercentiles(t *testing.T) {
	recorder := &latencyRecorder{}
	for i := 0; i < 100; i++ {
		recorder.record(time.Duration(i) * time.Millisecond)
	}
	percentiles := recorder.percentiles(0.5, 0.95, 0.99)
	if percentiles[0.5] != 49*time.Millisecond {
		t.Fatalf("p50 = %v, want 49ms", percentiles[0.5])
	}
	if percentiles[0.95] != 94*time.Millisecond {
		t.Fatalf("p95 = %v, want 94ms", percentiles[0.95])
	}
	if percentiles[0.99] != 98*time.Millisecond {
		t.Fatalf("p99 = %v, want 98ms", percentiles[0.99])
	}
}

func TestLatencyRecorderEmpty(t *testing.T) {
	recorder := &latencyRecorder{}
	percentiles := recorder.percentiles(0.5)
	if percentiles[0.5] != 0 {
		t.Fatalf("empty p50 = %v, want 0", percentiles[0.5])
	}
}
