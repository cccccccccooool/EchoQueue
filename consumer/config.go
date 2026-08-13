// Package consumer hosts the staged consumption pipeline for EchoQueue:
// bounded Dispatcher -> batch channel -> Handler -> outcome channel ->
// Settler. All stages share a single MaxInFlight quota that is acquired
// before Dispatch, so a full pipeline stops calling Dispatch and leaves
// tasks in the Source list.
package consumer

import (
	"errors"
	"time"
)

// Config is host-local pipeline configuration. None of these fields enter
// the EchoQueue Config type; the library keeps its reliability protocol, the
// consumer package owns scheduling.
type Config struct {
	// Dispatchers is the number of Dispatch goroutines; at most this many
	// Dispatch calls run at once. Values above 4 rarely help because the
	// Redis Lua command serializes on the server anyway.
	Dispatchers int
	// Workers is the number of handler goroutines; at most this many
	// Handlers are active at once.
	Workers int
	// Settlers is the number of settle goroutines; at most this many
	// Settle calls run at once. A separate pool keeps Redis settle latency
	// from occupying handler workers.
	Settlers int
	// MaxInFlight bounds how many batches may be dispatched but not yet
	// settled. Each in-flight batch holds one global permit that is acquired
	// before Dispatch and released after Settle (or after any early return).
	MaxInFlight int
	// BatchSize is passed to Dispatch for every call.
	BatchSize int
	// BatchBuffer bounds how many dispatched batches may wait in memory for
	// a handler. Dispatch never runs without a reserved batch slot.
	BatchBuffer int
	// OutcomeBuffer bounds how many computed outcomes may wait in memory for
	// a settler.
	OutcomeBuffer int
	// PollInterval is the pause after an empty Dispatch.
	PollInterval time.Duration
	// ErrorBackoff is the jittered pause after a Dispatch or Settle error,
	// and the probe interval while the settle circuit breaker is open.
	ErrorBackoff time.Duration
	// ShutdownGrace bounds how long Run may wait for handlers and settlers
	// after context cancellation. Run always returns by this deadline;
	// abandoned batches are left to EchoQueue's Pending/Recover loop.
	ShutdownGrace time.Duration
	// Metrics receives stage counters; nil means no metrics.
	Metrics MetricsSink
}

// DefaultConfig returns conservative defaults. Raise Dispatchers only after
// measuring that Redis Lua latency stays flat.
func DefaultConfig() Config {
	return Config{
		Dispatchers:   1,
		Workers:       4,
		Settlers:      2,
		MaxInFlight:   16,
		BatchSize:     1,
		BatchBuffer:   8,
		OutcomeBuffer: 8,
		PollInterval:  time.Second,
		ErrorBackoff:  500 * time.Millisecond,
		ShutdownGrace: 5 * time.Second,
	}
}

func (c Config) validated() (Config, error) {
	defaults := DefaultConfig()
	if c.Dispatchers == 0 {
		c.Dispatchers = defaults.Dispatchers
	}
	if c.Workers == 0 {
		c.Workers = defaults.Workers
	}
	if c.Settlers == 0 {
		c.Settlers = defaults.Settlers
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = defaults.MaxInFlight
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaults.BatchSize
	}
	if c.BatchBuffer == 0 {
		c.BatchBuffer = defaults.BatchBuffer
	}
	if c.OutcomeBuffer == 0 {
		c.OutcomeBuffer = defaults.OutcomeBuffer
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaults.PollInterval
	}
	if c.ErrorBackoff == 0 {
		c.ErrorBackoff = defaults.ErrorBackoff
	}
	if c.ShutdownGrace == 0 {
		c.ShutdownGrace = defaults.ShutdownGrace
	}
	if c.Dispatchers <= 0 || c.Workers <= 0 || c.Settlers <= 0 || c.MaxInFlight <= 0 || c.BatchSize <= 0 || c.BatchBuffer <= 0 || c.OutcomeBuffer <= 0 {
		return Config{}, errors.New("echoqueue consumer: dispatchers, workers, settlers, max_in_flight, batch_size, batch_buffer, and outcome_buffer must be positive")
	}
	if c.PollInterval <= 0 || c.ErrorBackoff <= 0 || c.ShutdownGrace <= 0 {
		return Config{}, errors.New("echoqueue consumer: poll_interval, error_backoff, and shutdown_grace must be positive")
	}
	if c.Metrics == nil {
		c.Metrics = noopMetrics{}
	}
	return c, nil
}
