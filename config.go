package echoqueue

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Config contains the process-wide defaults and the few safety limits needed
// by the library. Queue addresses belong to QueueConfig, supplied by the host.
type Config struct {
	Namespace         string
	VisibilityTimeout time.Duration
	// ReceiptTTL bounds how long terminal receipts remain available for replay.
	ReceiptTTL time.Duration
	MaxRetry   int
	// MaxRetrySet must be true when MaxRetry is intentionally zero.
	MaxRetrySet     bool
	MaxBatchSize    int
	MaxPayloadBytes int
	MaxBatchBytes   int
	RunInterval     time.Duration
	RunBatchSize    int
}

// DefaultConfig returns conservative defaults. Callers that intentionally set
// MaxRetry to zero should set MaxRetrySet to true.
func DefaultConfig() Config {
	return Config{
		VisibilityTimeout: 30 * time.Second,
		ReceiptTTL:        24 * time.Hour,
		MaxRetry:          3,
		MaxRetrySet:       true,
		MaxBatchSize:      1000,
		MaxPayloadBytes:   1 << 20,
		MaxBatchBytes:     64 << 20,
		RunInterval:       500 * time.Millisecond,
		RunBatchSize:      32,
	}
}

func (c Config) normalized() (Config, error) {
	defaults := DefaultConfig()
	if c.VisibilityTimeout == 0 {
		c.VisibilityTimeout = defaults.VisibilityTimeout
	}
	if c.ReceiptTTL == 0 {
		c.ReceiptTTL = defaults.ReceiptTTL
	}
	if c.MaxBatchSize == 0 {
		c.MaxBatchSize = defaults.MaxBatchSize
	}
	if c.MaxPayloadBytes == 0 {
		c.MaxPayloadBytes = defaults.MaxPayloadBytes
	}
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = defaults.MaxBatchBytes
	}
	if c.RunInterval == 0 {
		c.RunInterval = defaults.RunInterval
	}
	if c.RunBatchSize == 0 {
		c.RunBatchSize = defaults.RunBatchSize
	}
	if c.MaxRetry < 0 {
		return Config{}, fmt.Errorf("max_retry cannot be negative")
	}
	if !c.MaxRetrySet && c.MaxRetry == 0 {
		c.MaxRetry = defaults.MaxRetry
	}
	c.MaxRetrySet = true
	if err := validateText("namespace", c.Namespace, true); err != nil {
		return Config{}, err
	}
	if c.VisibilityTimeout < time.Millisecond {
		return Config{}, fmt.Errorf("visibility_timeout must be at least 1ms")
	}
	if c.ReceiptTTL < time.Millisecond {
		return Config{}, fmt.Errorf("receipt_ttl must be at least 1ms")
	}
	if c.MaxBatchSize <= 0 || c.MaxPayloadBytes <= 0 || c.MaxBatchBytes <= 0 || c.RunBatchSize <= 0 {
		return Config{}, fmt.Errorf("batch and payload limits must be positive")
	}
	if c.MaxBatchBytes < c.MaxPayloadBytes {
		return Config{}, fmt.Errorf("max_batch_bytes cannot be smaller than max_payload_bytes")
	}
	if c.RunInterval <= 0 {
		return Config{}, fmt.Errorf("run_interval must be positive")
	}
	return c, nil
}

// QueueConfig is the only routing input. Source, Result, and Dead are Redis
// list keys supplied by the host; an empty Result or Dead means that output is
// kept in the library's namespace-local list.
type QueueConfig struct {
	TaskName string `json:"task_name"`
	Source   string `json:"source"`
	Result   string `json:"result,omitempty"`
	Dead     string `json:"dead,omitempty"`
}

func (c QueueConfig) normalized() (QueueConfig, error) {
	if err := validateText("task_name", c.TaskName, true); err != nil {
		return QueueConfig{}, err
	}
	if err := validateText("source", c.Source, true); err != nil {
		return QueueConfig{}, err
	}
	for name, value := range map[string]string{"result": c.Result, "dead": c.Dead} {
		if err := validateText(name, value, false); err != nil {
			return QueueConfig{}, err
		}
	}
	if c.Result == c.Source || c.Dead == c.Source || (c.Result != "" && c.Result == c.Dead) {
		return QueueConfig{}, fmt.Errorf("source, result, and dead keys must be distinct")
	}
	return c, nil
}

func validateText(name, value string, required bool) error {
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if !required && value == "" {
		return nil
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", name)
	}
	for _, r := range value {
		if r == 0 || r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s contains a control character", name)
		}
	}
	return nil
}
