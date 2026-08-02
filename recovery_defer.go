package echoqueue

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"
)

//go:embed scripts/defer_recover.lua
var deferRecoverScript string

var errDeferRecover = errors.New("echoqueue: defer recover failed")

func (s *Scheduler) deferRecover(ctx context.Context, batchID string) (string, error) {
	if s == nil || s.rdb == nil {
		return "", fmt.Errorf("%w: scheduler is nil", errDeferRecover)
	}
	if ctx == nil {
		return "", fmt.Errorf("%w: context is nil", errDeferRecover)
	}
	if err := validateText("batch_id", batchID, true); err != nil {
		return "", fmt.Errorf("%w: %v", errDeferRecover, err)
	}
	if err := s.ensureRedis(ctx); err != nil {
		return "", err
	}

	delay := s.config.RunInterval
	if delay < time.Second {
		delay = time.Second
	}
	delayMillis := delay.Milliseconds()
	if delayMillis < 1 {
		delayMillis = 1
	}
	value, err := s.rdb.Eval(ctx, deferRecoverScript, []string{
		s.keys.pending(batchID),
		s.keys.receipt(batchID),
		s.keys.deadline(),
	}, batchID, delayMillis).Result()
	if err != nil {
		return "", fmt.Errorf("%w: Redis script: %v", errDeferRecover, err)
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) == 0 {
		return "", fmt.Errorf("%w: malformed script response", errDeferRecover)
	}
	status, err := scriptString(parts[0])
	if err != nil {
		return "", fmt.Errorf("%w: status: %v", errDeferRecover, err)
	}
	switch status {
	case "terminal", "orphan", "deferred":
		return status, nil
	case "invalid":
		message := "script rejected defer"
		if len(parts) > 1 {
			message, _ = scriptString(parts[1])
		}
		return status, fmt.Errorf("%w: %s", errDeferRecover, message)
	default:
		return status, fmt.Errorf("%w: unknown status %q", errDeferRecover, status)
	}
}
