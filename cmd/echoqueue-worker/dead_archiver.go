// Dead archiver reference implementation. It moves terminal Dead records out
// of Redis into an external, host-supplied sink without ever acknowledging a
// record that was not durably persisted.
//
// Protocol per record:
//
//	reserve buffer capacity
//	  -> BLMOVE Dead DeadProcessing LEFT RIGHT
//	  -> bounded batch aggregator
//	  -> external sink persists by effect_id (idempotent)
//	  -> LREM DeadProcessing 1 rawRecord only after explicit success
//	  -> release capacity
//
// The Redis DeadProcessing list is the durable claim: Go memory is only a
// bounded buffer, never the record of record. A crash after BLMOVE but before
// persist leaves the record in DeadProcessing; a crash after persist but
// before ACK causes an idempotent duplicate persist on restart. The first
// version supports exactly one active archiver; there is no distributed lock.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrorSink receives host-visible errors from the archiver. The reference
// logs them; a production host would alert.
type ErrorSink func(err error)

// errArchiverAlreadyActive mirrors Scheduler's ErrRunAlreadyActive: an
// archiver may only have one active Run call.
var errArchiverAlreadyActive = errors.New("echoqueue worker: dead archiver is already running")

// DeadRecord is one raw Dead list element plus its parsed idempotency key.
type DeadRecord struct {
	// Raw is the exact Redis list element; ACK uses LREM with this value.
	Raw string
	// EffectID is the external idempotency key extracted from the record.
	EffectID string
}

// DeadSink is the host-local external persistence boundary. It must be
// idempotent by EffectID: a second PersistDead call for the same EffectID
// must succeed without creating a duplicate external record, because crashes
// between persist and ACK can deliver the same record more than once.
type DeadSink interface {
	PersistDead(ctx context.Context, records []DeadRecord) error
}

// ArchiverConfig is host-local configuration for the dead archiver. None of
// these fields enter the EchoQueue Config type.
type ArchiverConfig struct {
	// DeadKey is the Redis list written by Settle/Recover dead effects. The
	// host must provide an explicit Dead key; the library-internal default
	// dead list is private.
	DeadKey string
	// ProcessingKey must be a list that is never used as Source, Result,
	// Dead, or any other key. BLMOVE moves records here before persistence.
	ProcessingKey string
	// BatchSize is the hard cap on records held in the aggregator. Capacity
	// is always reserved before BLMOVE, so no unbounded prefetch is possible.
	BatchSize int
	// FlushInterval triggers a persist even when the batch is not full.
	FlushInterval time.Duration
	// ClaimTimeout bounds each BLMOVE call so the loop can check context
	// cancellation and the flush interval.
	ClaimTimeout time.Duration
	// ErrorBackoff is the pause after a persist/ack/claim error.
	ErrorBackoff time.Duration
}

func (c ArchiverConfig) validated() (ArchiverConfig, error) {
	defaults := defaultArchiverConfig()
	if c.DeadKey == "" || c.ProcessingKey == "" {
		return ArchiverConfig{}, errors.New("echoqueue worker: dead and processing keys are required")
	}
	if c.DeadKey == c.ProcessingKey {
		return ArchiverConfig{}, errors.New("echoqueue worker: dead and processing keys must be distinct")
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaults.BatchSize
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = defaults.FlushInterval
	}
	if c.ClaimTimeout == 0 {
		c.ClaimTimeout = defaults.ClaimTimeout
	}
	if c.ErrorBackoff == 0 {
		c.ErrorBackoff = defaults.ErrorBackoff
	}
	if c.BatchSize <= 0 || c.FlushInterval <= 0 || c.ClaimTimeout <= 0 || c.ErrorBackoff <= 0 {
		return ArchiverConfig{}, errors.New("echoqueue worker: batch_size, flush_interval, claim_timeout, and error_backoff must be positive")
	}
	return c, nil
}

func defaultArchiverConfig() ArchiverConfig {
	return ArchiverConfig{
		BatchSize:     64,
		FlushInterval: time.Second,
		ClaimTimeout:  time.Second,
		ErrorBackoff:  500 * time.Millisecond,
	}
}

// DeadArchiver claims Dead records with BLMOVE and persists them through the
// host sink before ACK removal.
type DeadArchiver struct {
	rdb     *redis.Client
	cfg     ArchiverConfig
	sink    DeadSink
	startMu sync.Mutex
	active  bool
}

// NewDeadArchiver constructs the archiver. It does not contact Redis.
func NewDeadArchiver(rdb *redis.Client, cfg ArchiverConfig, sink DeadSink) (*DeadArchiver, error) {
	if rdb == nil || sink == nil {
		return nil, errors.New("echoqueue worker: redis client and dead sink are required")
	}
	validated, err := cfg.validated()
	if err != nil {
		return nil, err
	}
	return &DeadArchiver{rdb: rdb, cfg: validated, sink: sink}, nil
}

// Run processes dead records until ctx is cancelled. Leftover DeadProcessing
// records from a previous run are replayed first, then the loop alternates
// between bounded claiming (BLMOVE), batch flushing (persist then ACK), and
// interval-triggered flushes. Persist failure, uncertain results, or context
// cancellation never ACK a record, so DeadProcessing remains the durable
// claim. Corrupt records and records without effect_id are preserved and
// reported. Run returns errArchiverAlreadyActive if called twice concurrently.
func (a *DeadArchiver) Run(ctx context.Context, report ErrorSink) error {
	if ctx == nil {
		return errors.New("echoqueue worker: context is nil")
	}
	if report == nil {
		return errors.New("echoqueue worker: error sink is required")
	}
	a.startMu.Lock()
	if a.active {
		a.startMu.Unlock()
		return errArchiverAlreadyActive
	}
	a.active = true
	a.startMu.Unlock()
	defer func() {
		a.startMu.Lock()
		a.active = false
		a.startMu.Unlock()
	}()

	// Replay the durable claim left by an earlier run before claiming more.
	if err := a.replayProcessing(ctx, report); err != nil {
		return fmt.Errorf("echoqueue worker: replay processing: %w", err)
	}

	buffer := make([]DeadRecord, 0, a.cfg.BatchSize)
	interval := time.NewTimer(a.cfg.FlushInterval)
	defer interval.Stop()
	// corruptStreak counts consecutive unparseable claims. Once it reaches
	// BatchSize the Dead head is considered poisoned: claiming more would
	// grow DeadProcessing without bound, so claiming pauses until the head
	// becomes parseable again.
	corruptStreak := 0

	for {
		if err := ctx.Err(); err != nil {
			if len(buffer) > 0 {
				if err := a.flush(ctx, buffer); err != nil {
					report(fmt.Errorf("echoqueue worker: final flush: %w", err))
				}
			}
			return nil
		}
		if len(buffer) >= a.cfg.BatchSize {
			if err := a.flush(ctx, buffer); err != nil {
				report(err)
				a.wait(ctx)
				continue
			}
			buffer = buffer[:0]
			interval.Reset(a.cfg.FlushInterval)
			continue
		}
		select {
		case <-ctx.Done():
			continue
		case <-interval.C:
			if len(buffer) > 0 {
				if err := a.flush(ctx, buffer); err != nil {
					report(err)
				} else {
					buffer = buffer[:0]
				}
			}
			interval.Reset(a.cfg.FlushInterval)
			continue
		default:
		}

		if corruptStreak >= a.cfg.BatchSize {
			// Poisoned head: do not claim. Corrupt records never enter the
			// buffer, so capacity alone cannot bound the prefetch; probing the
			// head instead ensures DeadProcessing grows by at most BatchSize
			// per corrupt burst and that claiming resumes as soon as the head
			// is parseable again.
			clean, probeErr := a.deadHeadClean(ctx)
			switch {
			case probeErr != nil:
				if ctx.Err() == nil {
					report(fmt.Errorf("echoqueue worker: probe dead head: %w", probeErr))
				}
			case clean:
				corruptStreak = 0
				continue
			default:
				report(fmt.Errorf("echoqueue worker: dead head rejected after %d consecutive corrupt claims; claiming paused until the head is parseable", corruptStreak))
			}
			a.wait(ctx)
			continue
		}

		// Capacity is available: claim one record with BLMOVE. The buffer is
		// never full at this point, so prefetch stays bounded.
		raw, err := a.rdb.BLMove(ctx, a.cfg.DeadKey, a.cfg.ProcessingKey, "LEFT", "RIGHT", a.cfg.ClaimTimeout).Result()
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			report(fmt.Errorf("echoqueue worker: claim dead record: %w", err))
			a.wait(ctx)
			continue
		}
		if raw == "" {
			continue
		}
		record, err := parseDeadRecord(raw)
		if err != nil {
			// The record stays in DeadProcessing and is never ACKed; it will
			// be reported again on restart.
			report(fmt.Errorf("echoqueue worker: dead record rejected: %w", err))
			corruptStreak++
			continue
		}
		corruptStreak = 0
		buffer = append(buffer, record)
	}
}

// deadHeadClean reports whether the head of the Dead list can be claimed:
// either Dead is empty or its head parses as a valid dead record.
func (a *DeadArchiver) deadHeadClean(ctx context.Context) (bool, error) {
	raws, err := a.rdb.LRange(ctx, a.cfg.DeadKey, 0, 0).Result()
	if err != nil {
		return false, err
	}
	if len(raws) == 0 {
		return true, nil
	}
	_, err = parseDeadRecord(raws[0])
	return err == nil, nil
}

// replayProcessing drains leftover DeadProcessing records from a previous run
// through the same persist-then-ACK path, reading fixed windows of at most
// BatchSize records so Go memory stays bounded no matter how many records were
// left behind. Records that fail parsing are preserved and reported; flush
// failure returns before any ACK so the unconfirmed records remain the durable
// claim. Window advancement counts only the records kept in Redis (corrupt
// ones): acknowledged records are removed from the list, so re-reading from
// the same offset would otherwise skip over unprocessed records.
func (a *DeadArchiver) replayProcessing(ctx context.Context, report ErrorSink) error {
	buffer := make([]DeadRecord, 0, a.cfg.BatchSize)
	var start int64
	for {
		raws, err := a.rdb.LRange(ctx, a.cfg.ProcessingKey, start, start+int64(a.cfg.BatchSize)-1).Result()
		if err != nil {
			return err
		}
		if len(raws) == 0 {
			return nil
		}
		kept := 0
		for _, raw := range raws {
			record, err := parseDeadRecord(raw)
			if err != nil {
				report(fmt.Errorf("echoqueue worker: processing record rejected: %w", err))
				kept++
				continue
			}
			buffer = append(buffer, record)
			if len(buffer) >= a.cfg.BatchSize {
				if err := a.flush(ctx, buffer); err != nil {
					return err
				}
				buffer = buffer[:0]
			}
		}
		if len(buffer) > 0 {
			if err := a.flush(ctx, buffer); err != nil {
				return err
			}
			buffer = buffer[:0]
		}
		start += int64(kept)
	}
}

// flush persists the batch first and only then ACKs each record. On persist
// failure nothing is ACKed. On ACK failure the records are already durably
// persisted, so a restart replays them and the sink deduplicates by effect_id.
func (a *DeadArchiver) flush(ctx context.Context, records []DeadRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := a.sink.PersistDead(ctx, records); err != nil {
		return fmt.Errorf("echoqueue worker: dead persist failed: %w", err)
	}
	for _, record := range records {
		removed, err := a.rdb.LRem(ctx, a.cfg.ProcessingKey, 1, record.Raw).Result()
		if err != nil {
			return fmt.Errorf("echoqueue worker: dead ack failed for effect_id %q: %w", record.EffectID, err)
		}
		if removed == 0 {
			// Persisted and already removed: the record is not lost. With a
			// single active archiver this should not happen.
			return nil
		}
	}
	return nil
}

func (a *DeadArchiver) wait(ctx context.Context) {
	timer := time.NewTimer(a.cfg.ErrorBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type deadEnvelope struct {
	EffectID string `json:"effect_id"`
}

// parseDeadRecord validates the raw record and extracts the idempotency key.
// Invalid JSON, a missing effect_id, or a wrong-typed effect_id is rejected;
// the caller keeps the raw record in DeadProcessing without ACK.
func parseDeadRecord(raw string) (DeadRecord, error) {
	var envelope deadEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return DeadRecord{}, fmt.Errorf("dead record is not valid JSON: %w", err)
	}
	if envelope.EffectID == "" {
		return DeadRecord{}, errors.New("dead record is missing effect_id")
	}
	return DeadRecord{Raw: raw, EffectID: envelope.EffectID}, nil
}
