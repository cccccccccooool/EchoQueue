package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestParseDeadRecord(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantErr      bool
		wantEffectID string
	}{
		{name: "valid with effect id", raw: `{"effect_id":"evt-1"}`, wantEffectID: "evt-1"},
		{name: "valid with unicode effect id", raw: `{"effect_id":"事件-🦄"}`, wantEffectID: "事件-🦄"},
		{name: "valid with whitespace raw", raw: `  {"effect_id": "evt-2", "data": {"a": 1}}  `, wantEffectID: "evt-2"},
		{name: "valid with extra fields", raw: `{"effect_id":"evt-3","task_id":"t-1","payload":[1,2,3]}`, wantEffectID: "evt-3"},
		{name: "empty string", raw: "", wantErr: true},
		{name: "not json", raw: "not json", wantErr: true},
		{name: "unbalanced brace", raw: "{", wantErr: true},
		{name: "json null", raw: "null", wantErr: true},
		{name: "json array", raw: "[1]", wantErr: true},
		{name: "empty object", raw: "{}", wantErr: true},
		{name: "missing effect id", raw: `{"other":1}`, wantErr: true},
		{name: "empty effect id", raw: `{"effect_id":""}`, wantErr: true},
		{name: "numeric effect id", raw: `{"effect_id":42}`, wantErr: true},
		{name: "boolean effect id", raw: `{"effect_id":true}`, wantErr: true},
		{name: "null effect id", raw: `{"effect_id":null}`, wantErr: true},
		{name: "object effect id", raw: `{"effect_id":{"a":1}}`, wantErr: true},
		{name: "array effect id", raw: `{"effect_id":[1]}`, wantErr: true},
		{name: "multiple json values", raw: `{"effect_id":"a"}{"effect_id":"b"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDeadRecord(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseDeadRecord(%q) = %+v, nil; want error", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDeadRecord(%q) error: %v", tt.raw, err)
			}
			if got.EffectID != tt.wantEffectID {
				t.Errorf("EffectID = %q; want %q", got.EffectID, tt.wantEffectID)
			}
			if got.Raw != tt.raw {
				t.Errorf("Raw = %q; want input %q byte-for-byte", got.Raw, tt.raw)
			}
		})
	}
}

func TestArchiverConfigValidated(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ArchiverConfig
		want    ArchiverConfig
		wantErr bool
	}{
		{
			name: "zero values use defaults",
			cfg:  ArchiverConfig{DeadKey: "dead", ProcessingKey: "proc"},
			want: ArchiverConfig{DeadKey: "dead", ProcessingKey: "proc", BatchSize: 64, FlushInterval: time.Second, ClaimTimeout: time.Second, ErrorBackoff: 500 * time.Millisecond},
		},
		{
			name: "zero batch size defaults",
			cfg:  ArchiverConfig{DeadKey: "dead", ProcessingKey: "proc", BatchSize: 0},
			want: ArchiverConfig{DeadKey: "dead", ProcessingKey: "proc", BatchSize: 64, FlushInterval: time.Second, ClaimTimeout: time.Second, ErrorBackoff: 500 * time.Millisecond},
		},
		{
			name: "explicit values preserved",
			cfg:  ArchiverConfig{DeadKey: "d", ProcessingKey: "p", BatchSize: 10, FlushInterval: 2 * time.Second, ClaimTimeout: 3 * time.Second, ErrorBackoff: 4 * time.Second},
			want: ArchiverConfig{DeadKey: "d", ProcessingKey: "p", BatchSize: 10, FlushInterval: 2 * time.Second, ClaimTimeout: 3 * time.Second, ErrorBackoff: 4 * time.Second},
		},
		{name: "empty dead key", cfg: ArchiverConfig{ProcessingKey: "proc"}, wantErr: true},
		{name: "empty processing key", cfg: ArchiverConfig{DeadKey: "dead"}, wantErr: true},
		{name: "identical keys", cfg: ArchiverConfig{DeadKey: "same", ProcessingKey: "same"}, wantErr: true},
		{name: "negative batch size", cfg: ArchiverConfig{DeadKey: "d", ProcessingKey: "p", BatchSize: -1}, wantErr: true},
		{name: "negative flush interval", cfg: ArchiverConfig{DeadKey: "d", ProcessingKey: "p", FlushInterval: -time.Second}, wantErr: true},
		{name: "negative claim timeout", cfg: ArchiverConfig{DeadKey: "d", ProcessingKey: "p", ClaimTimeout: -time.Second}, wantErr: true},
		{name: "negative error backoff", cfg: ArchiverConfig{DeadKey: "d", ProcessingKey: "p", ErrorBackoff: -time.Second}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.cfg.validated()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validated() = %+v, nil; want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("validated() error: %v", err)
			}
			if got != tt.want {
				t.Errorf("validated() = %+v; want %+v", got, tt.want)
			}
		})
	}
}

func TestDefaultArchiverConfig(t *testing.T) {
	got := defaultArchiverConfig()
	want := ArchiverConfig{BatchSize: 64, FlushInterval: time.Second, ClaimTimeout: time.Second, ErrorBackoff: 500 * time.Millisecond}
	if got != want {
		t.Errorf("defaultArchiverConfig() = %+v; want %+v", got, want)
	}
}

type testDeadSink struct{}

func (testDeadSink) PersistDead(ctx context.Context, records []DeadRecord) error {
	return nil
}

func TestNewDeadArchiver(t *testing.T) {
	validCfg := ArchiverConfig{DeadKey: "dead", ProcessingKey: "proc"}
	tests := []struct {
		name    string
		rdb     *redis.Client
		cfg     ArchiverConfig
		sink    DeadSink
		wantCfg ArchiverConfig
		wantErr bool
	}{
		{name: "nil redis client", rdb: nil, cfg: validCfg, sink: testDeadSink{}, wantErr: true},
		{name: "nil sink", rdb: &redis.Client{}, cfg: validCfg, sink: nil, wantErr: true},
		{name: "invalid config", rdb: &redis.Client{}, cfg: ArchiverConfig{}, sink: testDeadSink{}, wantErr: true},
		{
			name:    "valid applies defaults",
			rdb:     &redis.Client{},
			cfg:     validCfg,
			sink:    testDeadSink{},
			wantCfg: ArchiverConfig{DeadKey: "dead", ProcessingKey: "proc", BatchSize: 64, FlushInterval: time.Second, ClaimTimeout: time.Second, ErrorBackoff: 500 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archiver, err := NewDeadArchiver(tt.rdb, tt.cfg, tt.sink)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewDeadArchiver() = %+v, nil; want error", archiver)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewDeadArchiver() error: %v", err)
			}
			if archiver == nil {
				t.Fatal("NewDeadArchiver() = nil; want non-nil")
			}
			if archiver.cfg != tt.wantCfg {
				t.Errorf("archiver.cfg = %+v; want %+v", archiver.cfg, tt.wantCfg)
			}
		})
	}
}
