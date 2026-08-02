package testutil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// MustRedis returns a writable Redis 6.2+ standalone/replication-primary
// client. A missing Redis is a test failure for integration-tagged tests.
func MustRedis(t *testing.T) *redis.Client {
	t.Helper()
	address := strings.TrimSpace(os.Getenv("ECHOQUEUE_REDIS_ADDR"))
	if address == "" {
		t.Fatal("ECHOQUEUE_REDIS_ADDR is required for integration tests")
	}
	var options *redis.Options
	var err error
	if strings.Contains(address, "://") {
		options, err = redis.ParseURL(address)
		if err != nil {
			t.Fatalf("invalid ECHOQUEUE_REDIS_ADDR %q: %v", address, err)
		}
	} else {
		options = &redis.Options{Addr: address}
	}
	rdb := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("Redis unavailable at %q: %v", address, err)
	}
	server, err := rdb.Info(ctx, "server").Result()
	if err != nil {
		_ = rdb.Close()
		t.Fatalf("cannot inspect Redis server: %v", err)
	}
	major, minor, err := redisVersion(server)
	if err != nil || major < 6 || (major == 6 && minor < 2) {
		_ = rdb.Close()
		t.Fatalf("Redis 6.2+ is required, got %q", infoValue(server, "redis_version"))
	}
	cluster, err := rdb.Info(ctx, "cluster").Result()
	if err != nil || infoValue(cluster, "cluster_enabled") != "0" {
		_ = rdb.Close()
		t.Fatalf("integration Redis must have cluster_enabled=0, got %q (err=%v)", infoValue(cluster, "cluster_enabled"), err)
	}
	replication, err := rdb.Info(ctx, "replication").Result()
	if err != nil || infoValue(replication, "role") != "master" {
		_ = rdb.Close()
		t.Fatalf("integration Redis must be writable primary, got role %q (err=%v)", infoValue(replication, "role"), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func redisVersion(info string) (int, int, error) {
	parts := strings.Split(infoValue(info, "redis_version"), ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("malformed Redis version")
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	minor, err := strconv.Atoi(parts[1])
	return major, minor, err
}

func infoValue(info, key string) string {
	prefix := key + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

func WaitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !condition() {
		t.Fatalf("condition did not become true within %s", timeout)
	}
}
