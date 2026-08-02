package echoqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

var (
	errRedisCheck   = errors.New("echoqueue: redis capability check failed")
	errRedisVersion = errors.New("echoqueue: Redis 6.2 or newer is required")
	errRedisCluster = errors.New("echoqueue: Redis Cluster is not supported")
)

func checkRedis(ctx context.Context, client *redis.Client) error {
	if client == nil {
		return fmt.Errorf("%w: nil client", errRedisCheck)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", errRedisCheck)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return fmt.Errorf("%w: server info: %v", errRedisCheck, err)
	}
	major, minor, err := redisVersion(info)
	if err != nil {
		return fmt.Errorf("%w: %v", errRedisCheck, err)
	}
	if major < 6 || (major == 6 && minor < 2) {
		return fmt.Errorf("%w: got %d.%d", errRedisVersion, major, minor)
	}

	clusterInfo, infoErr := client.Info(ctx, "cluster").Result()
	if infoErr == nil {
		flag := infoValue(clusterInfo, "cluster_enabled")
		if flag == "1" {
			return errRedisCluster
		}
		if flag == "0" {
			return nil
		}
	}
	clusterCommand, clusterErr := client.ClusterInfo(ctx).Result()
	if clusterErr == nil {
		return fmt.Errorf("%w: CLUSTER INFO returned %q", errRedisCluster, clusterCommand)
	}
	if isClusterDisabled(clusterErr) {
		return nil
	}
	return fmt.Errorf("%w: cluster probe: info=%v command=%v", errRedisCheck, infoErr, clusterErr)
}

func serverMillis(ctx context.Context, client *redis.Client) (int64, error) {
	value, err := client.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	if value.Unix() <= 0 || value.Nanosecond() < 0 {
		return 0, fmt.Errorf("invalid Redis TIME %v", value)
	}
	return value.UnixMilli(), nil
}

func redisVersion(info string) (int, int, error) {
	raw := infoValue(info, "redis_version")
	parts := strings.Split(raw, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid redis_version %q", raw)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid redis_version %q: %w", raw, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid redis_version %q: %w", raw, err)
	}
	return major, minor, nil
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

func isClusterDisabled(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "cluster support disabled") ||
		strings.Contains(message, "cluster support is disabled") ||
		strings.Contains(message, "cluster is not enabled")
}
