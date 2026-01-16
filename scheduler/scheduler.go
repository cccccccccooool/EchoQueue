package scheduler

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	QueueKeyPrimary     = "ai_analyse"
	QueueKeyBackup      = "ai_analyse_backup"
	QueueKeyRetrySignal = "ai_analyse_retry_signal"
	TimePerTaskSeconds  = 10
)

// dispatchScript Lua 脚本: 原子性地从队列取出数据并存入备份 Hash
// KEYS[1]: 主队列 Key
// KEYS[2]: 备份 Hash Key
// ARGV[1]: 批量大小
// ARGV[2]: BatchID
// ARGV[3]: 当前时间戳 (Unix Seconds)
// ARGV[4]: 每个任务超时时间 (Seconds)
// 使用 go:embed 将同目录下的 redis.lua 嵌入进来，编译时包含脚本
//
//go:embed redis.lua
var dispatchScript string

type BackupData struct {
	Data     []string `json:"data"`
	Deadline int64    `json:"deadline"`
}

// TaskScheduler 任务调度器结构体
type TaskScheduler struct {
	rdb *redis.Client
}

// NewScheduler 创建新的调度器实例
func NewScheduler(rdb *redis.Client) *TaskScheduler {
	return &TaskScheduler{
		rdb: rdb,
	}
}

// Dispatch 调度任务: 原子性地拉取任务并备份
func (s *TaskScheduler) Dispatch(ctx context.Context, batchSize int) (string, []string, error) {
	if batchSize <= 0 {
		return "", nil, errors.New("batchSize must be positive")
	}

	batchID := uuid.New().String()
	now := time.Now().Unix()

	cmd := s.rdb.Eval(ctx, dispatchScript,
		[]string{QueueKeyPrimary, QueueKeyBackup},
		batchSize, batchID, now, TimePerTaskSeconds,
	)

	result, err := cmd.Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("redis eval error: %w", err)
	}

	itemsInterface, ok := result.([]interface{})
	if !ok {
		if result == nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("unexpected return type from lua script")
	}

	tasks := make([]string, 0, len(itemsInterface))
	for _, item := range itemsInterface {
		str, ok := item.(string)
		if !ok {
			str = fmt.Sprintf("%v", item)
		}
		tasks = append(tasks, str)
	}

	return batchID, tasks, nil
}

// Ack 确认任务完成: 从备份中删除
func (s *TaskScheduler) Ack(ctx context.Context, batchID string) error {
	if batchID == "" {
		return errors.New("empty batchID")
	}
	err := s.rdb.HDel(ctx, QueueKeyBackup, batchID).Err()
	if err != nil {
		return fmt.Errorf("ack failed: %w", err)
	}
	return nil
}

// 定期检查过期的 Batch
func (s *TaskScheduler) StartWatchDog(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	log.Println("[WatchDog] 已启动，正在监控任务超时...")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("[WatchDog] 停止运行")
				return
			case <-ticker.C:
				s.checkTimeouts(ctx)
			}
		}
	}()
}

// 检查超时逻辑
func (s *TaskScheduler) checkTimeouts(ctx context.Context) {
	now := time.Now().Unix()
	var cursor uint64 = 0
	const scanCount int64 = 100 // 每次扫描数量

	for {
		if ctx.Err() != nil {
			return
		}

		vals, next, err := s.rdb.HScan(ctx, QueueKeyBackup, cursor, "*", scanCount).Result()
		if err != nil {
			log.Printf("[WatchDog] HSCAN 读取 backup hash 失败: %v", err)
			return
		}

		for i := 0; i+1 < len(vals); i += 2 {
			batchID := vals[i]
			val := vals[i+1]

			var data BackupData
			if err := json.Unmarshal([]byte(val), &data); err != nil {
				log.Printf("[WatchDog] 解析 JSON 失败 (batchID=%s): %v", batchID, err)
				continue
			}
			//延后处理过期任务
			if data.Deadline < now {
				log.Printf("[WatchDog] 发现过期 BatchID: %s", batchID)
				if err := s.rdb.LPush(ctx, QueueKeyRetrySignal, batchID).Err(); err != nil {
					log.Printf("[WatchDog] 推送重试信号失败: %v", err)
				} else {
					newDeadline := now + 300 // 5分钟后再次检查
					data.Deadline = newDeadline
					if newData, err := json.Marshal(data); err == nil {
						s.rdb.HSet(ctx, QueueKeyBackup, batchID, newData)
					}
				}
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}
}

// 监听重试信号并恢复任务
func (s *TaskScheduler) StartRetryHandler(ctx context.Context) {
	log.Println("[RetryHandler] 已启动，等待重试信号...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("[RetryHandler] 停止运行")
				return
			default:
				res, err := s.rdb.BRPop(ctx, 5*time.Second, QueueKeyRetrySignal).Result()
				if err != nil {
					if err == redis.Nil {
						continue
					}
					if !errors.Is(err, context.Canceled) {
						log.Printf("[RetryHandler] BPOP 错误: %v", err)
						time.Sleep(1 * time.Second)
					}
					continue
				}

				if len(res) < 2 {
					continue
				}
				batchID := res[1]
				s.processRetry(ctx, batchID)
			}
		}
	}()
}

// p处理重试任务
func (s *TaskScheduler) processRetry(ctx context.Context, batchID string) {
	val, err := s.rdb.HGet(ctx, QueueKeyBackup, batchID).Result()
	if err != nil {
		if err == redis.Nil {
			return
		}
		log.Printf("[RetryHandler] 获取备份数据失败 (id=%s): %v", batchID, err)
		return
	}

	var data BackupData
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		log.Printf("[RetryHandler] 数据损坏 (id=%s): %v", batchID, err)
		if s.rdb.HDel(ctx, QueueKeyBackup, batchID).Err() != nil {
			log.Printf("[RetryHandler] 删除损坏数据失败 (id=%s)", batchID)
		}
		return
	}
	pipe := s.rdb.Pipeline()

	args := make([]interface{}, len(data.Data))
	for i, v := range data.Data {
		args[i] = v
	}

	if len(args) > 0 {
		pipe.LPush(ctx, QueueKeyPrimary, args...)
	}

	pipe.HDel(ctx, QueueKeyBackup, batchID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("[RetryHandler] 执行重试恢复失败 (id=%s): %v", batchID, err)
	} else {
		log.Printf("[RetryHandler] 成功恢复 BatchID: %s, 包含任务数: %d", batchID, len(data.Data))
	}
}
