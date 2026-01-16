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

// 常量定义
const (
	// QueueKeyPrimary 主队列 (List)
	QueueKeyPrimary = "ai_analyse"
	// QueueKeyBackup 备份表 (Hash), Key=BatchID, Value=BackupData JSON
	QueueKeyBackup = "ai_analyse_backup"
	// QueueKeyRetrySignal 重试信号队列 (List), 存储过期的 BatchID
	QueueKeyRetrySignal = "ai_analyse_retry_signal"

	// TimePerTaskSeconds 每个任务预估处理时间 (秒)
	TimePerTaskSeconds = 10
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

// BackupData 存储在 Redis 备份 Hash 中的结构
type BackupData struct {
	Data     []string `json:"data"`
	Deadline int64    `json:"deadline"` // Unix timestamp
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

	// 执行 Lua 脚本
	cmd := s.rdb.Eval(ctx, dispatchScript,
		[]string{QueueKeyPrimary, QueueKeyBackup}, // KEYS
		batchSize, batchID, now, TimePerTaskSeconds, // ARGV
	)

	result, err := cmd.Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil, nil // 队列为空
		}
		return "", nil, fmt.Errorf("redis eval error: %w", err)
	}

	// 转换结果为 []string
	// Redis Lua 返回的 table 会被 go-redis 转换为 []interface{}
	itemsInterface, ok := result.([]interface{})
	if !ok {
		// 假如返回 nil (脚本中 queue 为空时)
		if result == nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("unexpected return type from lua script")
	}

	tasks := make([]string, 0, len(itemsInterface))
	for _, item := range itemsInterface {
		str, ok := item.(string)
		if !ok {
			// 在某些 Redis 客户端版本中可能是 byte slice?
			// go-redis v9 通常处理得很好，但为了保险起见可以转字符串
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
	// HDEL 删除指定 BatchID
	// 无论删除是否成功（可能因为超时已经被重试逻辑删除了），ACK 操作本身可以视为幂等
	err := s.rdb.HDel(ctx, QueueKeyBackup, batchID).Err()
	if err != nil {
		return fmt.Errorf("ack failed: %w", err)
	}
	return nil
}

// StartWatchDog 启动看门狗: 定期检查过期的 Batch
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

// checkTimeouts 检查超时逻辑
func (s *TaskScheduler) checkTimeouts(ctx context.Context) {
	// 使用 HSCAN 分页遍历备份 Hash，避免 HGETALL 在数据量大时阻塞
	now := time.Now().Unix()
	var cursor uint64 = 0
	const scanCount int64 = 100 // 每次扫描数量，可根据负载调整

	for {
		// 响应上下文取消
		if ctx.Err() != nil {
			return
		}

		vals, next, err := s.rdb.HScan(ctx, QueueKeyBackup, cursor, "*", scanCount).Result()
		if err != nil {
			log.Printf("[WatchDog] HSCAN 读取 backup hash 失败: %v", err)
			return
		}

		// vals 格式: [field1, value1, field2, value2, ...]
		for i := 0; i+1 < len(vals); i += 2 {
			batchID := vals[i]
			val := vals[i+1]

			var data BackupData
			if err := json.Unmarshal([]byte(val), &data); err != nil {
				log.Printf("[WatchDog] 解析 JSON 失败 (batchID=%s): %v", batchID, err)
				continue
			}

			if data.Deadline < now {
				log.Printf("[WatchDog] 发现过期 BatchID: %s", batchID)
				// 1. 先推送信号
				if err := s.rdb.LPush(ctx, QueueKeyRetrySignal, batchID).Err(); err != nil {
					log.Printf("[WatchDog] 推送重试信号失败: %v", err)
				} else {
					newDeadline := now + 300 // 5分钟后再次检查
					data.Deadline = newDeadline
					// 重新序列化并更新回 Redis
					if newData, err := json.Marshal(data); err == nil {
						// 这里不需要原子性，因为只是更新时间
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

// StartRetryHandler 启动重试消费者: 监听重试信号并恢复任务
func (s *TaskScheduler) StartRetryHandler(ctx context.Context) {
	log.Println("[RetryHandler] 已启动，等待重试信号...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("[RetryHandler] 停止运行")
				return
			default:
				// 阻塞式等待信号，超时设置为 5 秒以便能响应 ctx.Done
				// BRPop 返回 [key, value]，对应 list 的尾部弹出 (配合 LPush 实现 FIFO)
				res, err := s.rdb.BRPop(ctx, 5*time.Second, QueueKeyRetrySignal).Result()
				if err != nil {
					if err == redis.Nil {
						continue // 超时无数据，继续循环
					}
					// 上下文取消或其他错误
					if !errors.Is(err, context.Canceled) {
						log.Printf("[RetryHandler] BPOP 错误: %v", err)
						time.Sleep(1 * time.Second) // 避免错误循环过快
					}
					continue
				}

				if len(res) < 2 {
					continue
				}

				// res[0] 是 key, res[1] 是 value (即 batchID)
				batchID := res[1]
				s.processRetry(ctx, batchID)
			}
		}
	}()
}

// processRetry 处理单个重试任务
func (s *TaskScheduler) processRetry(ctx context.Context, batchID string) {
	// 1. 根据 ID 从备份中读取原始数据
	val, err := s.rdb.HGet(ctx, QueueKeyBackup, batchID).Result()
	if err != nil {
		if err == redis.Nil {
			// 可能已经被 ACK 了，或者被其他 RetryHandler 处理了，忽略
			return
		}
		log.Printf("[RetryHandler] 获取备份数据失败 (id=%s): %v", batchID, err)
		return
	}

	var data BackupData
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		log.Printf("[RetryHandler] 数据损坏 (id=%s): %v", batchID, err)
		// 也可以选择删掉坏数据
		if s.rdb.HDel(ctx, QueueKeyBackup, batchID).Err() != nil {
			log.Printf("[RetryHandler] 删除损坏数据失败 (id=%s)", batchID)
		}
		return
	}

	// 2. 将数据重新推回主队列 (LPUSH - Re-queue 到队头，优先处理)
	// pipeline 保证一定的原子性，或者分步执行
	pipe := s.rdb.Pipeline()

	// 注意：data.Data 是 []string
	// LPUSH 支持 interface{} 变长参数
	args := make([]interface{}, len(data.Data))
	for i, v := range data.Data {
		args[i] = v
	}

	if len(args) > 0 {
		pipe.LPush(ctx, QueueKeyPrimary, args...)
	}

	// 3. 删除备份记录
	pipe.HDel(ctx, QueueKeyBackup, batchID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("[RetryHandler] 执行重试恢复失败 (id=%s): %v", batchID, err)
	} else {
		log.Printf("[RetryHandler] 成功恢复 BatchID: %s, 包含任务数: %d", batchID, len(data.Data))
	}
}
