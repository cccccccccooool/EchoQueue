package scheduler

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// 通用基础设施
	QueueBackup = "queue:backup"      // 统一备份 Hash
	QueueDead   = "queue:dead_letter" // 死信队列

	// 任务超时配置
	TimePerTaskSeconds = 10
)

//go:embed redis.lua
var dispatchScript string

// BackupData 备份数据结构
type BackupData struct {
	Data        []string `json:"data"`
	Deadline    int64    `json:"deadline"`
	SourceQueue string   `json:"source_queue"`
}

// AckRequest 通用 ACK 请求结构
type AckRequest struct {
	BatchID    string            `json:"batch_id"`
	Status     string            `json:"status"`
	FailedIDs  []string          `json:"failed_ids"`
	ResultData []json.RawMessage `json:"result_data"`
}

// 任务调度器结构体
type TaskScheduler struct {
	rdb   *redis.Client
	tasks map[string]QueueConfig
}

// 创建新的调度器实例，从 envPath 加载任务配置
func NewScheduler(rdb *redis.Client, envPath string) (*TaskScheduler, error) {
	tasks, err := LoadConfig(envPath)
	if err != nil {
		return nil, fmt.Errorf("加载任务配置失败: %w", err)
	}

	return &TaskScheduler{
		rdb:   rdb,
		tasks: tasks,
	}, nil
}

// 返回所有已配置的任务名列表
func (s *TaskScheduler) GetTaskNames() []string {
	names := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		names = append(names, name)
	}
	return names
}

// 获取指定任务的配置（用于外部查询）
func (s *TaskScheduler) GetTaskConfig(taskName string) (QueueConfig, bool) {
	cfg, ok := s.tasks[taskName]
	return cfg, ok
}

//	通用调度函数：根据任务名从对应的源队列原子性地拉取任务并备份
//
// 参数: taskName 为 .env 中定义的任务名
// 返回: BatchID（用于后续 ACK），任务列表，错误
func (s *TaskScheduler) Dispatch(ctx context.Context, taskName string, batchSize int) (string, []string, error) {
	if batchSize <= 0 {
		return "", nil, errors.New("batchSize 必须大于 0")
	}
	if taskName == "" {
		return "", nil, errors.New("taskName 不能为空")
	}

	cfg, ok := s.tasks[taskName]
	if !ok {
		return "", nil, fmt.Errorf("未知的任务名: %s（请检查 .env 配置）", taskName)
	}

	batchID := uuid.New().String() + randomString(6)
	now := time.Now().Unix()

	// 执行 Lua 脚本：原子性地从源队列 RPOP 并 HSET 到 QueueBackup
	cmd := s.rdb.Eval(ctx, dispatchScript,
		[]string{cfg.SourceQueue, QueueBackup, QueueDead},
		batchSize, batchID, now, TimePerTaskSeconds,
	)

	result, err := cmd.Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("redis eval 错误: %w", err)
	}

	// 解析 Lua 返回的任务列表
	itemsInterface, ok := result.([]interface{})
	if !ok {
		if result == nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("lua 脚本返回类型异常")
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

// 生成指定长度的安全随机字符串（字母数字小写）
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			result[i] = letters[time.Now().UnixNano()%int64(len(letters))]
			continue
		}
		result[i] = letters[num.Int64()]
	}
	return string(result)
}

// Ack 确认任务完成：从备份中删除
func (s *TaskScheduler) Ack(ctx context.Context, batchID string) error {
	if batchID == "" {
		return errors.New("batchID 不能为空")
	}
	err := s.rdb.HDel(ctx, QueueBackup, batchID).Err()
	if err != nil {
		return fmt.Errorf("ACK 失败: %w", err)
	}
	return nil
}

// 通用重试计数增加方法
func (s *TaskScheduler) incrementTaskRetry(ctx context.Context, tasks []string) []interface{} {
	updatedTasks := make([]interface{}, 0, len(tasks))

	for _, taskJson := range tasks {
		var task map[string]interface{}
		if err := json.Unmarshal([]byte(taskJson), &task); err != nil {
			s.rdb.LPush(ctx, QueueDead, taskJson)
			continue
		}

		// 获取当前重试次数（默认0）
		retryCount := 0
		if rc, ok := task["__retry_count"].(float64); ok {
			retryCount = int(rc)
		}

		// 重试次数 +1
		retryCount++
		task["__retry_count"] = retryCount

		// 重新序列化
		newTaskJson, err := json.Marshal(task)
		if err != nil {
			continue
		}
		updatedTasks = append(updatedTasks, string(newTaskJson))
	}

	return updatedTasks
}

func (s *TaskScheduler) HandleResponse(ctx context.Context, taskName string, req AckRequest) error {
	if req.BatchID == "" {
		return errors.New("BatchID 不能为空")
	}

	cfg, ok := s.tasks[taskName]
	if !ok {
		return fmt.Errorf("未知的任务名: %s（请检查 .env 配置）", taskName)
	}

	// 1. 读取备份数据
	val, err := s.rdb.HGet(ctx, QueueBackup, req.BatchID).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("读取备份失败: %w", err)
	}

	pipe := s.rdb.Pipeline()

	// 2. 根据重试模式处理失败任务
	if val != "" {
		var backup BackupData
		if err := json.Unmarshal([]byte(val), &backup); err == nil {
			switch cfg.RetryMode {
			case 0:
				// 部分重试
				if len(req.FailedIDs) > 0 {
					failedSet := make(map[string]bool)
					for _, fid := range req.FailedIDs {
						failedSet[fid] = true
					}

					var failedTasksJson []string
					for _, taskJson := range backup.Data {
						var task map[string]interface{}
						if err := json.Unmarshal([]byte(taskJson), &task); err != nil {
							continue
						}
						var taskID string
						if id, ok := task["ticket_on_id"].(string); ok {
							taskID = id
						}
						if taskID != "" && failedSet[taskID] {
							failedTasksJson = append(failedTasksJson, taskJson)
						}
					}

					if len(failedTasksJson) > 0 {
						updatedTasks := s.incrementTaskRetry(ctx, failedTasksJson)
						if len(updatedTasks) > 0 {
							pipe.LPush(ctx, cfg.SourceQueue, updatedTasks...)
						}
					}
				}

			case 1:
				// 全量重试
				if req.Status == "fail" && len(backup.Data) > 0 {
					updatedTasks := s.incrementTaskRetry(ctx, backup.Data)
					if len(updatedTasks) > 0 {
						pipe.LPush(ctx, cfg.SourceQueue, updatedTasks...)
					}
				}
			}
		}
	}

	// 3. 处理成功的结果
	if cfg.ResultQueue != "" && len(req.ResultData) > 0 {
		shouldPush := true
		if cfg.RetryMode == 1 && req.Status != "success" {
			shouldPush = false
		}
		if shouldPush {
			results := make([]interface{}, len(req.ResultData))
			for i, r := range req.ResultData {
				results[i] = string(r)
			}
			pipe.LPush(ctx, cfg.ResultQueue, results...)
		}
	}

	// 4. 删除备份记录
	pipe.HDel(ctx, QueueBackup, req.BatchID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("执行管道操作失败: %w", err)
	}

	return nil
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

		vals, next, err := s.rdb.HScan(ctx, QueueBackup, cursor, "*", scanCount).Result()
		if err != nil {
			return
		}

		for i := 0; i+1 < len(vals); i += 2 {
			batchID := vals[i]
			val := vals[i+1]

			var data BackupData
			if err := json.Unmarshal([]byte(val), &data); err != nil {
				s.rdb.HDel(ctx, QueueBackup, batchID)
				continue
			}

			if data.SourceQueue == "" {
				s.rdb.LPush(ctx, QueueDead, val)
				s.rdb.HDel(ctx, QueueBackup, batchID)
				continue
			}

			// 延后处理过期任务
			if data.Deadline < now {
				updatedTasks := s.incrementTaskRetry(ctx, data.Data)
				if len(updatedTasks) > 0 {
					s.rdb.LPush(ctx, data.SourceQueue, updatedTasks...)
				}
				s.rdb.HDel(ctx, QueueBackup, batchID)
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}
}

// StartWatchDog 定期检查过期的 Batch 并触发重试
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
