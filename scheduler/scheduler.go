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
	QueueBackup = "queue:backup" // 统一备份 Hash

	QueueAIAnalyse  = "queue:ai_analyse"   // Go -> Python
	QueueAICallback = "queue:ai_call_back" // Python 结果 -> Go

	QueueKnowPending = "queue:knowledge_pending" // Step 1: Go -> Python
	QueueKnowSimilar = "queue:knowledge_similar" // Step 1 结果: Python -> Go
	QueueKnowPass    = "queue:knowledge_pass"    // Step 2: Go -> Python

	// 任务超时配置
	TimePerTaskSeconds = 10
)

//go:embed redis.lua
var dispatchScript string

type BackupData struct {
	Data        []string `json:"data"`
	Deadline    int64    `json:"deadline"`
	SourceQueue string   `json:"source_queue"` // 标识任务来源队列，用于故障恢复时重新入队
}

type AckRequest struct {
	BatchID    string            `json:"batch_id"`    // 批次 ID
	Status     string            `json:"status"`      // "success" 或 "fail"
	FailedIDs  []string          `json:"failed_ids"`  // 失败的任务 ID 列表（Pipeline 1 部分重试）
	ResultData []json.RawMessage `json:"result_data"` // 成功处理的结果数据
}

// 任务调度器结构体
type TaskScheduler struct {
	rdb *redis.Client
}

// 创建新的调度器实例
func NewScheduler(rdb *redis.Client) *TaskScheduler {
	return &TaskScheduler{
		rdb: rdb,
	}
}

//	通用调度函数：从指定队列原子性地拉取任务并备份
//
// 返回: BatchID（用于后续 ACK），任务列表，错误
func (s *TaskScheduler) Dispatch(ctx context.Context, queueName string, batchSize int) (string, []string, error) {
	if batchSize <= 0 {
		return "", nil, errors.New("batchSize 必须大于 0")
	}
	if queueName == "" {
		return "", nil, errors.New("queueName 不能为空")
	}

	batchID := uuid.New().String() + randomString(6)
	now := time.Now().Unix()

	// 执行 Lua 脚本：原子性地从 queueName RPOP 并 HSET 到 QueueBackup
	cmd := s.rdb.Eval(ctx, dispatchScript,
		[]string{queueName, QueueBackup, "queue:dead_letter"}, // KEYS[1]=源队列, KEYS[2]=备份Hash, KEYS[3]=死信队列
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
func (s *TaskScheduler) incrementTaskRetry(ctx context.Context, tasks []string, logPrefix string) []interface{} {
	updatedTasks := make([]interface{}, 0, len(tasks))

	for _, taskJson := range tasks {
		var task map[string]interface{}
		if err := json.Unmarshal([]byte(taskJson), &task); err != nil {
			s.rdb.LPush(ctx, "queue:dead_letter", taskJson)
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

// 处理 AI 分析任务的响应
func (s *TaskScheduler) HandleAIResponse(ctx context.Context, req AckRequest) error {
	if req.BatchID == "" {
		return errors.New("BatchID 不能为空")
	}

	// 1. 读取备份数据以便后续处理失败任务
	val, err := s.rdb.HGet(ctx, QueueBackup, req.BatchID).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("读取备份失败: %w", err)
	}

	pipe := s.rdb.Pipeline()

	if len(req.FailedIDs) > 0 && val != "" {
		var backup BackupData
		if err := json.Unmarshal([]byte(val), &backup); err == nil {
			failedSet := make(map[string]bool)
			for _, fid := range req.FailedIDs {
				failedSet[fid] = true
			}

			// 过滤出失败的任务
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
			var failedTasks []interface{}
			if len(failedTasksJson) > 0 {
				failedTasks = s.incrementTaskRetry(ctx, failedTasksJson, "HandleAI")
			}
			if len(failedTasks) > 0 {
				pipe.LPush(ctx, QueueAIAnalyse, failedTasks...)
			}
		}
	}

	// 3. 处理成功的结果：写入回调队列
	if len(req.ResultData) > 0 {
		results := make([]interface{}, len(req.ResultData))
		for i, r := range req.ResultData {
			results[i] = string(r)
		}
		pipe.LPush(ctx, QueueAICallback, results...)
	}

	// 4. 删除备份记录
	pipe.HDel(ctx, QueueBackup, req.BatchID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("执行管道操作失败: %w", err)
	}

	return nil
}

// 处理 knowledge_pending 队列的响应（Step 1）
func (s *TaskScheduler) HandleKnowPendingResponse(ctx context.Context, req AckRequest) error {
	if req.BatchID == "" {
		return errors.New("BatchID 不能为空")
	}

	val, err := s.rdb.HGet(ctx, QueueBackup, req.BatchID).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("读取备份失败: %w", err)
	}

	pipe := s.rdb.Pipeline()

	if req.Status == "fail" && val != "" {
		// 失败：全量重新入队，并增加重试计数
		var backup BackupData
		if err := json.Unmarshal([]byte(val), &backup); err == nil {
			if len(backup.Data) > 0 {
				updatedTasks := s.incrementTaskRetry(ctx, backup.Data, "HandleKnowPending")
				if len(updatedTasks) > 0 {
					pipe.LPush(ctx, QueueKnowPending, updatedTasks...)
				}
			}
		}
	} else if req.Status == "success" && len(req.ResultData) > 0 {
		// 成功：结果写入下一阶段队列
		results := make([]interface{}, len(req.ResultData))
		for i, r := range req.ResultData {
			results[i] = string(r)
		}
		pipe.LPush(ctx, QueueKnowSimilar, results...)
	}

	pipe.HDel(ctx, QueueBackup, req.BatchID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("执行管道操作失败: %w", err)
	}

	return nil
}

// 处理 knowledge_pass 队列的响应（Step 2，最终步骤）
func (s *TaskScheduler) HandleKnowPassResponse(ctx context.Context, req AckRequest) error {
	if req.BatchID == "" {
		return errors.New("BatchID 不能为空")
	}

	val, err := s.rdb.HGet(ctx, QueueBackup, req.BatchID).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("读取备份失败: %w", err)
	}

	pipe := s.rdb.Pipeline()

	if req.Status == "fail" && val != "" {
		// 失败：全量重新入队，并增加重试计数
		var backup BackupData
		if err := json.Unmarshal([]byte(val), &backup); err == nil {
			if len(backup.Data) > 0 {
				updatedTasks := s.incrementTaskRetry(ctx, backup.Data, "HandleKnowPass")
				if len(updatedTasks) > 0 {
					pipe.LPush(ctx, QueueKnowPass, updatedTasks...)
				}
			}
		}
	}

	// 无论成功失败都删除备份（成功则流程结束，失败已重入队）
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
				s.rdb.LPush(ctx, "queue:dead_letter", val)
				s.rdb.HDel(ctx, QueueBackup, batchID)
				continue
			}

			// 延后处理过期任务
			if data.Deadline < now {
				updatedTasks := s.incrementTaskRetry(ctx, data.Data, "WatchDog")
				if len(updatedTasks) > 0 {
					if err := s.rdb.LPush(ctx, data.SourceQueue, updatedTasks...).Err(); err != nil {
					} else {
					}
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

// 定期检查过期的 Batch 并触发重试
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
