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
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	//备份队列
	QueueBackupZSet    = "queue:backup"
	QueueBackupDataFmt = "queue:backup:data:%s"
	//死信队列
	QueueDead = "queue:dead_letter"
)

// 统一的任务封装格式
type TaskEnvelope struct {
	TaskID     string          `json:"task_id"`     // 唯一 UUID
	RetryCount int             `json:"retry_count"` // 记录重试次数
	Payload    json.RawMessage `json:"payload"`     // 用原始数据
}

// 批次备份数据
type BackupEntry struct {
	Tasks       []TaskEnvelope `json:"tasks"`
	SourceQueue string         `json:"source_queue"` // 任务来源队列
	MaxRetry    int            `json:"max_retry"`    // 最大重试次数
}

type AckRequest struct {
	BatchID    string            `json:"batch_id"`    // 批次 ID
	Status     string            `json:"status"`      // 任务状态：success 或 fail
	FailedIDs  []string          `json:"failed_ids"`  // 失败的任务 ID 列表
	ResultData []json.RawMessage `json:"result_data"` // 成功处理的结果数据
}

//go:embed redis.lua
var dispatchScript string

// 任务调度器
type TaskScheduler struct {
	rdb   *redis.Client
	tasks map[string]QueueConfig
}

// 创建新的调度器实例
func NewScheduler(rdb *redis.Client, configPath string) (*TaskScheduler, error) {
	tasks, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("载入任务配置失败: %w", err)
	}
	return &TaskScheduler{rdb: rdb, tasks: tasks}, nil
}

// 返回所有已设置的任务名称
func (s *TaskScheduler) GetTaskNames() []string {
	names := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		names = append(names, name)
	}
	return names
}

// 获取指定任务的配置
func (s *TaskScheduler) GetTaskConfig(taskName string) (QueueConfig, bool) {
	cfg, ok := s.tasks[taskName]
	return cfg, ok
}

// 通用调度函数
func (s *TaskScheduler) Dispatch(ctx context.Context, taskName string, batchSize int) (string, []TaskEnvelope, error) {
	if batchSize <= 0 {
		return "", nil, errors.New("batchSize 必须大于 0")
	}
	if taskName == "" {
		return "", nil, errors.New("taskName 不能为空")
	}

	cfg, ok := s.tasks[taskName]
	if !ok {
		return "", nil, fmt.Errorf("未知的任务名: %s（请检查 config.yaml）", taskName)
	}

	batchID := uuid.New().String() + randomString(6)
	now := time.Now().Unix()
	dataKey := fmt.Sprintf(QueueBackupDataFmt, batchID)

	// 执行 Lua 脚本
	cmd := s.rdb.Eval(ctx, dispatchScript,
		[]string{cfg.SourceQueue, QueueBackupZSet, QueueDead},
		batchSize, batchID, now, cfg.Timeout, dataKey, cfg.MaxRetry,
	)
	result, err := cmd.Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("redis eval 错误: %w", err)
	}
	itemsInterface, ok := result.([]interface{})
	if !ok || result == nil {
		return "", nil, nil
	}

	// 将原始任务数据封装为 TaskEnvelope
	envelopes := make([]TaskEnvelope, 0, len(itemsInterface))
	for _, item := range itemsInterface {
		raw, ok := item.(string)
		if !ok {
			raw = fmt.Sprintf("%v", item)
		}
		envelopes = append(envelopes, wrapOrParseEnvelope(raw))
	}
	deadline := now + int64(len(envelopes)*cfg.Timeout)
	ttl := time.Duration(deadline-now+3600) * time.Second
	entry := BackupEntry{
		Tasks:       envelopes,
		SourceQueue: cfg.SourceQueue,
		MaxRetry:    cfg.MaxRetry,
	}
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return "", nil, fmt.Errorf("序列化备份资料失败: %w", err)
	}
	if err := s.rdb.Set(ctx, dataKey, string(entryJSON), ttl).Err(); err != nil {
		return "", nil, fmt.Errorf("储存备份资料失败: %w", err)
	}

	return batchID, envelopes, nil
}

// 解析原始字符串。
// 若已是合法的 TaskEnvelope（含有非空 task_id）则直接返回；
// 否则视为用户的原始 Payload，生成新的 TaskEnvelope 进行封装。
func wrapOrParseEnvelope(raw string) TaskEnvelope {
	var env TaskEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err == nil && env.TaskID != "" {
		return env // 重试回流任务，保留原有封装
	}
	return TaskEnvelope{
		TaskID:     uuid.New().String(),
		RetryCount: 0,
		Payload:    json.RawMessage(raw),
	}
}

// 生成BatchID
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

// 确认任务批次完成
func (s *TaskScheduler) Ack(ctx context.Context, batchID string) error {
	if batchID == "" {
		return errors.New("batchID 不能为空")
	}
	pipe := s.rdb.Pipeline()
	pipe.ZRem(ctx, QueueBackupZSet, batchID)
	pipe.Del(ctx, fmt.Sprintf(QueueBackupDataFmt, batchID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("ACK 失败: %w", err)
	}
	return nil
}

// 记录任务重试次数
func (s *TaskScheduler) retryEnvelopes(ctx context.Context, envelopes []TaskEnvelope, maxRetry int) []TaskEnvelope {
	result := make([]TaskEnvelope, 0, len(envelopes))
	for _, env := range envelopes {
		env.RetryCount++
		if env.RetryCount > maxRetry {
			data, _ := json.Marshal(env)
			s.rdb.LPush(ctx, QueueDead, string(data))
			continue
		}
		result = append(result, env)
	}
	return result
}

// 通用响应处理方法
// 成功的结果数据会推入设置的 ResultQueue
func (s *TaskScheduler) HandleResponse(ctx context.Context, taskName string, req AckRequest) error {
	if req.BatchID == "" {
		return errors.New("BatchID 不能为空")
	}

	cfg, ok := s.tasks[taskName]
	if !ok {
		return fmt.Errorf("未知的任务名: %s（请检	查 config.yaml）", taskName)
	}

	// 1. 从备份读取批次数据
	dataKey := fmt.Sprintf(QueueBackupDataFmt, req.BatchID)
	val, err := s.rdb.Get(ctx, dataKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("读取备份失败: %w", err)
	}

	pipe := s.rdb.Pipeline()

	// 2. 根据重试模式处理失败任务
	// 模式 0（部分重试）
	// 模式 1（全量重试）
	if val != "" {
		var entry BackupEntry
		if err := json.Unmarshal([]byte(val), &entry); err == nil {
			maxRetry := entry.MaxRetry
			if maxRetry <= 0 {
				maxRetry = cfg.MaxRetry
			}

			switch cfg.RetryMode {
			case 0:
				if len(req.FailedIDs) > 0 {
					failedSet := make(map[string]bool, len(req.FailedIDs))
					for _, fid := range req.FailedIDs {
						failedSet[fid] = true
					}

					var failedEnvelopes []TaskEnvelope
					for _, env := range entry.Tasks {
						if failedSet[env.TaskID] {
							failedEnvelopes = append(failedEnvelopes, env)
						}
					}

					if len(failedEnvelopes) > 0 {
						retried := s.retryEnvelopes(ctx, failedEnvelopes, maxRetry)
						for _, env := range retried {
							data, _ := json.Marshal(env)
							pipe.LPush(ctx, cfg.SourceQueue, string(data))
						}
					}
				}

			case 1:
				if req.Status == "fail" && len(entry.Tasks) > 0 {
					retried := s.retryEnvelopes(ctx, entry.Tasks, maxRetry)
					for _, env := range retried {
						data, _ := json.Marshal(env)
						pipe.LPush(ctx, cfg.SourceQueue, string(data))
					}
				}
			}
		}
	}

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
	pipe.ZRem(ctx, QueueBackupZSet, req.BatchID)
	pipe.Del(ctx, dataKey)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("執行管道操作失敗: %w", err)
	}

	return nil
}

// 检查是否有超时的批次，并根据备份数据触发重试
func (s *TaskScheduler) checkTimeouts(ctx context.Context) {
	now := time.Now().Unix()

	expiredIDs, err := s.rdb.ZRangeByScore(ctx, QueueBackupZSet, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil {
		log.Printf("[WatchDog] ZRANGEBYSCORE 错误: %v", err)
		return
	}

	for _, batchID := range expiredIDs {
		if ctx.Err() != nil {
			return
		}

		dataKey := fmt.Sprintf(QueueBackupDataFmt, batchID)
		val, err := s.rdb.Get(ctx, dataKey).Result()
		if err == redis.Nil {
			s.rdb.ZRem(ctx, QueueBackupZSet, batchID)
			continue
		}
		if err != nil {
			log.Printf("[WatchDog] 读取备份数据失败 batchID=%s: %v", batchID, err)
			continue
		}

		var entry BackupEntry
		if err := json.Unmarshal([]byte(val), &entry); err != nil {
			log.Printf("[WatchDog] 解析备份数据失败 batchID=%s: %v", batchID, err)
			pipe := s.rdb.Pipeline()
			pipe.ZRem(ctx, QueueBackupZSet, batchID)
			pipe.Del(ctx, dataKey)
			pipe.Exec(ctx) //nolint:errcheck
			continue
		}

		if entry.SourceQueue == "" {
			log.Printf("[WatchDog] 备份数据缺少 source_queue，送入死信 batchID=%s", batchID)
			data, _ := json.Marshal(entry)
			s.rdb.LPush(ctx, QueueDead, string(data))
			pipe := s.rdb.Pipeline()
			pipe.ZRem(ctx, QueueBackupZSet, batchID)
			pipe.Del(ctx, dataKey)
			pipe.Exec(ctx) //nolint:errcheck
			continue
		}

		// 重试超时任务
		maxRetry := entry.MaxRetry
		if maxRetry <= 0 {
			maxRetry = 3
		}
		retried := s.retryEnvelopes(ctx, entry.Tasks, maxRetry)
		if len(retried) > 0 {
			args := make([]interface{}, len(retried))
			for i, env := range retried {
				data, _ := json.Marshal(env)
				args[i] = string(data)
			}
			s.rdb.LPush(ctx, entry.SourceQueue, args...)
		}

		// 清理备份记录
		pipe := s.rdb.Pipeline()
		pipe.ZRem(ctx, QueueBackupZSet, batchID)
		pipe.Del(ctx, dataKey)
		pipe.Exec(ctx) //nolint:errcheck
	}
}

// 定期检查超时的批次并触发重试
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
