package scheduler

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type QueueConfig struct {
	SourceQueue string // 监听的源队列名
	RetryMode   int    // 0=部分重试, 1=全量重试
	ResultQueue string // 成功结果推送目标队列
}

func LoadConfig(envPath string) (map[string]QueueConfig, error) {
	file, err := os.Open(envPath)
	if err != nil {
		return nil, fmt.Errorf("无法打开配置文件 %s: %w", envPath, err)
	}
	defer file.Close()

	configs := make(map[string]QueueConfig)
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("第 %d 行格式错误: %s", lineNum, line)
		}

		taskName := strings.TrimSpace(parts[0])
		valuePart := strings.TrimSpace(parts[1])

		if taskName == "" {
			return nil, fmt.Errorf("第 %d 行任务名为空", lineNum)
		}

		// 按逗号分割：源队列,重试模式,结果队列(可选)
		values := strings.Split(valuePart, ",")
		if len(values) < 2 {
			return nil, fmt.Errorf("第 %d 行格式错误(至少需要 源队列,重试模式): %s", lineNum, line)
		}

		sourceQueue := strings.TrimSpace(values[0])
		if sourceQueue == "" {
			return nil, fmt.Errorf("第 %d 行源队列名为空", lineNum)
		}

		retryMode, err := strconv.Atoi(strings.TrimSpace(values[1]))
		if err != nil || (retryMode != 0 && retryMode != 1) {
			return nil, fmt.Errorf("第 %d 行重试模式无效(应为0或1): %s", lineNum, values[1])
		}

		cfg := QueueConfig{
			SourceQueue: sourceQueue,
			RetryMode:   retryMode,
		}

		// 第三个字段为结果队列（可选）
		if len(values) >= 3 {
			cfg.ResultQueue = strings.TrimSpace(values[2])
		}

		configs[taskName] = cfg
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取配置文件时出错: %w", err)
	}

	if len(configs) == 0 {
		return nil, fmt.Errorf("配置文件为空，未定义任何任务")
	}

	return configs, nil
}
