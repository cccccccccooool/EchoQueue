package scheduler

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// 单一队列的设置
type QueueConfig struct {
	SourceQueue string `yaml:"source_queue"` // 监听的来源队列名称
	RetryMode   int    `yaml:"retry_mode"`   // 0=部分重试, 1=全量重试
	ResultQueue string `yaml:"result_queue"` // 成功结果推送目标队列（空字串=终端节点）
	Timeout     int    `yaml:"timeout"`      // 每个任务的超时秒数（默认 30）
	MaxRetry    int    `yaml:"max_retry"`    // 最大重试次数（默认 3）
}

// rootConfig YAML 配置文件根节点
type rootConfig struct {
	Queues map[string]QueueConfig `yaml:"queues"`
}

func LoadConfig(configPath string) (map[string]QueueConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("无法读取配置文件 %s: %w", configPath, err)
	}

	var root rootConfig
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("解析 YAML 配置失败: %w", err)
	}

	if len(root.Queues) == 0 {
		return nil, fmt.Errorf("配置文件未定义任何队列（queues 为空）")
	}

	for name, cfg := range root.Queues {
		if cfg.SourceQueue == "" {
			return nil, fmt.Errorf("队列 %q 的 source_queue 不能为空", name)
		}
		if cfg.RetryMode != 0 && cfg.RetryMode != 1 {
			return nil, fmt.Errorf("队列 %q 的 retry_mode 必须为 0 或 1", name)
		}
		if cfg.Timeout <= 0 {
			cfg.Timeout = 30 // 默认每任务 30 秒超时
		}
		if cfg.MaxRetry <= 0 {
			cfg.MaxRetry = 3 // 默认最多重试 3 次
		}
		root.Queues[name] = cfg
	}

	return root.Queues, nil
}
