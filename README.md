# EchoQueue

EchoQueue 是面向个人项目的轻量 Redis 批处理 Go 库。目标环境是 Redis 6.2+ standalone 或 replication primary；不支持 Redis Cluster、Streams、服务化和外部配置中心。

## 接入

当前 `go.mod` 仍使用占位 module path `example.com/m`。私有发布前必须替换为维护者确认的真实路径；本示例中的 import 也要同步替换。

```go
import (
	"context"
	"encoding/json"
	"time"

	echoqueue "example.com/m"
	"github.com/redis/go-redis/v9"
)

ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

cfg := echoqueue.DefaultConfig()
cfg.Namespace = "billing"
cfg.VisibilityTimeout = 30 * time.Second
cfg.ReceiptTTL = 24 * time.Hour
cfg.MaxRetry = 2

eq, err := echoqueue.New(rdb, cfg)
if err != nil {
	return err
}

queue, err := eq.Bind(echoqueue.QueueConfig{
	TaskName: "invoice",
	Source:   "billing:invoice:source",
	Result:   "billing:invoice:result",
	Dead:     "billing:invoice:dead",
})
if err != nil {
	return err
}

batch, err := queue.Dispatch(ctx, 1)
if err != nil {
	return err
}
if batch.ID != "" {
	receipt, err := eq.Settle(ctx, batch.ID, echoqueue.Outcome{
		RequestID: "worker-response-123",
		Results: []echoqueue.Result{{
			TaskID: batch.Tasks[0].TaskID,
			Data:   json.RawMessage(`{"ok":true}`),
		}},
	})
	_ = receipt
	_ = err
}
```

宿主项目负责将任务 JSON 放入 `QueueConfig.Source` List。任务应包含稳定的 `task_id`、`retry_count` 和 `payload`；没有 `task_id` 的原始 JSON 会在当前批次内生成 ID。业务副作用必须按 TaskID 做幂等处理。

超时恢复由 Scheduler 生命周期负责：

```go
runCtx, stop := context.WithCancel(context.Background())
defer stop()
if err := eq.Run(runCtx); err != nil {
	// Redis 能力检查或 Recover 批次错误可在这里观察到。
	return err
}
```

`Run` 每轮最多处理 `RunBatchSize` 个 deadline 候选。Recover 失败后会通过一个原子 Lua 操作把该候选的 deadline score 向后轮转，再继续同轮候选；因此多次受控 `Run` 调用可以跨窗口前进，且不会用无界扫描掩盖坏批次。错误仍会在本轮结束后返回，并包含首个失败批次 ID。

这里的 `defer` 只是恢复索引轮转，不是业务重试退避：它不修改 Pending，也不修改任务的 `RetryCount`，不会创建 Retry ZSet。宿主必须记录 `Run` 返回的错误，并按自己的生命周期策略修复后重新启动 `Run`；宿主停止调用时，恢复也会停止。

合法 Receipt、损坏 Receipt、孤儿 deadline（没有 Pending 和 Receipt）以及损坏 Pending 是不同状态。合法 Receipt 可以原子清理残留 deadline；孤儿 deadline 可以清理；损坏 Receipt 或损坏 Pending 必须保留证据，并只允许在仍有 Pending 时轮转索引。所有交付仍是 At-Least-Once，不是 Exactly Once，业务副作用必须按 TaskID 幂等。

## 配置约定

- 使用 `DefaultConfig()` 取得默认值，再修改 namespace、重试和安全上限。
- `MaxRetry=0` 表示不重试；必须同时设置 `MaxRetrySet=true`，以区别于“未指定、采用默认重试次数”。
- `ReceiptTTL` 默认为 24 小时，必须为至少 1ms 的有限正值。Pending 仍不设置普通 TTL。
- `Result`、`Dead` 为空时使用 namespace 内部 List。
- `Bind` 会拒绝重复 TaskName，以及跨 Queue 或同一 Queue 内 Source/Result/Dead 的有效物理 Redis key 冲突。

## 语义和限制

- Dispatch 原子保存不可变 Pending 快照，再移除 Source 任务。
- Settle 与 Recover 竞争同一个 Receipt 第一终态栅栏；第一个合法操作获胜。
- 相同 request ID 且 command hash 相同返回 `duplicate`；内容不同返回 `conflict`；超时恢复后迟到响应返回 `stale`。
- Result/Dead 持久化记录包含 TaskID 和 EffectID。交付语义是 At-Least-Once，不是 Exactly Once。
- Redis 能力检查按 Scheduler 缓存成功结果；首次失败不永久缓存，后续操作可以重试。只接受 Redis 6.2+ 且 Cluster 未启用。
- 普通失败重试当前立即回到 Source，不提供 Retry ZSet、退避调度、quarantine、events、reconcile 或完整观测平台。
- 不提供 YAML 主配置、V1/V2 双轨、复杂迁移、服务化、Web UI 或多租户。

## 验证

```powershell
go test ./... -count=1
go vet ./...
$env:ECHOQUEUE_REDIS_ADDR = "127.0.0.1:6380"
go test -tags=integration ./... -count=1
```

`integration` build tag 必须连接真实 Redis；测试不会用 mock 替代 Lua 或 Settle/Recover 竞态路径。Linux amd64 发布前还应补跑 race 测试。
