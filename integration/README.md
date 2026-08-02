# EchoQueue 真实 Redis 测试

本目录的测试全部带 `integration` build tag，必须连接真实 Redis 6.2+ standalone 或 replication primary；不会用 mock 替代 Lua 和竞态路径。

```powershell
$env:ECHOQUEUE_REDIS_ADDR = "127.0.0.1:6380"
go test -tags=integration ./...
```

测试只使用带随机后缀的 source/result/dead key，并在结束时删除自身控制 key。库的普通单元测试不连接 Redis：

```powershell
go test ./...
go vet ./...
```
