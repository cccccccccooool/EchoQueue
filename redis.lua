-- KEYS[1]: sourceQueue  KEYS[2]: backupZSet
-- ARGV[1]: batchSize  ARGV[2]: batchID  ARGV[3]: now  ARGV[4]: timeoutPerTask
-- 说明：此脚本只负责原子地「出队 + 注册超时哨兵」，
--       dataKey 的写入由 Go 端在脚本返回后统一完成，避免二次覆写。
local sourceQueue    = KEYS[1]
local backupZSet     = KEYS[2]
local batchSize      = tonumber(ARGV[1])
local batchID        = ARGV[2]
local now            = tonumber(ARGV[3])
local timeoutPerTask = tonumber(ARGV[4])

local tasks = redis.call('RPOP', sourceQueue, batchSize)

if (not tasks) or (#tasks == 0) then
    return nil
end

local count    = #tasks
local deadline = now + (count * timeoutPerTask)

redis.call('ZADD', backupZSet, deadline, batchID)

return tasks
