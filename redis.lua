-- 全原子调度脚本：出队 → 封装 TaskEnvelope → 备份 → 注册超时哨兵
-- 所有写操作在同一次 EVAL 中完成，确保原子性。
--
-- KEYS[1]: sourceQueue     来源队列
-- KEYS[2]: backupZSet      超时哨兵 Sorted Set
-- KEYS[3]: dataKey         备份数据的 String Key (queue:backup:data:{batchID})
--
-- ARGV[1]: batchSize       批量大小
-- ARGV[2]: batchID         批次 ID（Go 端 UUID）
-- ARGV[3]: now             当前时间戳（Unix 秒）
-- ARGV[4]: timeoutPerTask  每个任务的超时秒数
-- ARGV[5]: sourceQueueName 来源队列名（写入 BackupEntry 用于故障恢复）
-- ARGV[6]: maxRetry        最大重试次数

local sourceQueue    = KEYS[1]
local backupZSet     = KEYS[2]
local dataKey        = KEYS[3]
local batchSize      = tonumber(ARGV[1])
local batchID        = ARGV[2]
local now            = tonumber(ARGV[3])
local timeoutPerTask = tonumber(ARGV[4])
local srcQueueName   = ARGV[5]
local maxRetry       = tonumber(ARGV[6])

local tasks = redis.call('RPOP', sourceQueue, batchSize)

if (not tasks) or (#tasks == 0) then
    return nil
end

local count = #tasks

local envelopes = {}
for i, raw in ipairs(tasks) do
    local ok, parsed = pcall(cjson.decode, raw)
    if ok and type(parsed) == "table" and parsed.task_id and parsed.task_id ~= "" then
        envelopes[i] = raw
    else
        local payload
        if ok then
            payload = parsed
        else
            payload = raw
        end
        envelopes[i] = cjson.encode({
            task_id     = batchID .. ":" .. tostring(i),
            retry_count = 0,
            payload     = payload
        })
    end
end

-- Step 3: 组装 BackupEntry
local backupTasks = {}
for i, envJson in ipairs(envelopes) do
    backupTasks[i] = cjson.decode(envJson)
end

local backupEntry = cjson.encode({
    tasks        = backupTasks,
    source_queue = srcQueueName,
    max_retry    = maxRetry
})

-- Step 4: 计算 deadline 和 TTL
local deadline   = now + (count * timeoutPerTask)
local ttlSeconds = (count * timeoutPerTask) + 3600

-- Step 5: 原子写入备份数据 + 注册超时哨兵
redis.call('SET', dataKey, backupEntry, 'EX', ttlSeconds)
redis.call('ZADD', backupZSet, deadline, batchID)

return envelopes
