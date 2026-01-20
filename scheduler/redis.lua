-- 通用调度 Lua 脚本：从指定队列取出数据并备份到统一 Hash
-- KEYS[1]: 源队列名 (如 queue:ai_analyse)
-- KEYS[2]: 备份 Hash Key (固定为 queue:backup)
-- KEYS[3]: 死信队列 Key (queue:dead_letter)
-- ARGV[1]: 批量大小
-- ARGV[2]: BatchID (UUID)
-- ARGV[3]: 当前时间戳 (Unix Seconds)
-- ARGV[4]: 每个任务超时时间 (Seconds)

local sourceQueue = KEYS[1]
local backupKey = KEYS[2]
local deadLetterQueue = KEYS[3]
local batchSize = tonumber(ARGV[1])
local batchID = ARGV[2]
local now = tonumber(ARGV[3])
local timeoutPerTask = tonumber(ARGV[4])

local rawTasks = redis.call('RPOP', sourceQueue, batchSize)

if (not rawTasks) or (#rawTasks == 0) then
    return nil
end

local validTasks = {}
local deadTasks = 0

for i, taskJson in ipairs(rawTasks) do
    local ok, task = pcall(cjson.decode, taskJson)
    
    if ok and task then
        local retryCount = task.__retry_count or 0
        
        -- 如果已重试3次，直接移入死信队列
        if retryCount >= 3 then
            redis.call('LPUSH', deadLetterQueue, taskJson)
            deadTasks = deadTasks + 1
        else
            table.insert(validTasks, taskJson)
        end
    else
        redis.call('LPUSH', deadLetterQueue, taskJson)
        deadTasks = deadTasks + 1
    end
end

if #validTasks == 0 then
    return nil
end

local count = #validTasks
local deadline = now + (count * timeoutPerTask)

local payload = {
    data = validTasks,
    deadline = deadline,
    source_queue = sourceQueue
}
local payloadJson = cjson.encode(payload)

-- 将备份数据写入统一备份 Hash
redis.call('HSET', backupKey, batchID, payloadJson)

return validTasks
