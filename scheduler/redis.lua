local queueKey = KEYS[1]
local backupKey = KEYS[2]
local batchSize = tonumber(ARGV[1])
local batchID = ARGV[2]
local now = tonumber(ARGV[3])
local timeoutPerTask = tonumber(ARGV[4])

-- 从右侧弹出 batchSize 个元素
local tasks = redis.call('RPOP', queueKey, batchSize)

if (not tasks) or (#tasks == 0) then
    return nil
end

local count = #tasks
local deadline = now + (count * timeoutPerTask)

local payload = {
    data = tasks,
    deadline = deadline
}
local payloadJson = cjson.encode(payload)

-- 将备份数据写入 Hash
redis.call('HSET', backupKey, batchID, payloadJson)

return tasks