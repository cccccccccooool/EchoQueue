local sourceQueue    = KEYS[1]
local backupZSet     = KEYS[2]
local batchSize      = tonumber(ARGV[1])
local batchID        = ARGV[2]
local now            = tonumber(ARGV[3])
local timeoutPerTask = tonumber(ARGV[4])
local dataKey        = ARGV[5]

local tasks = redis.call('RPOP', sourceQueue, batchSize)

if (not tasks) or (#tasks == 0) then
    return nil
end

local count    = #tasks
local deadline = now + (count * timeoutPerTask)
local ttl      = (deadline - now) + 3600  

local rawPayload = cjson.encode({ tasks = tasks, source_queue = sourceQueue })
redis.call('SET', dataKey, rawPayload, 'EX', ttl)

redis.call('ZADD', backupZSet, deadline, batchID)

return tasks
