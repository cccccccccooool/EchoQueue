-- EchoQueue dispatch. Pending is an immutable String with no ordinary TTL.
-- KEYS[1] source list supplied by the host
-- KEYS[2] pending snapshot
-- KEYS[3] namespace deadline zset
-- ARGV[1] batch size
-- ARGV[2] batch id
-- ARGV[3] snapshot base JSON
-- ARGV[4] visibility timeout in milliseconds
-- ARGV[5] max payload bytes
-- ARGV[6] max batch bytes
-- ARGV[7] maximum tasks per batch

local sourceKey = KEYS[1]
local pendingKey = KEYS[2]
local deadlineKey = KEYS[3]
local batchSize = tonumber(ARGV[1])
local batchID = ARGV[2]
local base = cjson.decode(ARGV[3])
local visibilityMs = tonumber(ARGV[4])
local maxPayloadBytes = tonumber(ARGV[5])
local maxBatchBytes = tonumber(ARGV[6])
local maxTasks = tonumber(ARGV[7])

local function validTaskID(value)
    if type(value) ~= 'string' or value == '' or string.match(value, '^%s*$') then
        return false
    end
    for i = 1, #value do
        local byte = string.byte(value, i)
        if byte == 0 or byte < 32 or byte == 127 then
            return false
        end
    end
    return true
end

if not batchSize or batchSize < 1 or batchSize > maxTasks then
    return {"invalid", "batch size is invalid"}
end
if not visibilityMs or visibilityMs < 1 then
    return {"invalid", "visibility timeout is invalid"}
end

local deadlineType = redis.call('TYPE', deadlineKey)['ok']
if deadlineType ~= 'none' and deadlineType ~= 'zset' then
    return {"invalid", "deadline key has wrong type"}
end

-- Peek the tail window read-only so validation failures leave the source
-- untouched; the window is popped only after it is known to be valid. The
-- list cannot change between LRANGE and RPOP because the script runs
-- atomically.
local rawItems = redis.call('LRANGE', sourceKey, -batchSize, -1)
if not rawItems or #rawItems == 0 then
    return {"empty"}
end

local now = redis.call('TIME')
local createdAt = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local deadlineAt = createdAt + visibilityMs
local tasks = {}
local seen = {}
local totalBytes = 0

for i, raw in ipairs(rawItems) do
    local ok, parsed = pcall(cjson.decode, raw)
    local task
    if ok and type(parsed) == 'table' and type(parsed.task_id) == 'string' and parsed.task_id ~= '' then
        task = parsed
        if task.retry_count == nil then task.retry_count = 0 end
        if task.payload == nil then
            return {"invalid", "task payload is missing"}
        end
        if not validTaskID(task.task_id) then
            return {"invalid", "task id is invalid"}
        end
    else
        local payload = raw
        if ok then payload = parsed end
        task = {
            task_id = batchID .. ":" .. tostring(i),
            retry_count = 0,
            payload = payload
        }
    end
    local retryCount = tonumber(task.retry_count)
    if retryCount == nil or retryCount < 0 or retryCount % 1 ~= 0 then
        return {"invalid", "task retry_count is invalid"}
    end
    if seen[task.task_id] then
        return {"invalid", "duplicate task_id in source batch"}
    end
    seen[task.task_id] = true
    local payloadBytes = #cjson.encode(task.payload)
    if payloadBytes > maxPayloadBytes then
        return {"invalid", "task payload exceeds limit"}
    end
    local encoded = cjson.encode({
        task_id = task.task_id,
        retry_count = retryCount,
        payload = task.payload
    })
    totalBytes = totalBytes + #encoded
    if totalBytes > maxBatchBytes then
        return {"invalid", "batch exceeds limit"}
    end
    table.insert(tasks, encoded)
end

base.schema_version = 1
base.protocol_version = 1
base.state = "PENDING"
base.batch_id = batchID
base.created_at = createdAt
base.deadline_at = deadlineAt
base.tasks = {}
for _, encoded in ipairs(tasks) do
    table.insert(base.tasks, cjson.decode(encoded))
end

local pendingJSON = cjson.encode(base)
if not redis.call('SET', pendingKey, pendingJSON, 'NX') then
    return {"invalid", "pending key already exists"}
end
-- Pop the validated tail window in one command (RPOP with count, Redis 6.2+).
redis.call('RPOP', sourceKey, batchSize)
redis.call('ZADD', deadlineKey, deadlineAt, batchID)

local result = {"applied", batchID, tostring(createdAt), tostring(deadlineAt), tostring(#tasks)}
for _, encoded in ipairs(tasks) do table.insert(result, encoded) end
return result
