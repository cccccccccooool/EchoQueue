-- EchoQueue settle. The receipt is the first-terminal-command fence.
-- KEYS[1] pending
-- KEYS[2] receipt
-- KEYS[3] namespace deadline zset
-- KEYS[4] result list
-- KEYS[5] source list for immediate retries
-- KEYS[6] dead list
-- ARGV[1] batch id
-- ARGV[2] request id
-- ARGV[3] trusted command hash
-- ARGV[4] receipt base JSON
-- ARGV[5] result records JSON
-- ARGV[6] retry tasks JSON
-- ARGV[7] dead records JSON
-- ARGV[8] receipt TTL in milliseconds

local pendingKey = KEYS[1]
local receiptKey = KEYS[2]
local deadlineKey = KEYS[3]
local resultKey = KEYS[4]
local sourceKey = KEYS[5]
local deadKey = KEYS[6]
local batchID = ARGV[1]
local requestID = ARGV[2]
local commandHash = ARGV[3]
local receiptTTL = tonumber(ARGV[8])

if not receiptTTL or receiptTTL < 1 then
    return {"invalid", "receipt ttl is invalid"}
end

local function validateReceipt(raw, expectedBatchID, expectedRequestID, expectedHash, expectedWinner, requireClosed)
    local ok, receipt = pcall(cjson.decode, raw)
    if not ok or type(receipt) ~= 'table' then
        return nil, "receipt is corrupt"
    end
    if receipt.schema_version ~= 1 or receipt.protocol_version ~= 1 then
        return nil, "receipt protocol is invalid"
    end
    if type(receipt.batch_id) ~= 'string' or receipt.batch_id ~= expectedBatchID then
        return nil, "receipt batch id is invalid"
    end
    if type(receipt.request_id) ~= 'string' or receipt.request_id == '' then
        return nil, "receipt request id is invalid"
    end
    if expectedRequestID and receipt.request_id ~= expectedRequestID then
        return nil, "receipt request id does not match command"
    end
    if type(receipt.command_hash) ~= 'string' or receipt.command_hash == '' then
        return nil, "receipt command hash is invalid"
    end
    if expectedHash and receipt.command_hash ~= expectedHash then
        return nil, "receipt command hash does not match command"
    end
    if expectedWinner then
        if receipt.winner ~= expectedWinner then
            return nil, "receipt winner is invalid"
        end
    elseif receipt.winner ~= 'settle' and receipt.winner ~= 'recover' then
        return nil, "receipt winner is invalid"
    end
    if type(receipt.closed_at) ~= 'number' or (requireClosed and receipt.closed_at <= 0) or (not requireClosed and receipt.closed_at < 0) then
        return nil, "receipt closed_at is invalid"
    end
    for _, field in ipairs({"result_count", "retry_count", "dead_count"}) do
        if type(receipt[field]) ~= 'number' or receipt[field] < 0 or receipt[field] % 1 ~= 0 then
            return nil, "receipt count is invalid"
        end
    end
    return receipt, nil
end

local receiptType = redis.call('TYPE', receiptKey)['ok']
if receiptType ~= 'none' and receiptType ~= 'string' then
    return {"invalid", "receipt key has wrong type"}
end
local existing = nil
if receiptType == 'string' then
    existing = redis.call('GET', receiptKey)
end
if existing then
    local parsed, receiptErr = validateReceipt(existing, batchID, nil, nil, nil, true)
    if not parsed then return {"invalid", receiptErr} end
    if parsed.request_id == requestID then
        if parsed.command_hash == commandHash then
            return {"duplicate", existing}
        end
        return {"conflict", existing}
    end
    return {"stale", existing}
end

local pendingRaw = redis.call('GET', pendingKey)
if not pendingRaw then
    return {"not_found"}
end
local pendingOK, pending = pcall(cjson.decode, pendingRaw)
if not pendingOK or type(pending) ~= 'table' then
    return {"invalid", "pending is corrupt"}
end
if pending.schema_version ~= 1 or pending.protocol_version ~= 1 or pending.state ~= "PENDING" or pending.batch_id ~= batchID then
    return {"invalid", "pending is not the requested open batch"}
end

local deadlineType = redis.call('TYPE', deadlineKey)['ok']
if deadlineType ~= 'none' and deadlineType ~= 'zset' then
    return {"invalid", "deadline key has wrong type"}
end

local resultOK, results = pcall(cjson.decode, ARGV[5])
local retryOK, retries = pcall(cjson.decode, ARGV[6])
local deadOK, dead = pcall(cjson.decode, ARGV[7])
if not resultOK or type(results) ~= 'table' or not retryOK or type(retries) ~= 'table' or not deadOK or type(dead) ~= 'table' then
    return {"invalid", "effect records are corrupt"}
end

local function checkList(key, needed)
    if needed == 0 then return true end
    local kind = redis.call('TYPE', key)['ok']
    return kind == 'none' or kind == 'list'
end
if not checkList(resultKey, #results) then return {"invalid", "result key has wrong type"} end
if not checkList(sourceKey, #retries) then return {"invalid", "source key has wrong type"} end
if not checkList(deadKey, #dead) then return {"invalid", "dead key has wrong type"} end

for _, record in ipairs(results) do
    if type(record) ~= 'table' or record.schema_version ~= 1 or record.protocol_version ~= 1 or not record.effect_id or not record.task_id then
        return {"invalid", "result record identity is missing"}
    end
end
for _, task in ipairs(retries) do
    if type(task) ~= 'table' or not task.task_id or task.retry_count == nil or task.payload == nil then
        return {"invalid", "retry task is malformed"}
    end
end
for _, record in ipairs(dead) do
    if type(record) ~= 'table' or record.schema_version ~= 1 or record.protocol_version ~= 1 or not record.effect_id or not record.task_id then
        return {"invalid", "dead record identity is missing"}
    end
end

local receipt, receiptErr = validateReceipt(ARGV[4], batchID, requestID, commandHash, 'settle', false)
if not receipt then return {"invalid", receiptErr} end
if receipt.result_count ~= #results or receipt.retry_count ~= #retries or receipt.dead_count ~= #dead then
    return {"invalid", "receipt counts do not match effects"}
end

for _, record in ipairs(results) do redis.call('RPUSH', resultKey, cjson.encode(record)) end
for _, task in ipairs(retries) do redis.call('RPUSH', sourceKey, cjson.encode(task)) end
for _, record in ipairs(dead) do redis.call('RPUSH', deadKey, cjson.encode(record)) end

local now = redis.call('TIME')
receipt.closed_at = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local receiptJSON = cjson.encode(receipt)
if not redis.call('SET', receiptKey, receiptJSON, 'NX', 'PX', receiptTTL) then
    local raced = redis.call('GET', receiptKey)
    if raced then return {"stale", raced} end
    return {"invalid", "receipt could not be stored"}
end

redis.call('ZREM', deadlineKey, batchID)
redis.call('DEL', pendingKey)
return {"applied", receiptJSON}
