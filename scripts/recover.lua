-- Recover one expired batch atomically.
-- KEYS[1] pending, KEYS[2] receipt, KEYS[3] deadline
-- KEYS[4] source/result, KEYS[5] dead-letter/result
-- ARGV[1] batch id, ARGV[2] request id, ARGV[3] command hash,
-- ARGV[4] receipt base JSON, ARGV[5] retry effects JSON, ARGV[6] dead effects JSON,
-- ARGV[7] receipt ttl milliseconds.
local pendingKey = KEYS[1]
local receiptKey = KEYS[2]
local deadlineKey = KEYS[3]
local sourceKey = KEYS[4]
local deadKey = KEYS[5]

local batchID = ARGV[1]
local requestID = ARGV[2]
local commandHash = ARGV[3]
local receiptTTL = tonumber(ARGV[7])
if not receiptTTL or receiptTTL < 1 or receiptTTL % 1 ~= 0 then
  return {'invalid', 'invalid receipt ttl'}
end

local function validateReceipt(raw, expectedBatchID, expectedRequestID, expectedHash, expectedWinner, requireClosed)
  local ok, value = pcall(cjson.decode, raw or '')
  if not ok or type(value) ~= 'table' then
    return nil, 'invalid receipt json'
  end
  if value.schema_version ~= 1 then
    return nil, 'invalid receipt schema'
  end
  if value.protocol_version ~= 1 then
    return nil, 'invalid receipt protocol'
  end
  if value.batch_id ~= expectedBatchID then
    return nil, 'invalid receipt batch'
  end
  if type(value.request_id) ~= 'string' or value.request_id == '' then
    return nil, 'invalid receipt request'
  end
  if type(value.command_hash) ~= 'string' or value.command_hash == '' then
    return nil, 'invalid receipt command hash'
  end
  if expectedHash and value.command_hash ~= expectedHash then
    return nil, 'invalid receipt command hash'
  end
  if value.winner ~= 'settle' and value.winner ~= 'recover' then
    return nil, 'invalid receipt winner'
  end
  if expectedRequestID and value.request_id ~= expectedRequestID then
    return nil, 'invalid receipt request'
  end
  if expectedWinner and value.winner ~= expectedWinner then
    return nil, 'invalid receipt winner'
  end
  if requireClosed then
    if type(value.closed_at) ~= 'number' or value.closed_at <= 0 or value.closed_at % 1 ~= 0 then
      return nil, 'invalid receipt closed_at'
    end
  elseif value.closed_at ~= nil and (type(value.closed_at) ~= 'number' or value.closed_at < 0 or value.closed_at % 1 ~= 0) then
    return nil, 'invalid receipt closed_at'
  end
  if type(value.result_count) ~= 'number' or value.result_count < 0 or value.result_count % 1 ~= 0 then
    return nil, 'invalid receipt result_count'
  end
  if type(value.retry_count) ~= 'number' or value.retry_count < 0 or value.retry_count % 1 ~= 0 then
    return nil, 'invalid receipt retry_count'
  end
  if type(value.dead_count) ~= 'number' or value.dead_count < 0 or value.dead_count % 1 ~= 0 then
    return nil, 'invalid receipt dead_count'
  end
  return value, nil
end

local function invalidReceiptType(kind)
  return kind == 'none' or kind == 'string'
end

local function checkList(key, count)
  if count <= 0 then
    return true
  end
  local kind = redis.call('TYPE', key)['ok']
  return kind == 'none' or kind == 'list'
end

local function checkEffects(items, expectedKind)
  for _, item in ipairs(items) do
    if type(item) ~= 'table' or type(item.task_id) ~= 'string' or item.task_id == '' then
      return false
    end
    if expectedKind == 'retry' then
      if type(item.retry_count) ~= 'number' or item.retry_count < 0 or item.retry_count % 1 ~= 0 or item.payload == nil then
        return false
      end
    else
      if item.schema_version ~= 1 or item.protocol_version ~= 1 then
        return false
      end
      if type(item.effect_id) ~= 'string' or item.effect_id == '' then
        return false
      end
      if type(item.batch_id) ~= 'string' or item.batch_id == '' or item.batch_id ~= batchID then
        return false
      end
      if type(item.retry_count) ~= 'number' or item.retry_count < 0 or item.retry_count % 1 ~= 0 or item.payload == nil then
        return false
      end
    end
  end
  return true
end

-- Validate all caller-supplied JSON and target types before any write.
local receipt, receiptErr = validateReceipt(ARGV[4], batchID, requestID, commandHash, 'recover', false)
if not receipt then
  return {'invalid', receiptErr}
end

local okRetries, retries = pcall(cjson.decode, ARGV[5] or '')
if not okRetries or type(retries) ~= 'table' or #retries ~= receipt.retry_count then
  return {'invalid', 'invalid retry effects'}
end
local okDead, dead = pcall(cjson.decode, ARGV[6] or '')
if not okDead or type(dead) ~= 'table' or #dead ~= receipt.dead_count then
  return {'invalid', 'invalid dead effects'}
end
if receipt.result_count ~= 0 then
  return {'invalid', 'invalid result count'}
end
if not checkEffects(retries, 'retry') then
  return {'invalid', 'invalid retry effect'}
end
if not checkEffects(dead, 'dead') then
  return {'invalid', 'invalid dead effect'}
end
if not checkList(sourceKey, #retries) then
  return {'invalid', 'invalid source key type'}
end
if not checkList(deadKey, #dead) then
  return {'invalid', 'invalid dead key type'}
end
local receiptType = redis.call('TYPE', receiptKey)['ok']
if not invalidReceiptType(receiptType) then
  return {'invalid', 'invalid receipt key type'}
end

local deadlineType = redis.call('TYPE', deadlineKey)['ok']
if deadlineType ~= 'none' and deadlineType ~= 'zset' then
  return {'invalid', 'invalid deadline key type'}
end

local existingRaw = nil
if receiptType == 'string' then
  existingRaw = redis.call('GET', receiptKey)
end
if existingRaw then
  local existing, existingErr = validateReceipt(existingRaw, batchID, nil, nil, nil, true)
  if not existing then
    return {'invalid', existingErr}
  end
  redis.call('ZREM', deadlineKey, batchID)
  if existing.request_id == requestID and existing.command_hash == commandHash then
    return {'duplicate', cjson.encode(existing)}
  end
  return {'stale', cjson.encode(existing)}
end

local pendingType = redis.call('TYPE', pendingKey)['ok']
if pendingType ~= 'none' and pendingType ~= 'string' then
  return {'invalid', 'invalid pending key type'}
end

local pendingRaw = nil
if pendingType == 'string' then
  pendingRaw = redis.call('GET', pendingKey)
end
if not pendingRaw then
  redis.call('ZREM', deadlineKey, batchID)
  return {'not_found'}
end

local okPending, pending = pcall(cjson.decode, pendingRaw)
if not okPending or type(pending) ~= 'table' then
  return {'invalid', 'invalid pending json'}
end
if pending.schema_version ~= 1 or pending.protocol_version ~= 1 or pending.state ~= 'PENDING' or pending.batch_id ~= batchID then
  return {'invalid', 'invalid pending schema'}
end
if type(pending.deadline_at) ~= 'number' or pending.deadline_at <= 0 or pending.deadline_at % 1 ~= 0 then
  return {'invalid', 'invalid pending deadline'}
end

local nowParts = redis.call('TIME')
local nowMillis = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
if pending.deadline_at > nowMillis then
  redis.call('ZADD', deadlineKey, pending.deadline_at, batchID)
  return {'not_due'}
end

for _, item in ipairs(retries) do
  redis.call('RPUSH', sourceKey, cjson.encode(item))
end
for _, item in ipairs(dead) do
  redis.call('RPUSH', deadKey, cjson.encode(item))
end

local closedAtParts = redis.call('TIME')
receipt.closed_at = tonumber(closedAtParts[1]) * 1000 + math.floor(tonumber(closedAtParts[2]) / 1000)
receipt.winner = 'recover'
local receiptJSON = cjson.encode(receipt)
local stored = redis.call('SET', receiptKey, receiptJSON, 'NX', 'PX', receiptTTL)
if not stored then
  return {'stale'}
end

redis.call('ZREM', deadlineKey, batchID)
redis.call('DEL', pendingKey)
return {'applied', receiptJSON}
