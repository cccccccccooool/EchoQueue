-- Defer a failed recovery candidate without changing Pending.
-- KEYS[1] pending, KEYS[2] receipt, KEYS[3] deadline
-- ARGV[1] batch id, ARGV[2] error delay milliseconds.
local pendingKey = KEYS[1]
local receiptKey = KEYS[2]
local deadlineKey = KEYS[3]
local batchID = ARGV[1]
local errorDelayMillis = tonumber(ARGV[2])

if not errorDelayMillis or errorDelayMillis < 1 or errorDelayMillis % 1 ~= 0 then
  return {'invalid', 'invalid error delay'}
end

local deadlineType = redis.call('TYPE', deadlineKey)['ok']
if deadlineType ~= 'none' and deadlineType ~= 'zset' then
  return {'invalid', 'invalid deadline key type'}
end

local function validateReceipt(raw)
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
  if type(value.batch_id) ~= 'string' or value.batch_id ~= batchID then
    return nil, 'invalid receipt batch'
  end
  if type(value.request_id) ~= 'string' or value.request_id == '' then
    return nil, 'invalid receipt request'
  end
  if type(value.command_hash) ~= 'string' or value.command_hash == '' then
    return nil, 'invalid receipt command hash'
  end
  if value.winner ~= 'settle' and value.winner ~= 'recover' then
    return nil, 'invalid receipt winner'
  end
  if type(value.closed_at) ~= 'number' or value.closed_at <= 0 or value.closed_at % 1 ~= 0 then
    return nil, 'invalid receipt closed_at'
  end
  for _, field in ipairs({'result_count', 'retry_count', 'dead_count'}) do
    if type(value[field]) ~= 'number' or value[field] < 0 or value[field] % 1 ~= 0 then
      return nil, 'invalid receipt count'
    end
  end
  return value, nil
end

local function deferPending()
  local now = redis.call('TIME')
  local nowMillis = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
  local deferAt = nowMillis + errorDelayMillis
  redis.call('ZADD', deadlineKey, deferAt, batchID)
  return {'deferred', tostring(deferAt)}
end

local receiptType = redis.call('TYPE', receiptKey)['ok']
local receiptRaw = nil
local receiptErr = nil
if receiptType == 'string' then
  receiptRaw = redis.call('GET', receiptKey)
  if receiptRaw then
    local receipt
    receipt, receiptErr = validateReceipt(receiptRaw)
    if receipt then
      redis.call('ZREM', deadlineKey, batchID)
      return {'terminal'}
    end
  end
elseif receiptType ~= 'none' then
  receiptErr = 'invalid receipt key type'
end

local pendingType = redis.call('TYPE', pendingKey)['ok']
if pendingType == 'string' then
  return deferPending()
end
if pendingType ~= 'none' then
  return {'invalid', 'invalid pending key type'}
end

if receiptType == 'none' then
  redis.call('ZREM', deadlineKey, batchID)
  return {'orphan'}
end

return {'invalid', receiptErr or 'invalid receipt'}
