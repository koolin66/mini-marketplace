-- KEYS[1] = ключ ведра (например "ratelimit:customer:X")
-- ARGV[1] = capacity (максимум токенов в ведре)
-- ARGV[2] = refill_rate (токенов в секунду)
-- ARGV[3] = now (текущее время в секундах, unix timestamp с плавающей точкой)
-- ARGV[4] = requested (сколько токенов запрашивается, обычно 1)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- Если ведра ещё нет (первый запрос клиента) — начинаем с полного ведра.
if tokens == nil then
	tokens = capacity
	last_refill = now
end

-- Сколько времени прошло с последнего пополнения, сколько токенов должно было добавиться.
local elapsed = now - last_refill
local refill = elapsed * refill_rate
tokens = math.min(capacity, tokens + refill)

local allowed = 0
if tokens >= requested then
	tokens = tokens - requested
	allowed = 1
end

redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], 3600) -- ведро само истечёт, если клиент долго неактивен

return allowed