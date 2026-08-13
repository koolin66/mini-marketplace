package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const tokenBucketScript = `
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

if tokens == nil then
	tokens = capacity
	last_refill = now
end

local elapsed = now - last_refill
local refill = elapsed * refill_rate
tokens = math.min(capacity, tokens + refill)

local allowed = 0
if tokens >= requested then
	tokens = tokens - requested
	allowed = 1
end

redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], 3600)

return allowed
`

type TokenBucketLimiter struct {
	client     *redis.Client
	capacity   int
	refillRate float64 // токенов в секунду
}

func NewTokenBucketLimiter(client *redis.Client, capacity int, refillRate float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{client: client, capacity: capacity, refillRate: refillRate}
}

// Allow — возвращает true, если запрос разрешён (токен списан), false — если лимит исчерпан.
func (l *TokenBucketLimiter) Allow(ctx context.Context, key string) (bool, error) {
	now := float64(time.Now().UnixNano()) / 1e9 // секунды с дробной частью, для точности пополнения

	result, err := l.client.Eval(ctx, tokenBucketScript, []string{key},
		l.capacity, l.refillRate, now, 1,
	).Result()
	if err != nil {
		return false, fmt.Errorf("eval token bucket script: %w", err)
	}

	allowed, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected script result type: %T", result)
	}

	return allowed == 1, nil
}
