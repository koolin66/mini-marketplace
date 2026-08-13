package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {

	return &RedisCache{client: client, ttl: ttl}
}

// Get — возвращает (найдено, ошибка). Пустое значение при "не найдено" — не ошибка,
// это нормальный cache miss, отличаем через bool, а не через nil/пустую строку.
func (c *RedisCache) Get(ctx context.Context, key string, dest any) (bool, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil // ключ просто не найден — это НЕ ошибка
		}
		return false, fmt.Errorf("redis get: %w", err)
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		return false, fmt.Errorf("unmarshal cached value: %w", err)
	}
	return true, nil
}

func (c *RedisCache) Set(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value for cache: %w", err)
	}

	if err := c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
