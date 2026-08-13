package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"api-gateway/internal/ratelimit"
)

func RateLimit(limiter *ratelimit.TokenBucketLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Ключ по IP клиента — простая стратегия для pet-проекта.
		// В реальной системе часто ключуют по customer_id/API-key, если он есть в контексте авторизации.
		key := "ratelimit:" + c.ClientIP()

		allowed, err := limiter.Allow(c.Request.Context(), key)
		if err != nil {
			// Redis недоступен — та же философия graceful degradation, что и с кэшем:
			// не блокируем запрос из-за сбоя инфраструктуры rate limiter'а, пропускаем.
			c.Next()
			return
		}

		if !allowed {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			c.Abort()
			return
		}

		c.Next()
	}
}
