package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"api-gateway/internal/metrics"
)

func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next() // выполняем сам запрос — хендлер отработает здесь

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		// c.FullPath() — шаблон роута ("/orders/:id"), а не реальный URL ("/orders/abc-123").
		// Это критично: если бы использовали реальный URL, каждый уникальный order_id
		// создавал бы СВОЙ временной ряд в Prometheus — метрики "взорвались" бы по кардинальности.
		path := c.FullPath()
		if path == "" {
			path = "unmatched" // запрос на несуществующий роут (404 до роутинга)
		}

		metrics.HTTPRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}
