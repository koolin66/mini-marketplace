package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "api-gateway/docs" // НОВОЕ: подключает сгенерированную спецификацию (init() внутри docs.go регистрирует её)
	"api-gateway/internal/cache"
	grpcclient "api-gateway/internal/client/grpc"
	deliveryhttp "api-gateway/internal/delivery/http"
	"api-gateway/internal/delivery/http/middleware"
	"api-gateway/internal/logger"
	"api-gateway/internal/ratelimit"
)

// @title Mini-Marketplace API
// @version 1.0
// @description API Gateway для системы обработки заказов — создание, получение и листинг заказов.
// @host localhost:8080
// @BasePath /api/v1
func main() {
	httpPort := getEnv("HTTP_PORT", "8080")
	orderServiceAddr := getEnv("ORDER_SERVICE_GRPC_ADDR", "localhost:9091")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379") // НОВОЕ
	log := logger.New("api-gateway")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM) // ИЗМЕНЕНО: унифицировано с остальными сервисами
	defer stop()

	// --- gRPC-клиент к Order Service.
	orderClient, err := grpcclient.NewOrderClient(orderServiceAddr)
	if err != nil {
		log.Error("failed to create order client", "error", err)
		os.Exit(1)
	}
	defer orderClient.Close()

	// НОВОЕ: один общий redis.Client, переиспользуемый и для кэша, и для rate limiter'а —
	// не нужно два отдельных соединения ради двух разных задач.
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	orderCache := cache.NewRedisCache(redisClient, 5*time.Second)

	// НОВОЕ: token bucket — 10 токенов ёмкость, пополнение 2 токена в секунду
	// (то есть burst до 10 запросов подряд, устойчивый темп ~2 запроса/сек после исчерпания).
	limiter := ratelimit.NewTokenBucketLimiter(redisClient, 10, 2.0)

	// --- DI: клиент -> хендлер. Хендлер зависит от ports.OrderClient (интерфейса),
	// grpcclient.OrderClient просто случайно ему удовлетворяет по структуре методов.
	orderHandler := deliveryhttp.NewOrderHandler(orderClient, orderCache, log)

	router := gin.Default()
	router.Use(middleware.RateLimit(limiter))                                 // НОВОЕ: применяется ко ВСЕМ роутам глобально
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler)) // НОВОЕ
	router.Use(middleware.Metrics())                                          // НОВОЕ: метрики считаются для ВСЕХ роутов

	// НОВОЕ: эндпоинт, который Prometheus будет периодически скрейпить (обычно раз в 15с).
	// promhttp.Handler() отдаёт все зарегистрированные метрики в текстовом формате Prometheus.
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := router.Group("/api/v1")
	orderHandler.RegisterRoutes(v1)

	srv := &http.Server{
		Addr:    ":" + httpPort,
		Handler: router,
	}

	go func() {
		log.Info("gateway listening", "port", httpPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()

	log.Info("shutting down gateway")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		// ИЗМЕНЕНО: было Fatalf (немедленный os.Exit, минуя defer'ы orderClient.Close()/redisClient.Close())
		// -> Error, чтобы соединения всё равно закрылись перед выходом из main.
		log.Error("gateway forced to shutdown", "error", err)
	}

	log.Info("gateway exited gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
