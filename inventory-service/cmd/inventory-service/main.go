package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"inventory-service/internal/consumer"
	"inventory-service/internal/logger"
	"inventory-service/internal/metrics"
	"inventory-service/internal/repository/postgres"
	"inventory-service/internal/usecase"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5434/inventory?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "inventory-service")
	log := logger.New("inventory-service")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}

	// DI: repository -> usecase -> consumer, тот же порядок сборки, что и в Order Service.
	stockRepo := postgres.NewStockRepository(pool)
	inventoryUC := usecase.NewInventoryUseCase(stockRepo)
	orderConsumer := consumer.NewOrderCreatedConsumer(kafkaBrokers, consumerGroup, inventoryUC, log)
	defer orderConsumer.Close()

	metricsServer := metrics.StartServer(getEnv("METRICS_PORT_ADDR", ":9100"), log) // НОВОЕ
	defer metricsServer.Shutdown(context.Background())

	log.Info("inventory-service started")
	orderConsumer.Run(ctx) // блокируется до отмены ctx

	log.Info("inventory-service stopped gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
