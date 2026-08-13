package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"inventory-service/internal/consumer"
	"inventory-service/internal/repository/postgres"
	"inventory-service/internal/usecase"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5434/inventory?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "inventory-service")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to ping postgres: %v", err)
	}

	// DI: repository -> usecase -> consumer, тот же порядок сборки, что и в Order Service.
	stockRepo := postgres.NewStockRepository(pool)
	inventoryUC := usecase.NewInventoryUseCase(stockRepo)
	orderConsumer := consumer.NewOrderCreatedConsumer(kafkaBrokers, consumerGroup, inventoryUC)
	defer orderConsumer.Close()

	log.Println("inventory-service started")
	orderConsumer.Run(ctx) // блокируется до отмены ctx

	log.Println("inventory-service stopped gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
