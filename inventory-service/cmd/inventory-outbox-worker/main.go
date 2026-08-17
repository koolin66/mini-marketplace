package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"inventory-service/internal/logger"
	"inventory-service/internal/metrics"
	"inventory-service/internal/outbox"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5434/inventory?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	log := logger.New("inventory-outbox-worker")

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

	publisher := outbox.NewPublisher(kafkaBrokers)
	defer publisher.Close()

	worker := outbox.NewWorker(pool, publisher, 1*time.Second, 20, log)

	metricsServer := metrics.StartServer(getEnv("METRICS_PORT_ADDR", ":9101"), log) // НОВОЕ — другой порт!
	defer metricsServer.Shutdown(context.Background())

	log.Info("inventory outbox worker started")
	worker.Run(ctx)

	log.Info("inventory outbox worker stopped gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
