package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"notification-service/internal/consumer"
	"notification-service/internal/logger"
	"notification-service/internal/metrics"
	"notification-service/internal/repository/postgres"
	"notification-service/internal/usecase"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5435/notifications?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "notification-service")
	log := logger.New("notification-service")

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

	inboxRepo := postgres.NewInboxRepository(pool, log)
	notificationUC := usecase.NewNotificationUseCase(inboxRepo)
	eventsConsumer := consumer.NewOrderEventsConsumer(kafkaBrokers, consumerGroup, notificationUC, log)
	defer eventsConsumer.Close()

	metricsServer := metrics.StartServer(getEnv("METRICS_PORT_ADDR", ":9102"), log) // НОВОЕ
	defer metricsServer.Shutdown(context.Background())

	log.Info("notification-service started")
	eventsConsumer.Run(ctx)

	log.Info("notification-service stopped gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
