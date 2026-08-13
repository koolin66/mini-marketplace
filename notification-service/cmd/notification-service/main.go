package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"notification-service/internal/consumer"
	"notification-service/internal/repository/postgres"
	"notification-service/internal/usecase"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5435/notifications?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "notification-service")

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

	inboxRepo := postgres.NewInboxRepository(pool)
	notificationUC := usecase.NewNotificationUseCase(inboxRepo)
	eventsConsumer := consumer.NewOrderEventsConsumer(kafkaBrokers, consumerGroup, notificationUC)
	defer eventsConsumer.Close()

	log.Println("notification-service started")
	eventsConsumer.Run(ctx)

	log.Println("notification-service stopped gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
