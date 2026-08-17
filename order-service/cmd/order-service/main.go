package main

import (
	"KolinMarket/internal/consumer"
	grpcdelivery "KolinMarket/internal/delivery/grpc"
	deliveryhttp "KolinMarket/internal/delivery/http"
	"KolinMarket/internal/interceptor"
	"KolinMarket/internal/logger"
	"KolinMarket/internal/repository/postgres"
	"KolinMarket/internal/usecase"
	orderv1 "KolinMarket/proto/order/v1"
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/orders?sslmode=disable")
	httpPort := getEnv("HTTP_PORT", "8081")
	grpcPort := getEnv("GRPC_PORT", "9091")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "order-service")
	log := logger.New("order-service")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		// ИЗМЕНЕНО: log.Fatalf не существует у slog.Logger — используем log.Error + os.Exit(1).
		// slog сознательно не занимается завершением процесса, это отдельная ответственность.
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}

	orderRepo := postgres.NewOrderRepository(pool, log)
	orderUC := usecase.NewOrderUseCase(orderRepo, log)

	// ИЗМЕНЕНО: добавлен log третьим параметром — для единообразия DI по всему проекту
	orderHandler := deliveryhttp.NewOrderHandler(orderUC, log)
	router := gin.Default()
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	v1 := router.Group("api/v1")
	orderHandler.RegisterRoutes(v1)

	httpSrv := &http.Server{
		Addr:    ":" + httpPort,
		Handler: router,
	}

	go func() {
		// ИЗМЕНЕНО: log.Printf -> log.Info с явным полем вместо интерполяции в строку
		log.Info("http server listening", "port", httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.MetricsUnaryInterceptor),
	)
	orderGRPCServer := grpcdelivery.NewOrderGRPCServer(orderUC, log)
	orderv1.RegisterOrderServiceServer(grpcServer, orderGRPCServer)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen on grpc port", "error", err)
		os.Exit(1)
	}

	go func() {
		log.Info("grpc server listening", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc serve failed", "error", err)
			os.Exit(1)
		}
	}()

	stockConsumer := consumer.NewStockResultConsumer(kafkaBrokers, consumerGroup, orderUC, log)
	defer stockConsumer.Close()

	go func() {
		stockConsumer.Run(ctx)
	}()

	<-ctx.Done()
	log.Info("shutting down servers")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		// ИЗМЕНЕНО: было Fatalf (убивало процесс сразу) -> Error, чтобы дать шанс
		// остальному коду graceful shutdown (gRPC) всё равно отработать.
		log.Error("http server forced to shutdown", "error", err)
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("grpc server stopped gracefully")
	case <-shutdownCtx.Done():
		log.Warn("grpc shutdown timed out, forcing stop")
		grpcServer.Stop()
	}

	log.Info("server exited gracefully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
