package main

import (
	"KolinMarket/internal/consumer"
	grpcdelivery "KolinMarket/internal/delivery/grpc"
	deliveryhttp "KolinMarket/internal/delivery/http"
	"KolinMarket/internal/interceptor"
	"KolinMarket/internal/repository/postgres"
	"KolinMarket/internal/usecase"
	orderv1 "KolinMarket/proto/order/v1"
	"context"
	"log"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM) // ИЗМЕНЕНО: используем NotifyContext вместо ручного signal.Notify, для единообразия с воркерами
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к Postgres")
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Неудачная проверка Postgres %v", err)
	}

	orderRepo := postgres.NewOrderRepository(pool)
	orderUC := usecase.NewOrderUseCase(orderRepo)

	orderHandler := deliveryhttp.NewOrderHandler(orderUC)
	router := gin.Default()
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	v1 := router.Group("api/v1")
	orderHandler.RegisterRoutes(v1)

	httpSrv := &http.Server{
		Addr:    ":" + httpPort,
		Handler: router,
	}

	go func() {
		log.Printf("Маркет слушает на:%s", httpPort)
		if err := httpSrv.ListenAndServe(); err != nil {
			log.Fatalf("Сервер: %v", err)
		}
	}()

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.MetricsUnaryInterceptor),
	)
	orderGRPCServer := grpcdelivery.NewOrderGRPCServer(orderUC)
	orderv1.RegisterOrderServiceServer(grpcServer, orderGRPCServer)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("ошибка прослушивания порта грпц: %v", err)
	}

	go func() {
		log.Printf("grpc сервер слуаешт на порту: %v", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// --- НОВОЕ: Kafka consumer, слушающий stock.reserved/stock.failed
	stockConsumer := consumer.NewStockResultConsumer(kafkaBrokers, consumerGroup, orderUC)
	defer stockConsumer.Close()

	go func() {
		stockConsumer.Run(ctx) // блокируется до отмены ctx, но мы в отдельной горутине — не блокируем main
	}()

	// --- Graceful shutdown
	<-ctx.Done() // ИЗМЕНЕНО: раньше было signal.Notify + <-quit, теперь просто ждём отмены ctx

	// quit := make(chan os.Signal, 1)
	// signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// <-quit
	log.Printf("грейсфул шатдаун серверов...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("выключение сервера: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Println("grpc server stopped gracefully")
	case <-shutdownCtx.Done():
		log.Println("grpc shutdown timed out, forcing stop")
		grpcServer.Stop()

	}

	log.Println("сервер выключен успешно")

}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
