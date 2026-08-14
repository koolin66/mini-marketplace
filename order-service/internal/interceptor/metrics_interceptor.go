package interceptor

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"KolinMarket/internal/metrics"
)

// MetricsUnaryInterceptor — оборачивает КАЖДЫЙ входящий gRPC-вызов.
// Сигнатура строго определена gRPC — это то, что регистрируется через
// grpc.NewServer(grpc.UnaryInterceptor(...)).
func MetricsUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()

	// handler(ctx, req) — это ВЫЗОВ реального gRPC-метода (CreateOrder/GetOrder/ListOrders).
	// Всё, что до этой строки — код ДО обработки запроса, всё после — код ПОСЛЕ.
	resp, err := handler(ctx, req)

	duration := time.Since(start).Seconds()

	// info.FullMethod — что-то вроде "/order.v1.OrderService/CreateOrder",
	// стабильная строка (аналог c.FullPath() в gin — не зависит от конкретных данных запроса).
	method := info.FullMethod

	// status.Code(err) возвращает codes.OK, если err == nil — тот же механизм status codes,
	// что мы уже использовали в mapDomainErrToGRPC.
	code := status.Code(err).String()

	metrics.GRPCRequestsTotal.WithLabelValues(method, code).Inc()
	metrics.GRPCRequestDuration.WithLabelValues(method).Observe(duration)

	return resp, err
}
