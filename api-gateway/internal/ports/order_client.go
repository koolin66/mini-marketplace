package ports

import (
	"context"

	orderv1 "api-gateway/proto/order/v1"
)

// OrderClient — абстракция над способом связи с Order Service.
// Хендлер зависит от этого интерфейса, не от grpc.OrderClient напрямую —
// так же, как usecase в Order Service не знал про pgx, а только про ports.OrderRepository.
type OrderClient interface {
	CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.OrderResponse, error)
	GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.OrderResponse, error)
	ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error)
}
