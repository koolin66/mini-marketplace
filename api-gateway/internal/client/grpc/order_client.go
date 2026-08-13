package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderv1 "api-gateway/proto/order/v1"
)

// OrderClient — обёртка над сгенерированным orderv1.OrderServiceClient.
// Зачем обёртка, а не использовать сырой orderv1.OrderServiceClient напрямую в хендлерах?
// Чтобы HTTP-хендлеры Gateway зависели от ports-интерфейса (который объявим дальше),
// а не от gRPC-специфики напрямую — тот же принцип Dependency Inversion, что и в Order Service.
type OrderClient struct {
	client orderv1.OrderServiceClient
	conn   *grpc.ClientConn
}

func NewOrderClient(addr string) (*OrderClient, error) {
	// insecure.NewCredentials() — без TLS, нормально для internal traffic между
	// сервисами в одной docker-сети. В проде между datacenter'ами обычно mTLS.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to order service: %w", err)
	}

	return &OrderClient{
		client: orderv1.NewOrderServiceClient(conn),
		conn:   conn,
	}, nil
}

func (c *OrderClient) Close() error {
	return c.conn.Close()
}

func (c *OrderClient) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.OrderResponse, error) {
	return c.client.CreateOrder(ctx, req)
}

func (c *OrderClient) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.OrderResponse, error) {
	return c.client.GetOrder(ctx, req)
}

func (c *OrderClient) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	return c.client.ListOrders(ctx, req)
}
