package ports

import (
	"context"

	"KolinMarket/internal/domain"
)

// CreateOrderInput/Output — DTO на границе usecase, чтобы delivery-слой (HTTP/gRPC)
// не тащил в usecase свои структуры запроса напрямую.
type CreateOrderInput struct {
	CustomerID string
	Items      []domain.OrderItem
}
type OrderUseCase interface {
	CreateOrder(ctx context.Context, input CreateOrderInput) (*domain.Order, error)
	GetOrder(ctx context.Context, id string) (*domain.Order, error)
	ListOrders(ctx context.Context, customerID, cursor string, limit int) ([]*domain.Order, string, error)

	// UpdateOrderStatus — ИЗМЕНЕНО: добавлен параметр reason.
	// Используется только при переходе в CANCELLED — для CONFIRMED игнорируется (можно передать "").
	UpdateOrderStatus(ctx context.Context, id string, newStatus domain.OrderStatus, reason string) error
}
