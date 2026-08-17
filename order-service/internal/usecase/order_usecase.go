package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"KolinMarket/internal/domain"
	"KolinMarket/internal/metrics"
	"KolinMarket/internal/ports"
)

type orderUseCase struct {
	repo ports.OrderRepository
	log  *slog.Logger
}

func NewOrderUseCase(repo ports.OrderRepository, log *slog.Logger) ports.OrderUseCase {
	return &orderUseCase{repo: repo, log: log}
}

func (uc *orderUseCase) CreateOrder(ctx context.Context, input ports.CreateOrderInput) (*domain.Order, error) {
	id := uuid.NewString()

	order, err := domain.NewOrder(id, input.CustomerID, input.Items)
	if err != nil {
		return nil, fmt.Errorf("create order: %w", err)
	}

	event := domain.OrderCreatedEvent{
		OrderID:    order.ID,
		CustomerID: order.CustomerID,
		Items:      order.Items,
		TotalCost:  order.TotalCost,
		CreatedAt:  order.CreatedAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal order created event: %w", err)
	}

	if err := uc.repo.SaveWithEvent(ctx, order, domain.EventTypeOrderCreated, payload); err != nil {
		return nil, fmt.Errorf("save order: %w", err)
	}

	metrics.OrdersCreatedTotal.Inc()

	return order, nil
}

func (uc *orderUseCase) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	order, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("Получение инфо о заказе: %w", err)

	}

	return order, nil
}

func (uc *orderUseCase) ListOrders(ctx context.Context, customerID, cursor string, limit int) ([]*domain.Order, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	orders, nextCursor, err := uc.repo.ListByCustomer(ctx, customerID, cursor, limit)
	if err != nil {
		return nil, "", fmt.Errorf("Получение списка заказов: %w", err)
	}

	return orders, nextCursor, nil
}

func (uc *orderUseCase) UpdateOrderStatus(ctx context.Context, id string, newStatus domain.OrderStatus, reason string) error {
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		order, err := uc.repo.GetByID(ctx, id)
		if err != nil {
			return fmt.Errorf("get order for update: %w", err)
		}

		if err := order.TransitionTo(newStatus); err != nil {
			return fmt.Errorf("transition order status: %w", err)
		}

		eventType, payload, err := buildStatusEvent(order, newStatus, reason) // ИЗМЕНЕНО: передаём reason
		if err != nil {
			return fmt.Errorf("build status event: %w", err)
		}

		err = uc.repo.UpdateStatusWithEvent(ctx, order, eventType, payload)
		if err == nil {
			return nil
		}

		if errors.Is(err, domain.ErrOptimisticLock) {
			uc.log.Warn("optimistic lock conflict, retrying",
				"order id", id,
				"attempt", attempt+1,
				"max_retries", maxRetries)
			continue

		}

		uc.log.Error("exceeded max retries due to concurrent updates",
			"order_id", id,
			"max_retries", maxRetries)
		return fmt.Errorf("update order status: %w", err)
	}

	return fmt.Errorf("update order status: exceeded %d retries due to concurrent updates", maxRetries)
}

func buildStatusEvent(order *domain.Order, newStatus domain.OrderStatus, reason string) (string, []byte, error) {
	switch newStatus {
	case domain.StatusConfirmed:
		event := domain.OrderConfirmedEvent{OrderID: order.ID}
		payload, err := json.Marshal(event)
		return domain.EventTypeOrderConfirmed, payload, err

	case domain.StatusCancelled:
		event := domain.OrderCancelledEvent{OrderID: order.ID, Reason: reason} // ИЗМЕНЕНО: реальная причина вместо хардкода
		payload, err := json.Marshal(event)
		return domain.EventTypeOrderCancelled, payload, err

	default:
		return "", nil, fmt.Errorf("no event mapping for status %s", newStatus)
	}
}
