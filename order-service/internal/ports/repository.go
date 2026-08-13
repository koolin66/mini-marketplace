package ports

import (
	"context"

	"KolinMarket/internal/domain"
)

//go:generate mockgen -source=repository.go -destination=mocks/repository_mock.go -package=mocks

// OrderRepository — единственная точка доступа к Order-агрегату.
// Обрати внимание: нет отдельного OrderItemRepository — items приходят/уходят
// вместе с Order, одной транзакцией.
type OrderRepository interface {
	// SaveWithEvent — атомарная запись: orders + order_items + outbox одной транзакцией.
	// Заменяет прежний Save — в системе нет сценария создания заказа без outbox-события.
	SaveWithEvent(ctx context.Context, order *domain.Order, eventType string, payload []byte) error

	// GetByID — простое чтение одного агрегата целиком (orders + order_items JOIN).
	GetByID(ctx context.Context, id string) (*domain.Order, error)

	// UpdateStatusWithEvent — НОВОЕ: атомарно меняет статус (с optimistic lock проверкой)
	// И пишет outbox-запись в ОДНОЙ транзакции.
	UpdateStatusWithEvent(ctx context.Context, order *domain.Order, eventType string, payload []byte) error

	// ListByCustomer — курсорная пагинация (не OFFSET/LIMIT!).
	// cursor — обычно encoded (created_at, id) последней записи предыдущей страницы.
	// Вернём (заказы, следующий_курсор, error).
	ListByCustomer(ctx context.Context, customerID string, cursor string, limit int) ([]*domain.Order, string, error)
}

// OutboxRepository — отдельный интерфейс, не мешаем с OrderRepository.
// Разделение интерфейсов (Interface Segregation) — usecase, которому нужен только Save заказа,
// не обязан знать про Outbox; а вот CreateOrder-flow нуждается в обоих сразу.
type OutboxRepository interface {
	// SaveWithOrder — ключевой метод: принимает уже открытую транзакцию (или Order+event),
	// пишет и orders/order_items, и outbox ОДНОЙ транзакцией.
	SaveOrderWithEvent(ctx context.Context, order *domain.Order, eventType string, payload []byte) error
}
