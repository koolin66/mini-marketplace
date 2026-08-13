package domain

import (
	"errors"
	"time"
)

type OrderStatus string

const (
	StatusCreated       OrderStatus = "CREATED"        //заказ создан
	StatusAwaitingStock OrderStatus = "AWAITING_STOCK" //опубликован в кафка
	StatusConfirmed     OrderStatus = "CONFIRMED"      //товар зарезервирован (ответит Inventory)
	StatusCancelled     OrderStatus = "CANCELLED"      //товара нет (ответ от Inventory)
	StatusCompleted     OrderStatus = "COMPLETED"      // финальный статус (после уведомления)
)

// allowedTransitions — явная FSM (finite state machine).
// Это отвечает на собеседовательский вопрос "как вы защищаетесь от невалидных переходов статуса?"
// тут видно во какой статус может переходить прошлый статус
var allowedTransactions = map[OrderStatus][]OrderStatus{
	StatusCreated:       {StatusAwaitingStock},
	StatusAwaitingStock: {StatusConfirmed, StatusCancelled},
	StatusConfirmed:     {StatusCompleted},
	StatusCancelled:     {},
	StatusCompleted:     {},
}

// Про таймаут: если заказ висит в AWAITING_STOCK дольше N секунд —
// отдельная фоновая джоба (order timeout checker) сама переводит его в CANCELLED
// и публикует компенсирующее событие. Это НЕ статус, а отдельный воркер,
// который просто дергает тот же CanTransitionTo + Cancel()

// OrderCreatedEvent — то, что реально уйдёт в Kafka. Отдельная структура от Order,
// потому что событие — это "снимок" на момент создания, не обязан быть 1-в-1 с агрегатом
// (например, в событие можно не включать все внутренние поля).
type OrderCreatedEvent struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	TotalCost  Money       `json:"total_cost"`
	CreatedAt  time.Time   `json:"created_at"`
}

const (
	EventTypeOrderCreated   = "order.created"
	EventTypeOrderConfirmed = "order.confirmed"
	EventTypeOrderCancelled = "order.cancelled"
)

type OrderCancelledEvent struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type OrderConfirmedEvent struct {
	OrderID string `json:"order_id"`
}

var (
	ErrEmptyItems        = errors.New("заказ должен содержать как минимум 1 товар")
	ErrInvalidTransition = errors.New("невалидный переход статуса")
	ErrOptimisticLock    = errors.New("заказ обрабатывается конкурентно, повтор")
)

type Money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type OrderItem struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Price    Money  `json:"price"`
	Quantity int    `json:"quantity"`
}

func NewMoney(amount int64, currency string) Money {
	return Money{Amount: amount, Currency: currency}
}

func (m Money) Add(other Money) Money {
	return Money{Amount: m.Amount + other.Amount, Currency: m.Currency}
}

func (i OrderItem) Subtotal() Money {
	return Money{Amount: i.Price.Amount * int64(i.Quantity), Currency: i.Price.Currency}
}

// Order — Aggregate Root.
type Order struct {
	ID         string
	CustomerID string
	Items      []OrderItem
	TotalCost  Money
	Status     OrderStatus
	Version    int // optimistic locking
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewOrder(id, customerID string, items []OrderItem) (*Order, error) {
	if len(items) == 0 {
		return nil, ErrEmptyItems
	}

	total := Money{Amount: 0, Currency: items[0].Price.Currency}
	for _, it := range items {
		total = total.Add(it.Subtotal())
	}

	now := time.Now().UTC()

	return &Order{
		ID:         id,
		CustomerID: customerID,
		Items:      items,
		TotalCost:  total,
		Status:     StatusCreated,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

// TransitionTo — единственный способ поменять статус. Никто не пишет order.Status = X напрямую.
func (o *Order) TransitionTo(newStatus OrderStatus) error {
	allowed := allowedTransactions[o.Status]
	for _, s := range allowed {
		if s == newStatus {
			o.Status = newStatus
			o.UpdatedAt = time.Now().UTC()
			return nil
		}
	}
	return ErrInvalidTransition
}
