package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/segmentio/kafka-go"

	"inventory-service/internal/domain"
	"inventory-service/internal/ports"
)

// OrderCreatedEvent — своя копия структуры события (та же форма, что и в Order Service,
// но НЕ общий тип — сервисы не шарят Go-структуры напрямую, только JSON-контракт).
// Это тот самый trade-off, что мы обсуждали с proto: можно было бы шарить через модуль,
// но раз мы решили копировать proto просто копированием файлов — здесь то же самое,
// просто в виде Go-структуры под JSON, а не protobuf.
type OrderCreatedEvent struct {
	OrderID    string             `json:"order_id"`
	CustomerID string             `json:"customer_id"`
	Items      []OrderCreatedItem `json:"items"`
	CreatedAt  string             `json:"created_at"`
}

type OrderCreatedItem struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
}

// OrderCreatedConsumer — обёртка над kafka.Reader + usecase.
type OrderCreatedConsumer struct {
	reader *kafka.Reader
	uc     ports.InventoryUseCase
}

func NewOrderCreatedConsumer(brokers []string, groupID string, uc ports.InventoryUseCase) *OrderCreatedConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   "order.created",
		GroupID: groupID, // consumer group — важно для будущего масштабирования (несколько
		// инстансов Inventory Service с одним GroupID делят партиции между собой,
		// каждое сообщение обработается ровно одним инстансом группы)
	})

	return &OrderCreatedConsumer{reader: reader, uc: uc}
}

func (c *OrderCreatedConsumer) Close() error {
	return c.reader.Close()
}

// Run — блокирующий цикл чтения. Останавливается через отмену ctx (graceful shutdown).
func (c *OrderCreatedConsumer) Run(ctx context.Context) {
	log.Println("order.created consumer started")

	for {
		// FetchMessage — читает следующее сообщение, НЕ коммитит offset автоматически.
		// Явный контроль коммита — ключевой момент для надёжности (см. ниже).
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("order.created consumer: context cancelled, stopping")
				return
			}
			log.Printf("order.created consumer: fetch error: %v", err)
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			log.Printf("order.created consumer: handle error: %v", err)
			// НЕ коммитим offset при ошибке обработки — сообщение будет доставлено повторно
			// при следующем перезапуске/ребалансе. Это и есть at-least-once на стороне consumer'а.
			continue
		}

		// Коммитим offset ТОЛЬКО после успешной обработки (включая случай дубликата,
		// см. handleMessage — там ErrAlreadyProcessed тоже считается "успехом" для коммита).
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("order.created consumer: commit error: %v", err)
		}
	}
}

func (c *OrderCreatedConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var event OrderCreatedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		// Сообщение пришло в формате, который мы не можем распарсить — это "poison pill"
		// (сообщение, которое никогда не станет валидным при повторной попытке).
		// Логируем как критическую проблему, но НЕ ретраим бесконечно — considerations
		// про dead-letter queue см. ниже в вопросе.
		log.Printf("order.created consumer: failed to unmarshal message: %v", err)
		return nil // возвращаем nil, чтобы offset закоммитился, и мы не застряли на этом сообщении навсегда
	}

	items := make([]ports.ReservationItem, 0, len(event.Items))
	for _, it := range event.Items {
		items = append(items, ports.ReservationItem{SKU: it.SKU, Qty: it.Quantity})
	}

	err := c.uc.ReserveStock(ctx, event.OrderID, items)

	switch {
	case err == nil:
		log.Printf("order.created consumer: reserved stock for order %s", event.OrderID)
		return nil

	case errors.Is(err, ports.ErrAlreadyProcessed):
		// Дубликат — не ошибка, просто идемпотентный no-op. Логируем на всякий случай,
		// но не считаем это проблемой, коммитим offset как обычно.
		log.Printf("order.created consumer: order %s already processed, skipping", event.OrderID)
		return nil

	case errors.Is(err, domain.ErrInsufficientStock), errors.Is(err, domain.ErrStockNotFound):
		// Реальная бизнес-ошибка — товара не хватает. Это НЕ повод ретраить (повторная
		// попытка ничего не изменит, пока кто-то не пополнит склад) и НЕ повод НЕ коммитить offset —
		// мы обработали сообщение, просто с бизнес-результатом "отказ".
		log.Printf("order.created consumer: insufficient stock for order %s: %v", event.OrderID, err)
		return nil

	default:
		// Инфраструктурная ошибка (БД недоступна и т.п.) — вот здесь ХОТИМ ретрай,
		// поэтому возвращаем ошибку наверх в Run(), где offset НЕ будет закоммичен.
		return err
	}
}
