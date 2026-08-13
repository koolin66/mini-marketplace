package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/segmentio/kafka-go"

	"notification-service/internal/ports"
)

// Общий формат для всех трёх событий — используем "мягкий" парсинг:
// order_id есть у всех, reason есть только у order.cancelled (у остальных будет пустая строка).
type OrderEvent struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason,omitempty"`
}

type OrderEventsConsumer struct {
	readers []*kafka.Reader
	uc      ports.NotificationUseCase
}

func NewOrderEventsConsumer(brokers []string, groupID string, uc ports.NotificationUseCase) *OrderEventsConsumer {
	topics := []string{"order.created", "order.confirmed", "order.cancelled"}

	readers := make([]*kafka.Reader, 0, len(topics))
	for _, topic := range topics {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}))
	}

	return &OrderEventsConsumer{readers: readers, uc: uc}
}

func (c *OrderEventsConsumer) Close() error {
	var firstErr error
	for _, r := range c.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Run — по одной горутине на каждый topic reader, все параллельно.
func (c *OrderEventsConsumer) Run(ctx context.Context) {
	for _, reader := range c.readers {
		go c.consumeLoop(ctx, reader)
	}
	<-ctx.Done()
	log.Println("order events consumer: stopping")
}

func (c *OrderEventsConsumer) consumeLoop(ctx context.Context, reader *kafka.Reader) {
	topic := reader.Config().Topic
	log.Printf("%s consumer started", topic)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("%s consumer: fetch error: %v", topic, err)
			continue
		}

		if err := c.handleMessage(ctx, topic, msg); err != nil {
			log.Printf("%s consumer: handle error: %v", topic, err)
			continue // не коммитим — ретрай
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("%s consumer: commit error: %v", topic, err)
		}
	}
}

func (c *OrderEventsConsumer) handleMessage(ctx context.Context, topic string, msg kafka.Message) error {
	var event OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("%s consumer: failed to unmarshal: %v", topic, err)
		return nil // poison pill — коммитим и идём дальше
	}

	err := c.uc.Notify(ctx, event.OrderID, topic, event.Reason)
	if err != nil {
		if errors.Is(err, ports.ErrAlreadyProcessed) {
			log.Printf("%s consumer: order %s already notified, skipping", topic, event.OrderID)
			return nil
		}
		return err // инфраструктурная ошибка — не коммитим, ретрай
	}

	return nil
}
