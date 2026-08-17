package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"notification-service/internal/metrics"
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
	log     *slog.Logger
}

func NewOrderEventsConsumer(brokers []string, groupID string, uc ports.NotificationUseCase, log *slog.Logger) *OrderEventsConsumer {
	topics := []string{"order.created", "order.confirmed", "order.cancelled"}

	readers := make([]*kafka.Reader, 0, len(topics))
	for _, topic := range topics {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}))
	}

	return &OrderEventsConsumer{readers: readers, uc: uc, log: log}
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
	c.log.Info("order events consumer stopping")
}

func (c *OrderEventsConsumer) consumeLoop(ctx context.Context, reader *kafka.Reader) {
	topic := reader.Config().Topic
	c.log.Info("topic consumer started", "topic", topic)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.log.Error("topic consumer fetch failed", "topic", topic, "error", err)
			continue
		}

		if err := c.handleMessage(ctx, topic, msg); err != nil {
			c.log.Error("topic consumer handle failed", "topic", topic, "error", err)
			continue // не коммитим — ретрай
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			c.log.Error("topic consumer commit failed", "topic", topic, "error", err)
		}
	}
}

func (c *OrderEventsConsumer) handleMessage(ctx context.Context, topic string, msg kafka.Message) error {
	start := time.Now()

	var event OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.log.Error("topic consumer failed to unmarshal", "topic", topic, "error", err)
		metrics.NotificationsProcessedTotal.WithLabelValues(topic, "unmarshal_error").Inc()
		return nil // poison pill — коммитим и идём дальше
	}

	err := c.uc.Notify(ctx, event.OrderID, topic, event.Reason)

	metrics.NotificationProcessingDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())

	if err != nil {
		if errors.Is(err, ports.ErrAlreadyProcessed) {
			c.log.Warn("topic consumer: order already notified, skipping", "topic", topic, "order_id", event.OrderID)
			metrics.NotificationsProcessedTotal.WithLabelValues(topic, "already_processed").Inc()
			return nil
		}
		metrics.NotificationsProcessedTotal.WithLabelValues(topic, "error").Inc()
		return err // инфраструктурная ошибка — не коммитим, ретрай
	}
	metrics.NotificationsProcessedTotal.WithLabelValues(topic, "sent").Inc()
	return nil
}
