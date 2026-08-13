package outbox

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Publisher — тонкая обёртка над kafka-go writer.
// Отдельный тип, чтобы Worker не был завязан на конкретную реализацию Kafka-клиента —
// тот же принцип разделения интерфейса/реализации, что и с репозиторием.
type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.Hash{}, // хешируем по ключу сообщения — гарантирует, что все события
			// одного order_id попадут в одну партицию (важно для порядка!)
			RequiredAcks: kafka.RequireAll, // ждём подтверждения от ВСЕХ реплик перед тем как считать отправленным
		},
	}
}

func (p *Publisher) Publish(ctx context.Context, key string, topic string, payload []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key), // order_id как ключ — гарантия упорядоченности событий одного заказа
		Value: payload,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish message to kafka: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
