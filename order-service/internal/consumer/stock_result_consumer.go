package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/segmentio/kafka-go"

	"KolinMarket/internal/domain"
	"KolinMarket/internal/ports"
)

type StockReservedEvent struct {
	OrderID string `json:"order_id"`
}

type StockFailedEvent struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// StockResultConsumer — слушает ДВА топика сразу: stock.reserved и stock.failed.
// kafka-go Reader умеет слушать только один топик за раз, поэтому заведём два Reader'а
// внутри одного consumer'а, оба пишут в общий usecase.
type StockResultConsumer struct {
	reservedReader *kafka.Reader
	failedReader   *kafka.Reader
	uc             ports.OrderUseCase
}

func NewStockResultConsumer(brokers []string, groupID string, uc ports.OrderUseCase) *StockResultConsumer {
	reservedReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   "stock.reserved",
		GroupID: groupID,
	})
	failedReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   "stock.failed",
		GroupID: groupID,
	})

	return &StockResultConsumer{
		reservedReader: reservedReader,
		failedReader:   failedReader,
		uc:             uc,
	}
}

func (c *StockResultConsumer) Close() error {
	err1 := c.reservedReader.Close()
	err2 := c.failedReader.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// Run — запускает ДВЕ горутины, по одной на каждый топик, обе слушают параллельно.
func (c *StockResultConsumer) Run(ctx context.Context) {
	go c.runReserved(ctx)
	go c.runFailed(ctx)

	<-ctx.Done() // блокируем Run до отмены контекста, чтобы вызывающий main мог просто вызвать Run() и ждать
	log.Println("stock result consumer: stopping")
}

func (c *StockResultConsumer) runReserved(ctx context.Context) {
	log.Println("stock.reserved consumer started")
	for {
		msg, err := c.reservedReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("stock.reserved consumer: fetch error: %v", err)
			continue
		}

		if err := c.handleReserved(ctx, msg); err != nil {
			log.Printf("stock.reserved consumer: handle error: %v", err)
			continue // offset не коммитим, ретрай на следующей итерации poll
		}

		if err := c.reservedReader.CommitMessages(ctx, msg); err != nil {
			log.Printf("stock.reserved consumer: commit error: %v", err)
		}
	}
}

func (c *StockResultConsumer) runFailed(ctx context.Context) {
	log.Println("stock.failed consumer started")
	for {
		msg, err := c.failedReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("stock.failed consumer: fetch error: %v", err)
			continue
		}

		if err := c.handleFailed(ctx, msg); err != nil {
			log.Printf("stock.failed consumer: handle error: %v", err)
			continue
		}

		if err := c.failedReader.CommitMessages(ctx, msg); err != nil {
			log.Printf("stock.failed consumer: commit error: %v", err)
		}
	}
}

func (c *StockResultConsumer) handleReserved(ctx context.Context, msg kafka.Message) error {
	var event StockReservedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("stock.reserved consumer: failed to unmarshal: %v", err)
		return nil
	}

	err := c.uc.UpdateOrderStatus(ctx, event.OrderID, domain.StatusConfirmed, "") // ИЗМЕНЕНО: reason не нужен для CONFIRMED
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			log.Printf("stock.reserved consumer: order %s already in different state (likely duplicate): %v", event.OrderID, err)
			return nil
		}
		return err
	}

	log.Printf("stock.reserved consumer: order %s confirmed", event.OrderID)
	return nil
}

func (c *StockResultConsumer) handleFailed(ctx context.Context, msg kafka.Message) error {
	var event StockFailedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("stock.failed consumer: failed to unmarshal: %v", err)
		return nil
	}

	err := c.uc.UpdateOrderStatus(ctx, event.OrderID, domain.StatusCancelled, event.Reason) // ИЗМЕНЕНО: реальная причина из Inventory
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			log.Printf("stock.failed consumer: order %s already in different state (likely duplicate): %v", event.OrderID, err)
			return nil
		}
		return err
	}

	log.Printf("stock.failed consumer: order %s cancelled, reason: %s", event.OrderID, event.Reason)
	return nil
}
