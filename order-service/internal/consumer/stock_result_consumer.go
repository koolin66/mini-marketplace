package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

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

type StockResultConsumer struct {
	reservedReader *kafka.Reader
	failedReader   *kafka.Reader
	uc             ports.OrderUseCase
	log            *slog.Logger
}

func NewStockResultConsumer(brokers []string, groupID string, uc ports.OrderUseCase, log *slog.Logger) *StockResultConsumer {
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
		log:            log,
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

func (c *StockResultConsumer) Run(ctx context.Context) {
	go c.runReserved(ctx)
	go c.runFailed(ctx)

	<-ctx.Done()
	c.log.Info("stopping")
}

func (c *StockResultConsumer) runReserved(ctx context.Context) {
	// НОВОЕ: .With("consumer", "stock.reserved") — создаёт логгер с привязанным полем,
	// которое автоматически попадёт в КАЖДЫЙ вызов через него, без повторения в каждой строке.
	log := c.log.With("consumer", "stock.reserved")
	log.Info("started")

	for {
		msg, err := c.reservedReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("fetch error", "error", err)
			continue
		}

		if err := c.handleReserved(ctx, msg, log); err != nil {
			log.Error("handle error", "error", err)
			continue
		}

		if err := c.reservedReader.CommitMessages(ctx, msg); err != nil {
			log.Error("commit error", "error", err)
		}
	}
}

func (c *StockResultConsumer) runFailed(ctx context.Context) {
	log := c.log.With("consumer", "stock.failed")
	log.Info("started")

	for {
		msg, err := c.failedReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("fetch error", "error", err) // ИСПРАВЛЕНО: было Error, для единообразия с runReserved
			continue
		}

		if err := c.handleFailed(ctx, msg, log); err != nil {
			log.Error("handle error", "error", err)
			continue
		}

		if err := c.failedReader.CommitMessages(ctx, msg); err != nil {
			log.Error("commit error", "error", err) // ИСПРАВЛЕНО: было "handle error" — неверное сообщение, скопированное по ошибке
		}
	}
}

func (c *StockResultConsumer) handleReserved(ctx context.Context, msg kafka.Message, log *slog.Logger) error {
	var event StockReservedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Error("failed to unmarshal", "error", err)
		return nil
	}

	err := c.uc.UpdateOrderStatus(ctx, event.OrderID, domain.StatusConfirmed, "")
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			log.Warn("order already in different state (likely duplicate)", "error", err, "order_id", event.OrderID) // ИСПРАВЛЕНО: было Error
			return nil
		}
		return err
	}

	log.Info("order confirmed", "order_id", event.OrderID)
	return nil
}

func (c *StockResultConsumer) handleFailed(ctx context.Context, msg kafka.Message, log *slog.Logger) error {
	var event StockFailedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Error("failed to unmarshal", "error", err)
		return nil
	}

	err := c.uc.UpdateOrderStatus(ctx, event.OrderID, domain.StatusCancelled, event.Reason)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			log.Warn("order already in different state (likely duplicate)", "order_id", event.OrderID, "error", err) // ИСПРАВЛЕНО: было Error
			return nil
		}
		return err
	}

	log.Info("order cancelled", "order_id", event.OrderID, "reason", event.Reason)
	return nil
}
