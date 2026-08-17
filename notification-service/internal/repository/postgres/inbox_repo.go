package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"notification-service/internal/ports"
)

type inboxRepository struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewInboxRepository(pool *pgxpool.Pool, log *slog.Logger) *inboxRepository {
	return &inboxRepository{pool: pool, log: log}
}

func (r *inboxRepository) MarkProcessedAndNotify(ctx context.Context, orderID string, eventType string, message string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// INSERT сразу пытаемся сделать — если конфликт по PRIMARY KEY (order_id, event_type),
	// значит это дубликат, ловим ошибку через pgx.PgError с кодом 23505 (unique_violation).
	_, err = tx.Exec(ctx, `
		INSERT INTO inbox_events (order_id, event_type) VALUES ($1, $2)
	`, orderID, eventType)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ports.ErrAlreadyProcessed
		}
		return fmt.Errorf("insert inbox event: %w", err)
	}

	// "Отправка" уведомления — в проде тут был бы вызов email/push-сервиса.
	// Раз мы решили сохранить транзакционность, эта "отправка" логически внутри tx —
	// хотя реального отката для лога не бывает, паттерн остаётся готовым
	// на будущее (если этот вызов заменится на реальный с возможностью ошибки).
	r.log.Info("notification sent", "order_id", orderID, "event_type", eventType, "message", message)

	return tx.Commit(ctx)
}
