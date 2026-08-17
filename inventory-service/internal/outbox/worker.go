package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRecord struct {
	ID          string
	AggregateID string
	EventType   string
	Payload     []byte
}

type Worker struct {
	pool      *pgxpool.Pool
	publisher *Publisher
	interval  time.Duration
	batchSize int
	log       *slog.Logger
}

func NewWorker(pool *pgxpool.Pool, publisher *Publisher, interval time.Duration, batchSize int, log *slog.Logger) *Worker {
	return &Worker{pool: pool, publisher: publisher, interval: interval, batchSize: batchSize, log: log}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("outbox worker stopping")
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				w.log.Error("outbox worker process batch failed", "error", err)
			}
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	records, err := w.fetchPendingTx(ctx, tx)
	if err != nil {
		return err
	}

	for _, rec := range records {
		topic := rec.EventType

		if err := w.publisher.Publish(ctx, rec.AggregateID, topic, rec.Payload); err != nil {
			w.log.Error("outbox worker failed to publish record", "record_id", rec.ID, "error", err)
			return err
		}

		if err := w.markSentTx(ctx, tx, rec.ID); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (w *Worker) fetchPendingTx(ctx context.Context, tx pgx.Tx) ([]OutboxRecord, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, event_type, payload
		FROM outbox
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, w.batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.ID, &r.AggregateID, &r.EventType, &r.Payload); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (w *Worker) markSentTx(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `
		UPDATE outbox SET status = 'SENT', sent_at = $1 WHERE id = $2
	`, time.Now().UTC(), id)
	return err
}
