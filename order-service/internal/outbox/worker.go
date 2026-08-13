package outbox

import (
	"KolinMarket/internal/domain"
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRecord — то, что воркер читает из таблицы outbox.
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
}

func NewWorker(pool *pgxpool.Pool, publisher *Publisher, interval time.Duration, batchSize int) *Worker {
	return &Worker{
		pool:      pool,
		publisher: publisher,
		interval:  interval,
		batchSize: batchSize,
	}
}

// Run — блокирующий цикл. Вызывается в main, останавливается через ctx.Done() (graceful shutdown).
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("outbox worker: stopping")
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				log.Printf("outbox worker: process batch error: %v", err)
				// Не падаем, не паникуем — просто логируем и ждём следующий тик.
				// Временная ошибка (например Kafka недоступна) не должна убить весь воркер.
			}
		}
	}
}

// processBatch — ИЗМЕНЕНО ПОЛНОСТЬЮ: раньше это были три независимых похода в БД
// (fetchPending -> Publish -> markSent, каждый в своей транзакции/без транзакции).
// Теперь всё чтение+блокировка+обновление статуса происходит В ОДНОЙ транзакции,
// чтобы FOR UPDATE держал блокировку до момента, пока мы не опубликуем событие
// и не пометим его SENT — иначе второй воркер мог бы перехватить запись
// в промежутке между чтением и записью.
func (w *Worker) processBatch(ctx context.Context) error {
	tx, err := w.pool.Begin(ctx) // НОВОЕ: открываем транзакцию на весь батч
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // НОВОЕ: если что-то пойдёт не так — блокировка снимется, запись останется PENDING

	records, err := w.fetchPendingTx(ctx, tx) // ИЗМЕНЕНО: fetchPending теперь принимает tx, не pool напрямую
	if err != nil {
		return err
	}

	for _, rec := range records {
		topic := rec.EventType

		if err := w.publisher.Publish(ctx, rec.AggregateID, topic, rec.Payload); err != nil {
			log.Printf("outbox worker: failed to publish record %s: %v", rec.ID, err)
			// НОВОЕ: раньше был continue (пропустить и попробовать следующую запись).
			// Теперь, раз мы внутри ОДНОЙ транзакции на весь батч, ошибка публикации
			// одной записи должна прервать всю транзакцию — иначе она закоммитится
			// частично прочитанной, а FOR UPDATE блокировка снимется, но часть
			// записей уже помечена SENT, а часть нет — рассинхрон середины батча.
			// Проще и надёжнее: одна ошибка публикации — откатываем весь батч целиком,
			// на следующем тике попробуем заново (at-least-once, уже обсуждали).
			return err
		}

		if err := w.markSentTx(ctx, tx, rec.ID); err != nil { // ИЗМЕНЕНО: markSent теперь тоже внутри tx
			return err
		}
		// НОВОЕ: после успешной публикации order.created — переводим заказ в AWAITING_STOCK.
		// Обрати внимание: это происходит ПОСЛЕ Publish (событие точно ушло), но пока
		// мы ещё в той же транзакции, что markSent — статус и SENT фиксируются атомарно вместе.
		if rec.EventType == domain.EventTypeOrderCreated {
			if err := w.transitionToAwaitingStockTx(ctx, tx, rec.AggregateID); err != nil {
				log.Printf("outbox worker: failed to transition order %s to AWAITING_STOCK: %v", rec.AggregateID, err)
				return err
			}
		}
	}

	return tx.Commit(ctx) // НОВОЕ: коммитим только если ВСЕ записи батча успешно опубликованы и помечены
}

// fetchPendingTx — ИЗМЕНЕНО: было fetchPending(ctx) с w.pool.Query,
// стало fetchPendingTx(ctx, tx) с tx.Query + FOR UPDATE SKIP LOCKED.
func (w *Worker) fetchPendingTx(ctx context.Context, tx pgx.Tx) ([]OutboxRecord, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, event_type, payload
		FROM outbox
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, w.batchSize) // НОВОЕ: FOR UPDATE SKIP LOCKED — блокирует выбранные строки,
	// пропускает те, что уже заблокированы другим воркером, вместо ожидания/ошибки
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

// markSentTx — ИЗМЕНЕНО: было markSent(ctx, id) с w.pool.Exec,
// стало markSentTx(ctx, tx, id) с tx.Exec — обновление статуса в ТОЙ ЖЕ транзакции,
// что и SELECT FOR UPDATE, чтобы блокировка держалась до самого коммита.
func (w *Worker) markSentTx(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `
		UPDATE outbox SET status = 'SENT', sent_at = $1 WHERE id = $2
	`, time.Now().UTC(), id)
	return err
}

// transitionToAwaitingStockTx — НОВОЕ. Прямой SQL, а не через usecase.UpdateOrderStatus —
// потому что usecase-метод сам открывает СВОЮ транзакцию (через repo.GetByID + repo.UpdateStatus
// с retry-циклом), а нам здесь нужно остаться строго внутри уже открытой tx воркера.
// Поэтому логику FSM-перехода дублируем здесь по минимуму: транзакция не найдёт нужную
// строку (WHERE status = 'CREATED'), если переход невалиден — 0 rows affected, просто лог.
func (w *Worker) transitionToAwaitingStockTx(ctx context.Context, tx pgx.Tx, orderID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE orders
		SET status = $1, version = version + 1, updated_at = $2
		WHERE id = $3 AND status = $4
	`, domain.StatusAwaitingStock, time.Now().UTC(), orderID, domain.StatusCreated)
	return err
}
