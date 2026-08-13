package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"inventory-service/internal/domain"
	"inventory-service/internal/ports"
)

type stockRepository struct {
	pool *pgxpool.Pool
}

func NewStockRepository(pool *pgxpool.Pool) *stockRepository {
	return &stockRepository{pool: pool}
}

func (r *stockRepository) ReserveAll(ctx context.Context, orderID string, items []ports.ReservationItem) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Проверка идемпотентности — первым делом, внутри той же транзакции.
	var alreadyProcessed bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM processed_events WHERE order_id = $1)
	`, orderID).Scan(&alreadyProcessed)
	if err != nil {
		return fmt.Errorf("check processed: %w", err)
	}
	if alreadyProcessed {
		return ports.ErrAlreadyProcessed
	}

	// 2. Собираем список SKU для батч-чтения через SELECT ... WHERE sku = ANY($1) FOR UPDATE.
	skus := make([]string, 0, len(items))
	qtyBySku := make(map[string]int, len(items))
	for _, it := range items {
		skus = append(skus, it.SKU)
		qtyBySku[it.SKU] = it.Qty
	}

	rows, err := tx.Query(ctx, `
		SELECT sku, available_qty, reserved_qty, version
		FROM stock
		WHERE sku = ANY($1)
		FOR UPDATE
	`, skus)
	if err != nil {
		return fmt.Errorf("select stock for update: %w", err)
	}

	stocks := make(map[string]*domain.Stock)
	for rows.Next() {
		var s domain.Stock
		if err := rows.Scan(&s.SKU, &s.AvailableQty, &s.ReservedQty, &s.Version); err != nil {
			rows.Close()
			return fmt.Errorf("scan stock: %w", err)
		}
		stocks[s.SKU] = &s
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	// 3. Проверяем, что ВСЕ запрошенные SKU реально нашлись и всем хватает stock.
	// Если хотя бы один не найден или недостаточно — вся транзакция откатится (defer Rollback),
	// ни один SKU не будет зарезервирован (all-or-nothing).
	for _, it := range items {
		s, ok := stocks[it.SKU]
		if !ok {
			return fmt.Errorf("%w: sku %s", domain.ErrStockNotFound, it.SKU)
		}
		if s.AvailableQty < it.Qty {
			return fmt.Errorf("%w: sku %s", domain.ErrInsufficientStock, it.SKU)
		}
	}

	// 4. Все проверки прошли — применяем Reserve() к каждому и пишем обратно в БД.
	for _, it := range items {
		s := stocks[it.SKU]
		if err := s.Reserve(it.Qty); err != nil {
			// Не должно случиться, раз мы уже проверили выше, но проверяем на всякий случай
			// (защита от рассинхрона между шагом 3 и шагом 4, если кто-то допишет код между ними).
			return fmt.Errorf("reserve sku %s: %w", it.SKU, err)
		}

		_, err := tx.Exec(ctx, `
			UPDATE stock
			SET available_qty = $1, reserved_qty = $2, version = version + 1, updated_at = now()
			WHERE sku = $3
		`, s.AvailableQty, s.ReservedQty, it.SKU)
		if err != nil {
			return fmt.Errorf("update stock sku %s: %w", it.SKU, err)
		}
	}

	event := domain.StockReservedEvent{OrderID: orderID}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal stock reserved event: %w", err)
	}

	_, err = tx.Exec(ctx, `
	INSERT INTO outbox (aggregate_id, event_type, payload, status, created_at)
	VALUES ($1, $2, $3, 'PENDING', $4)
`, orderID, domain.EventTypeStockReserved, payload, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	// 5. Помечаем order_id как обработанный — тоже в этой же транзакции.
	_, err = tx.Exec(ctx, `
		INSERT INTO processed_events (order_id) VALUES ($1)
	`, orderID)
	if err != nil {
		return fmt.Errorf("insert processed event: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *stockRepository) RecordFailure(ctx context.Context, orderID string, reason string) error {
	event := domain.StockFailedEvent{OrderID: orderID, Reason: reason}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal stock failed event: %w", err)
	}

	// Обычный INSERT без транзакции, без FOR UPDATE — здесь просто фиксируем
	// факт "нужно уведомить об отказе", ничего не резервируем, конфликтов нет.
	_, err = r.pool.Exec(ctx, `
		INSERT INTO outbox (aggregate_id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, 'PENDING', $4)
	`, orderID, domain.EventTypeStockFailed, payload, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert outbox failure event: %w", err)
	}
	return nil
}
