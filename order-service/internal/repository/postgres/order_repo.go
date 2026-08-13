package postgres

import (
	"KolinMarket/internal/domain"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type orderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *orderRepository {
	return &orderRepository{pool: pool}
}

func (r *orderRepository) SaveWithEvent(ctx context.Context, order *domain.Order, eventType string, payload []byte) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, customer_id, total_amount, currency, status, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, order.ID, order.CustomerID, order.TotalCost.Amount, order.TotalCost.Currency,
		order.Status, order.Version, order.CreatedAt, order.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	batch := &pgx.Batch{}
	for _, item := range order.Items {
		batch.Queue(`
			INSERT INTO order_items (order_id, sku, name, price, currency, quantity)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, order.ID, item.SKU, item.Name, item.Price.Amount, item.Price.Currency, item.Quantity)
	}
	br := tx.SendBatch(ctx, batch)
	for range order.Items {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("insert order item: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}

	// Тот самый ключевой INSERT — outbox-запись в ТОЙ ЖЕ транзакции.
	// Если этот INSERT упадёт — откатится ВСЁ (включая orders/order_items) через defer Rollback.
	// Именно это и даёт атомарность: либо заказ+событие оба сохранились, либо ни то ни другое.
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, 'PENDING', $4)
	`, order.ID, eventType, payload, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
func (r *orderRepository) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	row := r.pool.QueryRow(ctx, `SELECT id, customer_id, total_amount, currency, status, version, created_at, updated_at
		FROM orders WHERE id = $1`, id)

	var o domain.Order
	var currency string
	err := row.Scan(&o.ID, &o.CustomerID, &o.TotalCost.Amount, &currency,
		&o.Status, &o.Version, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("Заказ не найден: %w", err)
		}
		return nil, fmt.Errorf("Сканирование заказа: %w", err)
	}

	o.TotalCost.Currency = currency
	rows, err := r.pool.Query(ctx, `
		SELECT sku, name, price, currency, quantity FROM order_items WHERE order_id = $1
	`, id)
	if err != nil {
		return nil, fmt.Errorf("Запрос на товары из заказа: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item domain.OrderItem
		if err := rows.Scan(&item.SKU, &item.Name, &item.Price.Amount, &item.Price.Currency, &item.Quantity); err != nil {
			return nil, fmt.Errorf("Сканирование товаров: %w", err)
		}
		o.Items = append(o.Items, item)
	}

	return &o, nil
}

func (r *orderRepository) UpdateStatusWithEvent(ctx context.Context, order *domain.Order, eventType string, payload []byte) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Тот же UPDATE с optimistic lock проверкой, что был раньше — просто теперь внутри tx.
	tag, err := tx.Exec(ctx, `
		UPDATE orders
		SET status = $1, version = version + 1, updated_at = $2
		WHERE id = $3 AND version = $4
	`, order.Status, time.Now().UTC(), order.ID, order.Version)
	if err != nil {
		return fmt.Errorf("update order status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOptimisticLock
	}

	// Тот же INSERT в outbox, что уже делали в SaveWithEvent — теперь и здесь.
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, 'PENDING', $4)
	`, order.ID, eventType, payload, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *orderRepository) ListByCustomer(ctx context.Context, customerID, cursor string, limit int) ([]*domain.Order, string, error) {
	var (
		rows pgx.Rows
		err  error
	)

	if cursor == "" {
		rows, err = r.pool.Query(ctx, `
		SELECT id, customer_id, total_amount, currency, status, version, created_at, updated_at
			FROM orders
			WHERE customer_id = $1
			ORDER BY created_at DESC, id DESC
			LIMIT $2 `, customerID, limit)
	} else {
		createdAt, id, decodeErr := decodeCursor(cursor)
		if decodeErr != nil {
			return nil, "", fmt.Errorf("Декодирование курсора: %w", decodeErr)
		}
		rows, err = r.pool.Query(ctx, `
			SELECT id, customer_id, total_amount, currency, status, version, created_at, updated_at
			FROM orders
			WHERE customer_id = $1 AND (created_at, id) < ($2, $3)
			ORDER BY created_at DESC, id DESC
			LIMIT $4
		`, customerID, createdAt, id, limit)
	}
	if err != nil {
		return nil, "", fmt.Errorf("Запрос заказов: %w", err)
	}
	defer rows.Close()

	var orders []*domain.Order
	for rows.Next() {
		var o domain.Order
		var currency string
		if err := rows.Scan(&o.ID, &o.CustomerID, &o.TotalCost.Amount, &currency,
			&o.Status, &o.Version, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, "", fmt.Errorf("scan order: %w", err)
		}
		o.TotalCost.Currency = currency
		orders = append(orders, &o)
	}
	var nextCursor string
	if len(orders) == limit {
		last := orders[len(orders)-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
	}

	return orders, nextCursor, nil
}

// кодирование курсора
func encodeCursor(createdAt time.Time, id string) string {
	raw := fmt.Sprintf("%s|%s", createdAt.Format(time.RFC3339Nano), id)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("невалидный формат курсора")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", err
	}
	return t, parts[1], nil
}
