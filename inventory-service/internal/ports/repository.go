package ports

import (
	"context"
	"errors"
)

type ReservationItem struct {
	SKU string
	Qty int
}

// ErrAlreadyProcessed — сигнал usecase'у, что это дубликат события, ничего делать не нужно,
// просто подтвердить обработку (commit offset) и идти дальше.
var ErrAlreadyProcessed = errors.New("order already processed")

type StockRepository interface {
	// ReserveAll — единая точка входа: проверяет идемпотентность И резервирует все позиции
	// ОДНОЙ транзакцией. Если orderID уже обработан — возвращает ErrAlreadyProcessed.
	// Если недостаточно стока хотя бы по одной позиции — возвращает domain.ErrInsufficientStock,
	// вся транзакция откатывается (в т.ч. запись в processed_events не создаётся —
	// значит при следующей попытке, если она будет, снова попробуем зарезервировать).
	ReserveAll(ctx context.Context, orderID string, items []ReservationItem) error

	// RecordFailure — самостоятельная запись в outbox о неудачном резервировании.
	// Вызывается ОТДЕЛЬНО от ReserveAll (не в той же транзакции — та уже откатилась),
	// когда ReserveAll вернул domain.ErrInsufficientStock/ErrStockNotFound.
	RecordFailure(ctx context.Context, orderID string, reason string) error
}

type InventoryUseCase interface {
	ReserveStock(ctx context.Context, orderID string, items []ReservationItem) error
}
