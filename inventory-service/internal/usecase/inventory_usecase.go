package usecase

import (
	"context"
	"errors"
	"fmt"

	"inventory-service/internal/domain"
	"inventory-service/internal/ports"
)

type inventoryUseCase struct {
	repo ports.StockRepository
}

func NewInventoryUseCase(repo ports.StockRepository) *inventoryUseCase {
	return &inventoryUseCase{repo: repo}
}

func (uc *inventoryUseCase) ReserveStock(ctx context.Context, orderID string, items []ports.ReservationItem) error {
	err := uc.repo.ReserveAll(ctx, orderID, items)
	if err == nil {
		return nil
	}

	// Дубликат — не бизнес-ошибка, просто пробрасываем как есть, RecordFailure не нужен.
	if errors.Is(err, ports.ErrAlreadyProcessed) {
		return err
	}

	// Реальная бизнес-ошибка (нехватка товара/SKU не найден) — записываем stock.failed.
	if errors.Is(err, domain.ErrInsufficientStock) || errors.Is(err, domain.ErrStockNotFound) {
		if recordErr := uc.repo.RecordFailure(ctx, orderID, err.Error()); recordErr != nil {
			// Если даже запись отказа не удалась — это уже серьёзная инфраструктурная проблема,
			// возвращаем recordErr, чтобы consumer НЕ закоммитил offset и попробовал заново.
			return fmt.Errorf("record failure for order %s: %w", orderID, recordErr)
		}
		return err // пробрасываем оригинальную бизнес-ошибку — consumer её залогирует, но offset закоммитит
	}

	// Любая другая (инфраструктурная) ошибка — пробрасываем как есть.
	return fmt.Errorf("reserve stock for order %s: %w", orderID, err)
}
