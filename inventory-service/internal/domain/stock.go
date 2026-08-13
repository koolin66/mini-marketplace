package domain

import "errors"

var (
	ErrInsufficientStock = errors.New("insufficient stock available")
	ErrStockNotFound     = errors.New("stock not found for sku")
)

type Stock struct {
	SKU          string
	AvailableQty int
	ReservedQty  int
	Version      int
}

// Reserve — доменный метод, инкапсулирует бизнес-правило "нельзя зарезервировать
// больше, чем есть в наличии". Аналог TransitionTo/NewOrder в Order Service —
// инвариант живёт в domain, не в usecase и не в SQL-запросе.
func (s *Stock) Reserve(qty int) error {
	if qty <= 0 {
		return errors.New("reserve quantity must be positive")
	}
	if s.AvailableQty < qty {
		return ErrInsufficientStock
	}

	s.AvailableQty -= qty
	s.ReservedQty += qty
	return nil
}

// Release — компенсирующая операция (понадобится для Saga: если Order отменяется
// уже после того, как товар был зарезервирован — возвращаем его обратно в available).
func (s *Stock) Release(qty int) error {
	if qty <= 0 {
		return errors.New("release quantity must be positive")
	}
	if s.ReservedQty < qty {
		return errors.New("cannot release more than reserved")
	}

	s.ReservedQty -= qty
	s.AvailableQty += qty
	return nil
}
