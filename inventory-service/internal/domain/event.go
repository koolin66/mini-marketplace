package domain

const (
	EventTypeStockReserved = "stock.reserved"
	EventTypeStockFailed   = "stock.failed"
)

// StockReservedEvent — публикуется, когда резервирование прошло успешно.
type StockReservedEvent struct {
	OrderID string `json:"order_id"`
}

// StockFailedEvent — публикуется, когда резервирование не удалось (нехватка товара
// или SKU не найден). Reason — человекочитаемая причина, пригодится в логах Order Service.
type StockFailedEvent struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}
