package domain

// Notification — то, что мы "отправляем" (в pet-проекте — просто логируем).
type Notification struct {
	OrderID string
	Message string
}

// BuildMessage — простое построение текста уведомления по типу события.
// Реальная логика шаблонов/форматирования тут не нужна, просто демонстрация паттерна.
func BuildMessage(eventType string, orderID string, reason string) string {
	switch eventType {
	case "order.created":
		return "Ваш заказ " + orderID + " принят в обработку"
	case "order.confirmed":
		return "Ваш заказ " + orderID + " подтверждён, товар зарезервирован"
	case "order.cancelled":
		return "Ваш заказ " + orderID + " отменён: " + reason
	default:
		return "Обновление по заказу " + orderID
	}
}
