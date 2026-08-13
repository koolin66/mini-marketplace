package ports

import (
	"context"
	"errors"
)

// ErrAlreadyProcessed — сигнал usecase'у, что событие уже обработано (идемпотентность).
var ErrAlreadyProcessed = errors.New("notification already processed")

type InboxRepository interface {
	// MarkProcessedAndNotify — атомарно проверяет идемпотентность (по orderID) и,
	// если ещё не обработано, вставляет запись в inbox_events + "отправляет" уведомление
	// (в нашем случае — просто лог) в рамках одной транзакции.
	// Если orderID уже был обработан — возвращает ErrAlreadyProcessed.
	MarkProcessedAndNotify(ctx context.Context, orderID string, eventType string, message string) error
}
