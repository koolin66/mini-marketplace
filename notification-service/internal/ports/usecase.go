package ports

import "context"

type NotificationUseCase interface {
	Notify(ctx context.Context, orderID string, eventType string, reason string) error
}
