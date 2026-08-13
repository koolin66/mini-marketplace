package usecase

import (
	"context"

	"notification-service/internal/domain"
	"notification-service/internal/ports"
)

type notificationUseCase struct {
	repo ports.InboxRepository
}

func NewNotificationUseCase(repo ports.InboxRepository) *notificationUseCase {
	return &notificationUseCase{repo: repo}
}

func (uc *notificationUseCase) Notify(ctx context.Context, orderID string, eventType string, reason string) error {
	message := domain.BuildMessage(eventType, orderID, reason)
	return uc.repo.MarkProcessedAndNotify(ctx, orderID, eventType, message)
}
