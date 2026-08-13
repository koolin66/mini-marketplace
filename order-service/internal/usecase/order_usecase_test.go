package usecase

import (
	"KolinMarket/internal/domain"
	"KolinMarket/internal/ports"
	"KolinMarket/internal/ports/mocks"
	"encoding/json"

	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestOrderUseCase_CreateOrder(t *testing.T) {
	tests := []struct {
		name        string
		input       ports.CreateOrderInput
		setupMock   func(t *testing.T, repo *mocks.MockOrderRepository)
		wantErr     error
		checkResult func(t *testing.T, order *domain.Order)
	}{
		{
			name: "успешное создание заказа",
			input: ports.CreateOrderInput{
				CustomerID: "customer-1",
				Items: []domain.OrderItem{
					{SKU: "SKU-1", Name: "Item", Price: domain.NewMoney(1000, "USD"), Quantity: 2},
				},
			},
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				repo.EXPECT().
					SaveWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, order *domain.Order, eventType string, payload []byte) error {
						assert.Equal(t, domain.EventTypeOrderCreated, eventType)

						var event domain.OrderCreatedEvent
						err := json.Unmarshal(payload, &event)
						require.NoError(t, err)
						assert.Equal(t, order.ID, event.OrderID)
						assert.Equal(t, order.CustomerID, event.CustomerID)
						assert.Equal(t, int64(2000), event.TotalCost.Amount)

						return nil
					}).
					Times(1)
			},
			wantErr: nil,
			checkResult: func(t *testing.T, order *domain.Order) {
				assert.Equal(t, int64(2000), order.TotalCost.Amount)
				assert.Equal(t, domain.StatusCreated, order.Status)
				assert.Equal(t, 1, order.Version)
			},
		},
		{
			name: "пустое поле с заказами возвращает domain error, бд не трогается",
			input: ports.CreateOrderInput{
				CustomerID: "customer-1",
				Items:      []domain.OrderItem{},
			},
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				repo.EXPECT().SaveWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErr: domain.ErrEmptyItems,
		},
		{
			name: "ошибка репозитория пробрасывается наружу",
			input: ports.CreateOrderInput{
				CustomerID: "customer-1",
				Items: []domain.OrderItem{
					{SKU: "SKU-1", Name: "Item", Price: domain.NewMoney(1000, "USD"), Quantity: 1},
				},
			},
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				repo.EXPECT().
					SaveWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("connection refused")).
					Times(1)
			},
			wantErr: errors.New("connection refused"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockRepo := mocks.NewMockOrderRepository(ctrl)
			tt.setupMock(t, mockRepo)
			uc := NewOrderUseCase(mockRepo)

			order, err := uc.CreateOrder(context.Background(), tt.input)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr.Error())
				return
			}
			require.NoError(t, err)
			require.NotNil(t, order)
			tt.checkResult(t, order)
		})
	}
}
func TestOrderUseCase_UpdateOrderStatus(t *testing.T) {
	tests := []struct {
		name        string
		orderID     string
		newStatus   domain.OrderStatus
		setupMock   func(t *testing.T, repo *mocks.MockOrderRepository)
		wantErr     bool
		expectedErr error
		errContains string
	}{
		{
			name:      "успешное изменение",
			orderID:   "1",
			newStatus: domain.StatusCompleted,
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				existingOrder := &domain.Order{
					ID:      "1",
					Status:  domain.StatusConfirmed,
					Version: 1,
				}
				repo.EXPECT().GetByID(gomock.Any(), "1").Return(existingOrder, nil).Times(1)
				repo.EXPECT().
					UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, order *domain.Order) error {
						assert.Equal(t, domain.StatusCompleted, order.Status)
						return nil
					}).Times(1)
			},
			wantErr:     false,
			expectedErr: nil,
		},
		{
			name: "появляется один ретрай из за Optimistic lock",
			// GetByID вызывается ДВАЖДЫ, UpdateStatus — первый раз ErrOptimisticLock, второй — nil
			orderID:   "1",
			newStatus: domain.StatusCompleted,
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				firstCall := &domain.Order{ID: "1", Status: domain.StatusConfirmed, Version: 1}
				secondCall := &domain.Order{ID: "1", Status: domain.StatusConfirmed, Version: 1}

				getCall1 := repo.EXPECT().GetByID(gomock.Any(), "1").Return(firstCall, nil)
				updateCall1 := repo.EXPECT().UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(domain.ErrOptimisticLock)
				getCall2 := repo.EXPECT().GetByID(gomock.Any(), "1").Return(secondCall, nil)
				updateCall2 := repo.EXPECT().UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				gomock.InOrder(getCall1, updateCall1, getCall2, updateCall2)
			},
			wantErr:     false,
			expectedErr: nil,
		},
		{
			name: "невалидный переход возвращает ошибку, updateStatus не вызывается",
			// например CREATED -> COMPLETED напрямую — TransitionTo вернёт ErrInvalidTransition
			// ДО похода в repo.UpdateStatus
			orderID:   "1",
			newStatus: domain.StatusCompleted,
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				existingOrder := &domain.Order{
					ID:      "1",
					Status:  domain.StatusCreated,
					Version: 1,
				}
				repo.EXPECT().GetByID(gomock.Any(), "1").Return(existingOrder, nil).Times(1)
				repo.EXPECT().UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(0)
			},
			wantErr:     true,
			expectedErr: domain.ErrInvalidTransition,
		},
		{
			name: "использованы все попытки записи в базу изменения статуса",
			// UpdateStatus ВСЕГДА возвращает ErrOptimisticLock — 3 попытки, потом сдаёмся
			orderID:   "1",
			newStatus: domain.StatusAwaitingStock,
			setupMock: func(t *testing.T, repo *mocks.MockOrderRepository) {
				existingOrder1 := &domain.Order{
					ID:      "1",
					Status:  domain.StatusCreated,
					Version: 1,
				}
				existingOrder2 := &domain.Order{
					ID:      "1",
					Status:  domain.StatusCreated,
					Version: 1,
				}
				existingOrder3 := &domain.Order{
					ID:      "1",
					Status:  domain.StatusCreated,
					Version: 1,
				}
				getCall1 := repo.EXPECT().GetByID(gomock.Any(), "1").Return(existingOrder1, nil).Times(1)
				getCall2 := repo.EXPECT().GetByID(gomock.Any(), "1").Return(existingOrder2, nil).Times(1)
				getCall3 := repo.EXPECT().GetByID(gomock.Any(), "1").Return(existingOrder3, nil).Times(1)
				updateCall1 := repo.EXPECT().UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(domain.ErrOptimisticLock).Times(1)
				updateCall2 := repo.EXPECT().UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(domain.ErrOptimisticLock).Times(1)
				updateCall3 := repo.EXPECT().UpdateStatusWithEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(domain.ErrOptimisticLock).Times(1)

				gomock.InOrder(getCall1, updateCall1, getCall2, updateCall2, getCall3, updateCall3)
			},
			wantErr:     true,
			expectedErr: nil,
			errContains: "было использовано 3 попыток",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: что здесь нужно создать/подготовить?
			ctrl := gomock.NewController(t)
			mockRepo := mocks.NewMockOrderRepository(ctrl)
			uc := NewOrderUseCase(mockRepo)
			tt.setupMock(t, mockRepo)

			// Act: какой вызов?
			err := uc.UpdateOrderStatus(t.Context(), tt.orderID, tt.newStatus, "")

			// Assert: что проверяем?
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != nil {
					assert.ErrorIs(t, err, tt.expectedErr)
				}
				if tt.errContains != "" {
					assert.ErrorContains(t, err, tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
		// ...
	}
}
