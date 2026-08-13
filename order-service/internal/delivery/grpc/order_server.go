package grpc

import (
	"KolinMarket/internal/domain"
	"KolinMarket/internal/ports"
	orderv1 "KolinMarket/proto/order/v1"
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OrderGRPCServer struct {
	orderv1.UnimplementedOrderServiceServer
	uc ports.OrderUseCase
}

func NewOrderGRPCServer(uc ports.OrderUseCase) *OrderGRPCServer {
	return &OrderGRPCServer{uc: uc}
}

func (s *OrderGRPCServer) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.OrderResponse, error) {
	items := make([]domain.OrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, domain.OrderItem{
			SKU:      it.Sku,
			Name:     it.Name,
			Price:    domain.NewMoney(it.Price.Amount, it.Price.Currency),
			Quantity: int(it.Quantity),
		})

	}

	order, err := s.uc.CreateOrder(ctx, ports.CreateOrderInput{
		CustomerID: req.CustomerId,
		Items:      items,
	})
	if err != nil {
		return nil, mapDomainErrToGRPC(err)
	}
	return toOrderResponse(order), nil
}

func (s *OrderGRPCServer) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.OrderResponse, error) {
	order, err := s.uc.GetOrder(ctx, req.Id)
	if err != nil {
		return nil, mapDomainErrToGRPC(err)
	}
	return toOrderResponse(order), nil
}

func (s *OrderGRPCServer) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	orders, nextCursor, err := s.uc.ListOrders(ctx, req.CustomerId, req.Cursor, int(req.Limit))
	if err != nil {
		return nil, mapDomainErrToGRPC(err)
	}

	resp := make([]*orderv1.OrderResponse, 0, len(orders))
	for _, o := range orders {
		resp = append(resp, toOrderResponse(o))
	}

	return &orderv1.ListOrdersResponse{
		Orders:     resp,
		NextCursor: nextCursor,
	}, nil
}

func mapDomainErrToGRPC(err error) error {
	switch {
	case errors.Is(err, domain.ErrEmptyItems):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrOptimisticLock):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func toOrderResponse(o *domain.Order) *orderv1.OrderResponse {
	items := make([]*orderv1.OrderItem, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, &orderv1.OrderItem{
			Sku:      it.SKU,
			Name:     it.Name,
			Price:    &orderv1.Money{Amount: it.Price.Amount, Currency: it.Price.Currency},
			Quantity: int32(it.Quantity),
		})
	}

	return &orderv1.OrderResponse{
		Id:         o.ID,
		CustomerId: o.CustomerID,
		Items:      items,
		TotalCost:  &orderv1.Money{Amount: o.TotalCost.Amount, Currency: o.TotalCost.Currency},
		Status:     string(o.Status),
		Version:    int32(o.Version),
		CreatedAt:  timestamppb.New(o.CreatedAt),
		UpdatedAt:  timestamppb.New(o.UpdatedAt),
	}
}
