package http

import (
	"KolinMarket/internal/domain"
	"KolinMarket/internal/ports"
	"log/slog"
	"strconv"

	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

type OrderHandler struct {
	uc  ports.OrderUseCase
	log *slog.Logger
}

func NewOrderHandler(uc ports.OrderUseCase, log *slog.Logger) *OrderHandler {
	return &OrderHandler{uc: uc, log: log}
}

func (h *OrderHandler) RegisterRoutes(rg *gin.RouterGroup) {
	orders := rg.Group("/orders")
	{
		orders.POST("", h.CreateOrder)
		orders.GET("/:id", h.GetOrder)
		orders.GET("", h.ListOrders)
	}
}

type createOrderItemRequest struct {
	SKU      string `json:"sku" binding:"required"`
	Name     string `json:"name" binding:"required"`
	Price    int64  `json:"price" binding:"required,gt=0"`
	Currency string `json:"currency" binding:"required,len=3"`
	Quantity int    `json:"quantity" binding:"required,gt=0"`
}

type createOrderRequest struct {
	CustomerID string                   `json:"customer_id" binding:"required,uuid"`
	Items      []createOrderItemRequest `json:"items" binding:"required,min=1,dive"`
}

type orderResponse struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	TotalCost  int64  `json:"total_cost"`
	Currency   string `json:"currency"`
	Status     string `json:"status"`
	Version    int    `json:"version"`
}

func toOrderResponse(o *domain.Order) orderResponse {
	return orderResponse{
		ID:         o.ID,
		CustomerID: o.CustomerID,
		TotalCost:  o.TotalCost.Amount,
		Currency:   o.TotalCost.Currency,
		Status:     string(o.Status),
		Version:    o.Version,
	}
}

func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items := make([]domain.OrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, domain.OrderItem{
			SKU:      it.SKU,
			Name:     it.Name,
			Price:    domain.NewMoney(it.Price, it.Currency),
			Quantity: it.Quantity,
		})
	}

	order, err := h.uc.CreateOrder(c.Request.Context(), ports.CreateOrderInput{
		CustomerID: req.CustomerID,
		Items:      items,
	})
	if err != nil {
		if errors.Is(err, domain.ErrEmptyItems) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		h.log.Error("create order failed", "error", err, "customer_id", req.CustomerID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusCreated, toOrderResponse(order))
}

func (h *OrderHandler) GetOrder(c *gin.Context) {
	id := c.Param("id")

	order, err := h.uc.GetOrder(c.Request.Context(), id)
	if err != nil {
		h.log.Warn("get order failed", "error", err, "order_id", id)
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return
	}

	c.JSON(http.StatusOK, toOrderResponse(order))
}

func (h *OrderHandler) ListOrders(c *gin.Context) {
	customerID := c.Query("customer_id")
	if customerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "customer_id is required"})
		return
	}
	cursor := c.Query("cursor")

	limit, err := strconv.Atoi(c.Query("limit"))
	if err != nil {
		limit = 0
	}

	orders, nextCursor, err := h.uc.ListOrders(c.Request.Context(), customerID, cursor, limit)
	if err != nil {
		h.log.Error("list orders failed", "error", err, "customer_id", customerID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	resp := make([]orderResponse, 0, len(orders))
	for _, o := range orders {
		resp = append(resp, toOrderResponse(o))
	}

	c.JSON(http.StatusOK, gin.H{
		"orders":      resp,
		"next_cursor": nextCursor,
	})
}
