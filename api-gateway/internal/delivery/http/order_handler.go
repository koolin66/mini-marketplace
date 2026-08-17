package http

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"api-gateway/internal/cache"
	"api-gateway/internal/ports"
	orderv1 "api-gateway/proto/order/v1"
)

type OrderHandler struct {
	orderClient ports.OrderClient
	cache       *cache.RedisCache
	log         *slog.Logger
}

func NewOrderHandler(orderClient ports.OrderClient, cache *cache.RedisCache, log *slog.Logger) *OrderHandler {
	return &OrderHandler{orderClient: orderClient, cache: cache, log: log}
}

func (h *OrderHandler) RegisterRoutes(rg *gin.RouterGroup) {
	orders := rg.Group("/orders")
	{
		orders.POST("", h.CreateOrder)
		orders.GET("/:id", h.GetOrder)
		orders.GET("", h.ListOrders)
	}
}

// --- Request DTO (уже было)

type createOrderItemRequest struct {
	SKU      string `json:"sku" binding:"required"`
	Name     string `json:"name" binding:"required"`
	Price    int64  `json:"price" binding:"required,gt=0"`
	Currency string `json:"currency" binding:"required,len=3"`
	Quantity int32  `json:"quantity" binding:"required,gt=0"`
}

type createOrderRequest struct {
	CustomerID string                   `json:"customer_id" binding:"required,uuid"`
	Items      []createOrderItemRequest `json:"items" binding:"required,min=1,dive"`
}

// --- Response DTO — публичный контракт Gateway, независимый от internal protobuf-схемы.

type orderItemResponse struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Price    int64  `json:"price"`
	Currency string `json:"currency"`
	Quantity int32  `json:"quantity"`
}

type orderResponse struct {
	ID         string              `json:"id"`
	CustomerID string              `json:"customer_id"`
	Items      []orderItemResponse `json:"items"`
	TotalCost  int64               `json:"total_cost"`
	Currency   string              `json:"currency"`
	Status     string              `json:"status"`
	Version    int32               `json:"version"`
	CreatedAt  string              `json:"created_at"` // RFC3339, конвертируем сами из timestamppb
	UpdatedAt  string              `json:"updated_at"`
}

type listOrdersResponse struct {
	Orders     []orderResponse `json:"orders"`
	NextCursor string          `json:"next_cursor"`
}

// toOrderResponse — маппинг protobuf OrderResponse -> Gateway DTO.
// Именно здесь и происходит развязка публичного контракта от внутреннего gRPC-контракта.
func toOrderResponse(o *orderv1.OrderResponse) orderResponse {
	items := make([]orderItemResponse, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, orderItemResponse{
			SKU:      it.Sku,
			Name:     it.Name,
			Price:    it.Price.Amount,
			Currency: it.Price.Currency,
			Quantity: it.Quantity,
		})
	}

	return orderResponse{
		ID:         o.Id,
		CustomerID: o.CustomerId,
		Items:      items,
		TotalCost:  o.TotalCost.Amount,
		Currency:   o.TotalCost.Currency,
		Status:     o.Status,
		Version:    o.Version,
		CreatedAt:  o.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"), // RFC3339
		UpdatedAt:  o.UpdatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"),
	}
}

// CreateOrder godoc
// @Summary      Создать новый заказ
// @Description  Создаёт заказ и запускает Saga-процесс резервирования товара через Inventory Service.
// @Tags         orders
// @Accept       json
// @Produce      json
// @Param        request  body      createOrderRequest  true  "Данные заказа"
// @Success      201      {object}  orderResponse
// @Failure      400      {object}  map[string]string  "невалидные данные запроса"
// @Failure      500      {object}  map[string]string  "внутренняя ошибка"
// @Router       /orders [post]
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items := make([]*orderv1.OrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, &orderv1.OrderItem{
			Sku:      it.SKU,
			Name:     it.Name,
			Price:    &orderv1.Money{Amount: it.Price, Currency: it.Currency},
			Quantity: it.Quantity,
		})
	}

	resp, err := h.orderClient.CreateOrder(c.Request.Context(), &orderv1.CreateOrderRequest{
		CustomerId: req.CustomerID,
		Items:      items,
	})
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	c.JSON(http.StatusCreated, toOrderResponse(resp))
}

// GetOrder godoc
// @Summary      Получить заказ по ID
// @Description  Возвращает данные заказа. Использует cache-aside через Redis (TTL 5с).
// @Tags         orders
// @Produce      json
// @Param        id   path      string  true  "ID заказа (UUID)"
// @Success      200  {object}  orderResponse
// @Failure      404  {object}  map[string]string  "заказ не найден"
// @Failure      500  {object}  map[string]string  "внутренняя ошибка"
// @Router       /orders/{id} [get]
func (h *OrderHandler) GetOrder(c *gin.Context) {
	id := c.Param("id")
	cacheKey := "order:" + id

	// 1. Пробуем взять из кэша.
	var cached orderResponse
	found, err := h.cache.Get(c.Request.Context(), cacheKey, &cached)
	if err != nil {
		// Redis недоступен — НЕ падаем, просто логируем и идём в Order Service напрямую.
		// Кэш — это оптимизация, а не источник правды; его недоступность не должна
		// ронять основной функционал.
		h.log.Warn("cache get error", "error", err)
	} else if found {
		c.JSON(http.StatusOK, cached)
		return
	}

	// 2. Cache miss (или Redis недоступен) — идём в Order Service как раньше.
	resp, err := h.orderClient.GetOrder(c.Request.Context(), &orderv1.GetOrderRequest{Id: id})
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	result := toOrderResponse(resp)

	// 3. Кладём в кэш для следующих запросов. Ошибку тоже не считаем фатальной.
	if err := h.cache.Set(c.Request.Context(), cacheKey, result); err != nil {
		h.log.Warn("cache set error", "error", err)
	}

	c.JSON(http.StatusOK, result)
}

// ListOrders godoc
// @Summary      Список заказов клиента
// @Description  Возвращает заказы клиента с курсорной пагинацией.
// @Tags         orders
// @Produce      json
// @Param        customer_id  query     string  true   "ID клиента (UUID)"
// @Param        cursor       query     string  false  "Курсор пагинации (из предыдущего ответа)"
// @Success      200          {object}  listOrdersResponse
// @Failure      400          {object}  map[string]string  "customer_id не указан"
// @Failure      500          {object}  map[string]string  "внутренняя ошибка"
// @Router       /orders [get]
func (h *OrderHandler) ListOrders(c *gin.Context) {
	customerID := c.Query("customer_id")
	if customerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "customer_id is required"})
		return
	}

	resp, err := h.orderClient.ListOrders(c.Request.Context(), &orderv1.ListOrdersRequest{
		CustomerId: customerID,
		Cursor:     c.Query("cursor"),
		Limit:      20,
	})
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	orders := make([]orderResponse, 0, len(resp.Orders))
	for _, o := range resp.Orders {
		orders = append(orders, toOrderResponse(o))
	}

	c.JSON(http.StatusOK, listOrdersResponse{
		Orders:     orders,
		NextCursor: resp.NextCursor,
	})
}

func writeGRPCError(c *gin.Context, err error) {
	st, ok := status.FromError(err)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	var httpCode int
	switch st.Code() {
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.FailedPrecondition:
		httpCode = http.StatusConflict
	case codes.Aborted:
		httpCode = http.StatusConflict
	case codes.Unavailable:
		httpCode = http.StatusServiceUnavailable
	default:
		httpCode = http.StatusInternalServerError
	}

	c.JSON(httpCode, gin.H{"error": st.Message()})
}
