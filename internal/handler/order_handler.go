package handler

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	"echo-shop/internal/auth"
	"echo-shop/internal/model"
	"echo-shop/internal/queue"
	"echo-shop/internal/ws"
)

type eventPublisher interface {
	Publish(ctx context.Context, ev queue.OrderEvent) error
}

type OrderHandler struct {
	db  *gorm.DB
	hub *ws.Hub
	mq  eventPublisher
}

func NewOrderHandler(db *gorm.DB, hub *ws.Hub, mq eventPublisher) *OrderHandler {
	return &OrderHandler{db: db, hub: hub, mq: mq}
}

type createOrderReq struct {
	ProductID uint `json:"product_id"`
	Quantity  int  `json:"quantity"`
}

type updateStatusReq struct {
	Status string `json:"status"`
}

func (h *OrderHandler) Create(c echo.Context) error {
	claims, ok := auth.ClaimsFrom(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
	}
	var req createOrderReq
	if err := c.Bind(&req); err != nil || req.ProductID == 0 || req.Quantity <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "product_id and quantity > 0 are required")
	}

	ctx := c.Request().Context()
	var product model.Product
	if err := h.db.WithContext(ctx).First(&product, req.ProductID).Error; err != nil {
		return queryError(c, err, "product not found")
	}

	order := model.Order{
		UserID:    claims.UserID,
		ProductID: product.ID,
		Quantity:  req.Quantity,
		Total:     product.Price * int64(req.Quantity),
		Status:    model.OrderPending,
	}
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&order).Error; err != nil {
			return err
		}
		return h.mq.Publish(ctx, queue.NewOrderEvent(queue.KeyCreated, order))
	})
	if err != nil {
		log.Println("create order failed:", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "order not accepted, try again")
	}
	return c.JSON(http.StatusAccepted, order)
}

func (h *OrderHandler) UpdateStatus(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid order id")
	}
	var req updateStatusReq
	if err := c.Bind(&req); err != nil || !model.IsValidOrderStatus(req.Status) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid status")
	}

	ctx := c.Request().Context()
	var order model.Order
	if err := h.db.WithContext(ctx).First(&order, id).Error; err != nil {
		return queryError(c, err, "order not found")
	}
	if err := h.db.WithContext(ctx).Model(&order).Update("status", req.Status).Error; err != nil {
		return queryError(c, err, "")
	}
	order.Status = req.Status

	h.hub.SendToUser(order.UserID, ws.Message{
		Type: ws.TypeOrderStatus, OrderID: order.ID, Status: order.Status,
	})
	return c.JSON(http.StatusOK, order)
}

func OrderRoomAuthorizer(gdb *gorm.DB) ws.RoomAuthorizer {
	return func(ctx context.Context, userID uint, role, room string) bool {
		idStr, ok := strings.CutPrefix(room, "order:")
		if !ok {
			return false
		}
		orderID, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return false
		}
		q := gdb.WithContext(ctx).Model(&model.Order{}).Where("id = ?", orderID)
		if role != model.RoleAdmin {
			q = q.Where("user_id = ?", userID)
		}
		var count int64
		return q.Count(&count).Error == nil && count > 0
	}
}

func queryError(c echo.Context, err error, notFoundMsg string) error {
	if notFoundMsg != "" && errors.Is(err, gorm.ErrRecordNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, notFoundMsg)
	}
	c.Logger().Errorf("db: %v", err)
	return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
}
