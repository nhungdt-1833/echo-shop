package handler

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"echo-shop/internal/auth"
	"echo-shop/internal/model"
	"echo-shop/internal/ws"
)

type OrderHandler struct {
	db  *gorm.DB
	hub *ws.Hub
	rdb *redis.Client
}

func NewOrderHandler(db *gorm.DB, hub *ws.Hub, rdb *redis.Client) *OrderHandler {
	return &OrderHandler{db: db, hub: hub, rdb: rdb}
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
	if err := h.db.WithContext(ctx).Create(&order).Error; err != nil {
		return queryError(c, err, "")
	}
	if err := h.rdb.ZIncrBy(ctx, "products:top", float64(order.Quantity), strconv.FormatUint(uint64(product.ID), 10)).Err(); err != nil {
		log.Println("update ranking failed:", err)
	}

	h.hub.SendToRole(model.RoleAdmin, ws.Message{
		Type: ws.TypeNewOrder, OrderID: order.ID, UserID: order.UserID,
		Total: order.Total, Status: order.Status,
	})
	return c.JSON(http.StatusCreated, order)
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
