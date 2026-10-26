package handler

import (
	"context"
	"echo-shop/internal/model"
	"log"
	"net/http"
	"strconv"
	"time"

	"gorm.io/gorm"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type ProductHandler struct {
	db     *gorm.DB
	rdb    *redis.Client
	flight singleflight.Group
}

type productReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Price       int64  `json:"price"`
	Quantity    int    `json:"quantity"`
}

func NewProductHandler(db *gorm.DB, rdb *redis.Client) *ProductHandler {
	return &ProductHandler{db: db, rdb: rdb}
}

func (h *ProductHandler) List(c echo.Context) error {
	result, err, _ := h.flight.Do("list", func() (any, error) {
		log.Println("đi vào DB: list products")
		time.Sleep((300 * time.Millisecond))

		var products []model.Product
		ctx := context.WithoutCancel(c.Request().Context())
		err := h.db.WithContext(ctx).Find((&products)).Error
		return products, err
	})

	if err != nil {
		return queryError(c, err, "")
	}
	return c.JSON(http.StatusOK, result.([]model.Product))
}

func (h *ProductHandler) Get(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid id")
	}
	time.Sleep(300 * time.Millisecond)

	var p model.Product
	if err := h.db.WithContext(c.Request().Context()).First(&p, id).Error; err != nil {
		return queryError(c, err, "product not found")
	}
	return c.JSON(http.StatusOK, p)
}

func (h *ProductHandler) Update(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid id")
	}

	var req productReq
	if err := c.Bind(&req); err != nil || req.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name is require")
	}

	ctx := c.Request().Context()
	var p model.Product
	if err := h.db.WithContext(ctx).First(&p, id).Error; err != nil {
		return queryError(c, err, "product not found")
	}
	p.Name, p.Description, p.Price, p.Quantity = req.Name, req.Description, req.Price, req.Quantity
	if err := h.db.WithContext(ctx).Save(&p).Error; err != nil {
		return queryError(c, err, "")
	}
	if err := h.rdb.Del(ctx, "cache:/products", "cache:/products/"+c.Param("id")); err != nil {
		log.Println("delete cache failed", err)
	}
	return c.JSON(http.StatusOK, p)
}
