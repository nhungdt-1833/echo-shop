package handler

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"

	"echo-shop/internal/model"
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

type topItem struct {
	ProductID uint64 `json:"product_id"`
	Sold      int64  `json:"sold"`
}

func NewProductHandler(db *gorm.DB, rdb *redis.Client) *ProductHandler {
	return &ProductHandler{db: db, rdb: rdb}
}

func (h *ProductHandler) Top(c echo.Context) error {
	n, err := strconv.Atoi(c.QueryParam("n"))
	if err != nil || n < 1 || n > 50 {
		n = 5
	}

	ctx := c.Request().Context()
	ranks, err := h.rdb.ZRevRangeWithScores(ctx, "products:top", 0, int64(n-1)).Result()
	if err != nil {
		log.Println("read ranking failed:", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "ranking unavailable")
	}

	items := make([]topItem, 0, len(ranks))
	for _, r := range ranks {
		id, _ := strconv.ParseUint(r.Member.(string), 10, 64)
		items = append(items, topItem{ProductID: id, Sold: int64(r.Score)})
	}

	return c.JSON(http.StatusOK, items)
}

func (h *ProductHandler) List(c echo.Context) error {
	result, err, _ := h.flight.Do("list", func() (any, error) {
		log.Println("go into DB: list products")
		time.Sleep(300 * time.Millisecond)

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
	result, err, _ := h.flight.Do(`get/`+c.Param("id"), func() (any, error) {
		log.Println("go into DB: get products" + c.Param("id"))

		time.Sleep(300 * time.Millisecond)
		var product model.Product
		ctx := context.WithoutCancel(c.Request().Context())
		err := h.db.WithContext(ctx).First(&product, id).Error
		return product, err
	})

	if err != nil {
		return queryError(c, err, "product not found")
	}
	return c.JSON(http.StatusOK, result.(model.Product))
}

func (h *ProductHandler) Update(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid id")
	}

	var req productReq
	if err := c.Bind(&req); err != nil || req.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name is required")
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

	if err := h.rdb.Del(ctx, "cache:/products", "cache:/products/"+c.Param("id")).Err(); err != nil {
		log.Println("delete cache failed", err)
	}
	return c.JSON(http.StatusOK, p)
}

func (h *ProductHandler) Create(c echo.Context) error {
	var input productReq
	if err := c.Bind(&input); err != nil || input.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name is required")
	}

	ctx := c.Request().Context()
	var product model.Product

	product.Name, product.Description, product.Price, product.Quantity = input.Name, input.Description, input.Price, input.Quantity
	if err := h.db.WithContext(ctx).Save(&product).Error; err != nil {
		return queryError(c, err, "")
	}

	if err := h.rdb.Del(ctx, "cache:/products").Err(); err != nil {
		log.Println("delete cache failed", err)
	}
	return c.JSON(http.StatusCreated, product)
}

func (h *ProductHandler) Delete(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid id")
	}

	ctx := c.Request().Context()
	var p model.Product
	if err := h.db.WithContext(ctx).First(&p, id).Error; err != nil {
		return queryError(c, err, "product not found")
	}
	if err := h.db.WithContext(ctx).Delete(&p).Error; err != nil {
		return queryError(c, err, "")
	}

	if err := h.rdb.Del(ctx, "cache:/products", "cache:/products/"+c.Param("id")).Err(); err != nil {
		log.Println("delete cache failed", err)
	}
	return c.NoContent(http.StatusNoContent)
}
