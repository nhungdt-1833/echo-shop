package handler

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"echo-shop/internal/auth"
	"echo-shop/internal/export"
	"echo-shop/internal/job"
)

type ExportHandler struct {
	svc *export.Service
}

func NewExportHandler(svc *export.Service) *ExportHandler { return &ExportHandler{svc: svc} }

func (h *ExportHandler) Create(c echo.Context) error {
	claims, ok := auth.ClaimsFrom(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
	}
	st, err := h.svc.Submit(c.Request().Context(), claims.UserID)
	switch {
	case errors.Is(err, job.ErrQueueFull), errors.Is(err, job.ErrStopped):
		c.Response().Header().Set("Retry-After", "10")
		return echo.NewHTTPError(http.StatusServiceUnavailable, "export queue is full, try again later")
	case err != nil:
		c.Logger().Errorf("submit export: %v", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "export unavailable")
	}
	return c.JSON(http.StatusAccepted, st)
}

func (h *ExportHandler) Show(c echo.Context) error {
	st, err := h.svc.Get(c.Request().Context(), c.Param("job_id"))
	switch {
	case errors.Is(err, export.ErrNotFound):
		return echo.NewHTTPError(http.StatusNotFound, "export not found")
	case err != nil:
		return echo.NewHTTPError(http.StatusServiceUnavailable, "export status unavailable")
	}
	return c.JSON(http.StatusOK, st)
}

func (h *ExportHandler) Download(c echo.Context) error {
	id := c.Param("job_id")
	st, err := h.svc.Get(c.Request().Context(), id)
	if errors.Is(err, export.ErrNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "export not found")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "export status unavailable")
	}
	if st.Status != export.StatusDone {
		return echo.NewHTTPError(http.StatusConflict, "export is "+st.Status)
	}
	return c.Attachment(h.svc.File(id), "orders-"+id+".csv")
}
