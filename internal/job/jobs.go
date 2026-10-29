package job

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"echo-shop/internal/mail"
	"echo-shop/internal/model"
	"echo-shop/internal/ws"
	"echo-shop/internal/wsbus"
)

type Jobs struct {
	DB             *gorm.DB
	RDB            *redis.Client
	Mailer         mail.Mailer
	Loc            *time.Location
	PendingTimeout time.Duration
	ExportDir      string
}

func (j *Jobs) DailyReport(ctx context.Context) error {
	now := time.Now().In(j.Loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, j.Loc)
	from := today.AddDate(0, 0, -1)

	var stats struct {
		Orders  int64
		Paid    int64
		Revenue int64
	}
	err := j.DB.WithContext(ctx).Model(&model.Order{}).
		Select(`count(*) AS orders,
			count(*) FILTER (WHERE status = ?) AS paid,
			coalesce(sum(total) FILTER (WHERE status = ?), 0) AS revenue`, model.OrderPaid, model.OrderPaid).
		Where("created_at >= ? AND created_at < ?", from, today).
		Scan(&stats).Error
	if err != nil {
		return err
	}

	var admins []string
	if err := j.DB.WithContext(ctx).Model(&model.User{}).Where("role = ?", model.RoleAdmin).Pluck("email", &admins).Error; err != nil {
		return err
	}
	day := from.Format("02/01/2006")
	body := fmt.Sprintf("Báo cáo ngày %s\n- Số đơn: %d\n- Đã thanh toán: %d\n- Doanh thu: %dđ",
		day, stats.Orders, stats.Paid, stats.Revenue)
	return j.Mailer.Send(ctx, admins, "Báo cáo doanh thu "+day, body)
}

func (j *Jobs) CancelExpiredOrders(ctx context.Context) error {
	var orders []model.Order
	cutoff := time.Now().Add(-j.PendingTimeout)
	if err := j.DB.WithContext(ctx).Where("status = ? AND created_at < ?", model.OrderPending, cutoff).Find(&orders).Error; err != nil {
		return err
	}
	cancelled := 0
	for _, o := range orders {
		res := j.DB.WithContext(ctx).Model(&o).Where("status = ?", model.OrderPending).Update("status", model.OrderCancelled)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			continue
		}
		cancelled++
		msg := ws.Message{Type: ws.TypeOrderStatus, OrderID: o.ID, Status: model.OrderCancelled}
		if err := wsbus.Send(ctx, j.RDB, ws.UserEvent(o.UserID, msg)); err != nil {
			log.Printf("[job cancel] notify order #%d: %v", o.ID, err)
		}
	}
	log.Printf("[job cancel] %d PENDING order(s) older than %v cancelled", cancelled, j.PendingTimeout)
	return nil
}

func (j *Jobs) WeeklyCleanup(ctx context.Context) error {
	files, _ := filepath.Glob(filepath.Join(j.ExportDir, "*.csv"))
	removed := 0
	for _, f := range files {
		if info, err := os.Stat(f); err == nil && time.Since(info.ModTime()) > 7*24*time.Hour {
			if os.Remove(f) == nil {
				removed++
			}
		}
	}
	res := j.DB.WithContext(ctx).Unscoped().
		Where("deleted_at IS NOT NULL AND deleted_at < ?", time.Now().AddDate(0, 0, -30)).
		Delete(&model.Product{})
	if res.Error != nil {
		return res.Error
	}
	log.Printf("[job cleanup] removed %d export file(s), %d old deleted product(s)", removed, res.RowsAffected)
	return nil
}
