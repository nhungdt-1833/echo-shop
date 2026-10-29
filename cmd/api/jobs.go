package main

import (
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"echo-shop/internal/export"
	"echo-shop/internal/job"
	"echo-shop/internal/mail"
)

func startJobs(gdb *gorm.DB, rdb *redis.Client) (*job.Scheduler, *export.Service, error) {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		return nil, nil, err
	}
	pending, err := time.ParseDuration(envOr("PENDING_TIMEOUT", "30m"))
	if err != nil {
		return nil, nil, err
	}
	exportDir := envOr("EXPORT_DIR", "exports")
	jobs := &job.Jobs{DB: gdb, RDB: rdb, Mailer: mail.LogMailer{}, Loc: loc, PendingTimeout: pending, ExportDir: exportDir}

	sched := job.NewScheduler(loc, job.NewLocker(rdb))
	if err := sched.Add(envOr("JOB_REPORT_SPEC", "0 7 * * *"), "daily-report", jobs.DailyReport); err != nil {
		return nil, nil, err
	}
	if err := sched.Add(envOr("JOB_CANCEL_SPEC", "@every 5m"), "cancel-expired-orders", jobs.CancelExpiredOrders); err != nil {
		return nil, nil, err
	}
	if err := sched.Add(envOr("JOB_CLEANUP_SPEC", "@weekly"), "weekly-cleanup", jobs.WeeklyCleanup); err != nil {
		return nil, nil, err
	}
	sched.Start()

	slow, _ := time.ParseDuration(envOr("EXPORT_SLOW", "2s"))
	failRate, _ := strconv.ParseFloat(envOr("EXPORT_FAIL_RATE", "0"), 64)
	exports, err := export.NewService(gdb, rdb, exportDir, slow, failRate)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("[jobs] scheduler started (Asia/Ho_Chi_Minh), export pool %d workers / %d queue", export.Workers, export.QueueSize)
	return sched, exports, nil
}
