package job

import (
	"context"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	c      *cron.Cron
	locker *Locker
	ctx    context.Context
	cancel context.CancelFunc
}

func NewScheduler(loc *time.Location, locker *Locker) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	logger := cron.VerbosePrintfLogger(log.Default())
	return &Scheduler{
		c: cron.New(
			cron.WithLocation(loc),
			cron.WithChain(cron.Recover(logger), cron.SkipIfStillRunning(logger)),
		),
		locker: locker,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Scheduler) Add(spec, name string, fn func(ctx context.Context) error) error {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}
	ttl := lockTTL(sched, time.Now())
	_, err = s.c.AddFunc(spec, func() {
		ok, err := s.locker.TryLock(s.ctx, name, ttl)
		if err != nil {
			log.Printf("[job %s] lock error, skip this run: %v", name, err)
			return
		}
		if !ok {
			log.Printf("[job %s] running on another instance, skip", name)
			return
		}
		start := time.Now()
		log.Printf("[job %s] start", name)
		if err := fn(s.ctx); err != nil {
			log.Printf("[job %s] failed after %v: %v", name, time.Since(start).Round(time.Millisecond), err)
			return
		}
		log.Printf("[job %s] done in %v", name, time.Since(start).Round(time.Millisecond))
	})
	return err
}

func lockTTL(s cron.Schedule, now time.Time) time.Duration {
	t1 := s.Next(now)
	return s.Next(t1).Sub(t1) * 9 / 10
}

func (s *Scheduler) Start() { s.c.Start() }

func (s *Scheduler) Stop(ctx context.Context) {
	done := s.c.Stop().Done()
	select {
	case <-done:
	case <-ctx.Done():
		s.cancel()
		<-done
	}
	s.cancel()
}
