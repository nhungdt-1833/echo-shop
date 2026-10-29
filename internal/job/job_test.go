package job

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

type failures struct {
	mu  sync.Mutex
	ids map[string]error
}

func (f *failures) record(id string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids[id] = err
}

func (f *failures) get(id string) (error, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	err, ok := f.ids[id]
	return err, ok
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func newPool(size, queue, retries int) (*Pool, *failures) {
	f := &failures{ids: map[string]error{}}
	return NewPool(size, queue, retries, time.Millisecond, Hooks{OnFail: f.record}), f
}

func TestPoolBackpressure(t *testing.T) {
	p, _ := newPool(5, 50, 0)
	release := make(chan struct{})
	var running atomic.Int32
	slow := func(ctx context.Context, _ int) error {
		running.Add(1)
		<-release
		return nil
	}

	for i := range 5 {
		if err := p.Submit(Task{ID: string(rune('a' + i)), Run: slow}); err != nil {
			t.Fatal(err)
		}
	}
	for running.Load() < 5 {
		time.Sleep(time.Millisecond)
	}
	accepted, rejected := 5, 0
	for range 55 {
		if err := p.Submit(Task{ID: "x", Run: slow}); errors.Is(err, ErrQueueFull) {
			rejected++
		} else {
			accepted++
		}
	}
	if running.Load() != 5 || accepted != 55 || rejected != 5 {
		t.Fatalf("running=%d accepted=%d rejected=%d, want 5/55/5", running.Load(), accepted, rejected)
	}
	close(release)
	p.Shutdown(context.Background())
}

func TestPoolRetriesThenFails(t *testing.T) {
	p, f := newPool(1, 1, 2)
	var calls atomic.Int32
	_ = p.Submit(Task{ID: "t1", Run: func(context.Context, int) error {
		calls.Add(1)
		return errors.New("boom")
	}})
	eventually(t, func() bool { _, ok := f.get("t1"); return ok })
	p.Shutdown(context.Background())
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
	if _, ok := f.get("t1"); !ok {
		t.Fatal("task must be reported failed")
	}
}

func TestPanicDoesNotKillWorker(t *testing.T) {
	p, f := newPool(1, 5, 0)
	var after atomic.Bool
	_ = p.Submit(Task{ID: "bad", Run: func(context.Context, int) error { panic("nil map") }})
	_ = p.Submit(Task{ID: "good", Run: func(context.Context, int) error { after.Store(true); return nil }})
	eventually(t, after.Load)
	p.Shutdown(context.Background())
	if err, ok := f.get("bad"); !ok || err.Error() != "panic: nil map" {
		t.Fatalf("panic must become a failure, got %v", err)
	}
	if !after.Load() {
		t.Fatal("the same worker must keep processing after a panic")
	}
}

func TestShutdownFinishesRunningAndFailsQueued(t *testing.T) {
	p, f := newPool(1, 10, 0)
	started := make(chan struct{})
	var finished atomic.Bool
	_ = p.Submit(Task{ID: "running", Run: func(context.Context, int) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		finished.Store(true)
		return nil
	}})
	<-started
	for _, id := range []string{"q1", "q2"} {
		_ = p.Submit(Task{ID: id, Run: func(context.Context, int) error { return nil }})
	}
	p.Shutdown(context.Background())

	if !finished.Load() {
		t.Fatal("running task must finish before shutdown returns")
	}
	for _, id := range []string{"q1", "q2"} {
		if _, ok := f.get(id); !ok {
			t.Fatalf("queued task %s must be recorded as failed", id)
		}
	}
	if err := p.Submit(Task{ID: "late"}); !errors.Is(err, ErrStopped) {
		t.Fatalf("submit after shutdown = %v", err)
	}
}

func TestShutdownTimeoutCancelsRunningTask(t *testing.T) {
	p, f := newPool(1, 1, 2)
	started := make(chan struct{})
	_ = p.Submit(Task{ID: "long", Run: func(ctx context.Context, _ int) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	p.Shutdown(ctx)
	if _, ok := f.get("long"); !ok {
		t.Fatal("cancelled task must be recorded as failed (and not retried)")
	}
}

func TestLockOnlyOneInstanceWins(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	a, b := NewLocker(rdb), NewLocker(rdb)
	ctx := context.Background()

	okA, _ := a.TryLock(ctx, "report", 9*time.Second)
	okB, _ := b.TryLock(ctx, "report", 9*time.Second)
	if !okA || okB {
		t.Fatalf("A=%v B=%v, want exactly one winner", okA, okB)
	}
	mr.FastForward(10 * time.Second)
	if okB, _ = b.TryLock(ctx, "report", 9*time.Second); !okB {
		t.Fatal("after the TTL the lock is free again")
	}
}

func TestLockTTLFollowsTheSchedule(t *testing.T) {
	now := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	for spec, want := range map[string]time.Duration{
		"@every 10s": 9 * time.Second,
		"@every 5m":  270 * time.Second,
		"0 7 * * *":  24 * time.Hour * 9 / 10,
	} {
		s, err := cron.ParseStandard(spec)
		if err != nil {
			t.Fatal(err)
		}
		if got := lockTTL(s, now); got != want {
			t.Fatalf("%s: ttl %v, want %v", spec, got, want)
		}
	}
}
