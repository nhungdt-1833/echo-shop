package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time { return f.t }

func newLimiters(t *testing.T) (*FixedWindow, *SlidingWindow, *fakeClock) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	clock := &fakeClock{t: time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)}
	return &FixedWindow{rdb: rdb, now: clock.now}, &SlidingWindow{rdb: rdb, now: clock.now}, clock
}

func burst(t *testing.T, l Limiter, n int) (allowed int, last Result) {
	t.Helper()
	for range n {
		res, err := l.Allow(context.Background(), "u:2", 60, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if res.Allowed {
			allowed++
		}
		last = res
	}
	return allowed, last
}

func TestBothAlgorithmsAllowExactlyTheLimit(t *testing.T) {
	fixed, sliding, _ := newLimiters(t)
	for name, l := range map[string]Limiter{"fixed": fixed, "sliding": sliding} {
		allowed, last := burst(t, l, 100)
		if allowed != 60 || last.Allowed || last.Remaining != 0 || last.Limit != 60 {
			t.Fatalf("%s: allowed=%d last=%+v, want 60 allowed then blocked", name, allowed, last)
		}
	}
}

func TestWindowBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pick      func(f *FixedWindow, s *SlidingWindow) Limiter
		wantTotal int
	}{
		{"fixed lets 2x through", func(f *FixedWindow, _ *SlidingWindow) Limiter { return f }, 120},
		{"sliding holds the line", func(_ *FixedWindow, s *SlidingWindow) Limiter { return s }, 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, s, clock := newLimiters(t)
			l := tc.pick(f, s)
			clock.t = clock.t.Add(59*time.Second + 500*time.Millisecond)
			a, _ := burst(t, l, 60)
			clock.t = clock.t.Add(time.Second)
			b, _ := burst(t, l, 60)
			if a+b != tc.wantTotal {
				t.Fatalf("allowed %d+%d=%d in one second, want %d", a, b, a+b, tc.wantTotal)
			}
		})
	}
}

func TestSlidingFreesSlotsAsTimePasses(t *testing.T) {
	_, s, clock := newLimiters(t)
	burst(t, s, 60)
	clock.t = clock.t.Add(30 * time.Second)
	if a, last := burst(t, s, 1); a != 0 || !last.ResetAt.Equal(time.Date(2026, 10, 29, 10, 1, 0, 0, time.UTC)) {
		t.Fatalf("still full after 30s, reset should be 10:01:00, got allowed=%d %+v", a, last)
	}
	clock.t = clock.t.Add(31 * time.Second)
	if a, _ := burst(t, s, 1); a != 1 {
		t.Fatal("a slot must be free after the window")
	}
}

func TestPeekDoesNotCount(t *testing.T) {
	fixed, sliding, _ := newLimiters(t)
	for name, l := range map[string]Limiter{"fixed": fixed, "sliding": sliding} {
		burst(t, l, 10)
		for range 5 {
			if res, err := l.Peek(context.Background(), "u:2", 60, time.Minute); err != nil || res.Remaining != 50 {
				t.Fatalf("%s: peek %+v %v, want remaining 50", name, res, err)
			}
		}
	}
}
