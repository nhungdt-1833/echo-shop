package job

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	ErrQueueFull = errors.New("queue full")
	ErrStopped   = errors.New("pool stopped")
)

type Task struct {
	ID  string
	Run func(ctx context.Context, attempt int) error
}

type Hooks struct {
	OnFail func(id string, err error)
}

type Pool struct {
	tasks    chan Task
	retries  int
	backoff  time.Duration
	hooks    Hooks
	stopping chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewPool(size, queue, retries int, backoff time.Duration, hooks Hooks) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		tasks:    make(chan Task, queue),
		retries:  retries,
		backoff:  backoff,
		hooks:    hooks,
		stopping: make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
	for i := 1; i <= size; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	return p
}

func (p *Pool) Submit(t Task) error {
	select {
	case <-p.stopping:
		return ErrStopped
	default:
	}
	select {
	case p.tasks <- t:
		return nil
	default:
		return ErrQueueFull
	}
}

func (p *Pool) worker(n int) {
	defer p.wg.Done()
	for {
		select {
		case <-p.stopping:
			return
		default:
		}
		select {
		case <-p.stopping:
			return
		case t := <-p.tasks:
			p.run(n, t)
		}
	}
}

func (p *Pool) run(worker int, t Task) {
	var err error
	for attempt := 1; attempt <= p.retries+1; attempt++ {
		if err = safeRun(p.ctx, t, attempt); err == nil {
			log.Printf("[pool] worker %d: task %s done (attempt %d)", worker, t.ID, attempt)
			return
		}
		log.Printf("[pool] worker %d: task %s attempt %d failed: %v", worker, t.ID, attempt, err)
		if p.ctx.Err() != nil {
			break
		}
		if attempt <= p.retries {
			time.Sleep(p.backoff * time.Duration(attempt))
		}
	}
	p.fail(t.ID, err)
}

func safeRun(ctx context.Context, t Task, attempt int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return t.Run(ctx, attempt)
}

func (p *Pool) fail(id string, err error) {
	if p.hooks.OnFail != nil {
		p.hooks.OnFail(id, err)
	}
}

func (p *Pool) Shutdown(ctx context.Context) {
	p.stopOnce.Do(func() { close(p.stopping) })
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		p.cancel()
		<-done
	}
	for {
		select {
		case t := <-p.tasks:
			p.fail(t.ID, errors.New("server shutting down before the task started"))
		default:
			p.cancel()
			return
		}
	}
}
