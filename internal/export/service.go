package export

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"echo-shop/internal/job"
	"echo-shop/internal/model"
	"echo-shop/internal/ws"
	"echo-shop/internal/wsbus"
)

const (
	StatusQueued  = "queued"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"

	Workers    = 5
	QueueSize  = 50
	MaxRetries = 2

	statusTTL = 24 * time.Hour
)

var ErrNotFound = errors.New("export not found")

type Status struct {
	ID       string `json:"job_id" redis:"-"`
	Status   string `json:"status" redis:"status"`
	Attempts int    `json:"attempts" redis:"attempts"`
	Rows     int    `json:"rows,omitempty" redis:"rows"`
	Error    string `json:"error,omitempty" redis:"error"`
	Download string `json:"download,omitempty" redis:"download"`
	AdminID  uint   `json:"-" redis:"admin_id"`
}

type Service struct {
	db       *gorm.DB
	rdb      *redis.Client
	pool     *job.Pool
	dir      string
	slow     time.Duration
	failRate float64
}

func NewService(db *gorm.DB, rdb *redis.Client, dir string, slow time.Duration, failRate float64) (*Service, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Service{db: db, rdb: rdb, dir: dir, slow: slow, failRate: failRate}
	s.pool = job.NewPool(Workers, QueueSize, MaxRetries, time.Second, job.Hooks{OnFail: s.markFailed})
	return s, nil
}

// Submit records the job as queued and hands it to the pool.
// Pool full → job.ErrQueueFull (the handler answers 503).
func (s *Service) Submit(ctx context.Context, adminID uint) (Status, error) {
	st := Status{ID: newID(), Status: StatusQueued, AdminID: adminID}
	if err := s.save(ctx, st.ID, map[string]any{"status": st.Status, "attempts": 0, "admin_id": adminID}); err != nil {
		return Status{}, err
	}
	err := s.pool.Submit(job.Task{ID: st.ID, Run: func(ctx context.Context, attempt int) error {
		return s.run(ctx, st.ID, adminID, attempt)
	}})
	if err != nil {
		s.rdb.Del(context.Background(), key(st.ID))
		return Status{}, err
	}
	return st, nil
}

func (s *Service) Get(ctx context.Context, id string) (Status, error) {
	var st Status
	res := s.rdb.HGetAll(ctx, key(id))
	if err := res.Err(); err != nil {
		return st, err
	}
	if len(res.Val()) == 0 {
		return st, ErrNotFound
	}
	if err := res.Scan(&st); err != nil {
		return st, err
	}
	st.ID = id
	return st, nil
}

// File returns the CSV path of a finished export.
func (s *Service) File(id string) string { return filepath.Join(s.dir, id+".csv") }

// Shutdown: running exports finish, queued ones are recorded as failed.
func (s *Service) Shutdown(ctx context.Context) { s.pool.Shutdown(ctx) }

// run is one attempt of an export (called by a pool worker).
func (s *Service) run(ctx context.Context, id string, adminID uint, attempt int) error {
	if err := s.save(ctx, id, map[string]any{"status": StatusRunning, "attempts": attempt}); err != nil {
		return err
	}
	var orders []model.Order
	if err := s.db.WithContext(ctx).Order("id").Find(&orders).Error; err != nil {
		return err
	}
	select { // a big export takes time; give up if the server is forced to stop
	case <-time.After(s.slow):
	case <-ctx.Done():
		return ctx.Err()
	}
	if mrand.Float64() < s.failRate {
		return errors.New("disk error (simulated)")
	}
	if err := s.writeCSV(id, orders); err != nil {
		return err
	}

	link := "/admin/exports/" + id + "/download"
	if err := s.save(ctx, id, map[string]any{"status": StatusDone, "rows": len(orders), "download": link, "error": ""}); err != nil {
		return err
	}
	s.notify(adminID, ws.Message{Type: ws.TypeExportDone, JobID: id, Status: StatusDone, Text: link})
	return nil
}

// writeCSV writes to a temp file then renames it: a half-written file is never
// visible under the final name.
func (s *Service) writeCSV(id string, orders []model.Order) error {
	tmp, err := os.CreateTemp(s.dir, id+"-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	w := csv.NewWriter(tmp)
	_ = w.Write([]string{"id", "user_id", "product_id", "quantity", "total", "status", "created_at"})
	for _, o := range orders {
		_ = w.Write([]string{
			strconv.FormatUint(uint64(o.ID), 10), strconv.FormatUint(uint64(o.UserID), 10),
			strconv.FormatUint(uint64(o.ProductID), 10), strconv.Itoa(o.Quantity),
			strconv.FormatInt(o.Total, 10), o.Status, o.CreatedAt.Format(time.RFC3339),
		})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.File(id))
}

// markFailed is the pool's OnFail hook: retries exhausted, panic, or shutdown.
func (s *Service) markFailed(id string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.save(ctx, id, map[string]any{"status": StatusFailed, "error": cause.Error()}); err != nil {
		log.Printf("[export %s] cannot record failure: %v", id, err)
	}
	if st, err := s.Get(ctx, id); err == nil {
		s.notify(st.AdminID, ws.Message{Type: ws.TypeExportFail, JobID: id, Status: StatusFailed, Error: cause.Error()})
	}
}

func (s *Service) notify(adminID uint, m ws.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := wsbus.Send(ctx, s.rdb, ws.UserEvent(adminID, m)); err != nil {
		log.Printf("[export %s] notify: %v", m.JobID, err)
	}
}

func (s *Service) save(ctx context.Context, id string, fields map[string]any) error {
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key(id), fields)
	pipe.Expire(ctx, key(id), statusTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func key(id string) string { return "export:" + id }

func newID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return fmt.Sprintf("exp-%s", hex.EncodeToString(b))
}
