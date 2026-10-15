package baqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SQLDriver implements durable SQLite and PostgreSQL storage. The baqueue_go_*
// tables are intentionally NOT schema-compatible with the Python drivers.
// Payloads use the shared JSON format; indexed columns are updated in the same
// transaction as the complete payload, including its attempt history.
type SQLDriver struct {
	AutoCleanupOnDiskFull bool
	ReconcileOnConnect    bool
	// Set before Connect. Positive values override PostgreSQL's 16/4 defaults;
	// SQLite always uses one open and one idle connection.
	MaxOpenConns int
	MaxIdleConns int

	driverName string
	dsn        string
	postgres   bool
	mu         sync.RWMutex
	db         *sql.DB
	cleaning   atomic.Bool
}

var _ Driver = (*SQLDriver)(nil)

const sqlSchema = `
CREATE TABLE IF NOT EXISTS baqueue_go_jobs (
 id TEXT PRIMARY KEY, queue TEXT NOT NULL, status TEXT NOT NULL,
 created_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL,
 delay_until DOUBLE PRECISION, started_at DOUBLE PRECISION, batch_id TEXT,
 payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS baqueue_go_jobs_queue_status_created ON baqueue_go_jobs(queue,status,created_at,id);
CREATE INDEX IF NOT EXISTS baqueue_go_jobs_status_created ON baqueue_go_jobs(status,created_at,id);
CREATE INDEX IF NOT EXISTS baqueue_go_jobs_created ON baqueue_go_jobs(created_at,id);
CREATE INDEX IF NOT EXISTS baqueue_go_jobs_delay ON baqueue_go_jobs(status,delay_until);
CREATE INDEX IF NOT EXISTS baqueue_go_jobs_batch ON baqueue_go_jobs(batch_id);
CREATE INDEX IF NOT EXISTS baqueue_go_jobs_updated ON baqueue_go_jobs(status,updated_at);
CREATE TABLE IF NOT EXISTS baqueue_go_batches (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS baqueue_go_metrics (
 id TEXT PRIMARY KEY, queue TEXT NOT NULL, metric TEXT NOT NULL,
 value DOUBLE PRECISION NOT NULL, recorded_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS baqueue_go_metrics_time ON baqueue_go_metrics(recorded_at);
CREATE INDEX IF NOT EXISTS baqueue_go_metrics_queue_time ON baqueue_go_metrics(queue,recorded_at,metric);
CREATE TABLE IF NOT EXISTS baqueue_go_supervisors (
 name TEXT PRIMARY KEY, data TEXT NOT NULL, heartbeat_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS baqueue_go_supervisors_heartbeat ON baqueue_go_supervisors(heartbeat_at);
CREATE TABLE IF NOT EXISTS baqueue_go_dynamic_queues (
 name TEXT PRIMARY KEY, idle_timeout_seconds DOUBLE PRECISION NOT NULL,
 created_at DOUBLE PRECISION NOT NULL, idle_since DOUBLE PRECISION, active BOOLEAN NOT NULL
);
`

func (d *SQLDriver) Connect(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db != nil {
		return d.db.PingContext(ctx)
	}
	db, err := sql.Open(d.driverName, d.dsn)
	if err != nil {
		return err
	}
	if d.postgres {
		maxOpen, maxIdle := 16, 4
		if d.MaxOpenConns > 0 {
			maxOpen = d.MaxOpenConns
		}
		if d.MaxIdleConns > 0 {
			maxIdle = d.MaxIdleConns
		}
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
	} else {
		// One connection also keeps :memory: databases alive and avoids local
		// writer contention. BEGIN IMMEDIATE fences writers in other processes.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	err = d.sqlRetry(ctx, func() error {
		return sqlTransaction(ctx, db, func(tx *sql.Tx) error {
			if d.postgres {
				// Serialize concurrent first-time schema creation across processes.
				if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(628171902341)"); err != nil {
					return err
				}
			}
			for _, statement := range strings.Split(sqlSchema, ";") {
				if strings.TrimSpace(statement) != "" {
					if _, err := tx.ExecContext(ctx, statement); err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
	if err != nil {
		_ = db.Close()
		return err
	}
	d.db = db
	// Reconciliation is intentionally a no-op: SQL indexes and payloads are
	// maintained transactionally, with no separate runnable-job index to heal.
	return nil
}

func (d *SQLDriver) Disconnect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

func (d *SQLDriver) sqlDB() (*sql.DB, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil {
		return nil, errors.New("baqueue: SQL driver is not connected")
	}
	return d.db, nil
}

func sqlTransaction(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func sqlErrorCode(err error) int {
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		return coded.Code()
	}
	return 0
}

func sqlState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return ""
}

func sqlStorageFull(err error) bool {
	return sqlErrorCode(err)&255 == 13 || sqlState(err) == "53100"
}

func (d *SQLDriver) sqlRetry(ctx context.Context, fn func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		code := sqlErrorCode(err) & 255
		if d.postgres || (code != 5 && code != 6) || attempt >= 4 {
			return err
		}
		timer := time.NewTimer(time.Duration(10<<attempt) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (d *SQLDriver) sqlRecover(ctx context.Context, db *sql.DB, fn func() error) error {
	err := d.sqlRetry(ctx, fn)
	if err == nil || !d.AutoCleanupOnDiskFull || !sqlStorageFull(err) || !d.cleaning.CompareAndSwap(false, true) {
		return err
	}
	defer d.cleaning.Store(false)
	// Never recurse into public write methods. Cleanup is best-effort; the
	// original operation gets exactly one further attempt, even if cleanup fails.
	for _, statement := range []string{
		"DELETE FROM baqueue_go_jobs WHERE status IN ('completed','failed','cancelled')",
		"DELETE FROM baqueue_go_metrics",
	} {
		_ = d.sqlRetry(ctx, func() error {
			_, cleanupErr := db.ExecContext(ctx, statement)
			return cleanupErr
		})
	}
	return d.sqlRetry(ctx, fn)
}

func (d *SQLDriver) sqlWrite(ctx context.Context, fn func(*sql.Tx) error) error {
	db, err := d.sqlDB()
	if err != nil {
		return err
	}
	return d.sqlRecover(ctx, db, func() error {
		return sqlTransaction(ctx, db, func(tx *sql.Tx) error {
			if d.postgres {
				// Queue-lock waiters must observe work committed by the lock holder.
				if _, err := tx.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
					return err
				}
			}
			return fn(tx)
		})
	})
}

func (d *SQLDriver) sqlLock(skip bool) string {
	if !d.postgres {
		return ""
	}
	if skip {
		return " FOR UPDATE SKIP LOCKED"
	}
	return " FOR UPDATE"
}

func sqlScanJob(row interface{ Scan(...any) error }) (*JobPayload, error) {
	var raw string
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var payload JobPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("baqueue: decode stored job: %w", err)
	}
	return &payload, nil
}

func sqlReadJobs(rows *sql.Rows) ([]*JobPayload, error) {
	defer rows.Close()
	out := make([]*JobPayload, 0)
	for rows.Next() {
		payload, err := sqlScanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, payload)
	}
	return out, rows.Err()
}

func sqlJobArgs(p *JobPayload) ([]any, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return []any{p.ID, p.Queue, p.Status, p.CreatedAt, p.UpdatedAt, p.DelayUntil, p.StartedAt, p.BatchID, string(raw)}, nil
}

func sqlSaveJob(ctx context.Context, tx *sql.Tx, p *JobPayload) error {
	args, err := sqlJobArgs(p)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE baqueue_go_jobs SET queue=$2,status=$3,
 created_at=$4,updated_at=$5,delay_until=$6,started_at=$7,batch_id=$8,payload=$9 WHERE id=$1`, args...)
	return err
}

func (d *SQLDriver) Push(ctx context.Context, payload *JobPayload) (string, error) {
	ids, err := d.PushMany(ctx, []*JobPayload{payload})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func (d *SQLDriver) PushMany(ctx context.Context, payloads []*JobPayload) ([]string, error) {
	return d.sqlPushMany(ctx, payloads, nil, 0)
}

func (d *SQLDriver) sqlPushMany(ctx context.Context, payloads []*JobPayload, dynamicName *string, timeout float64) ([]string, error) {
	ids := make([]string, len(payloads))
	arguments := make([][]any, len(payloads))
	names := make([]string, 0)
	seen := make(map[string]bool)
	for i, p := range payloads {
		if err := p.Validate(); err != nil {
			return nil, err
		}
		args, err := sqlJobArgs(p)
		if err != nil {
			return nil, err
		}
		ids[i], arguments[i] = p.ID, args
		if !seen[p.Queue] {
			seen[p.Queue] = true
			names = append(names, p.Queue)
		}
	}
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		lockNames := names
		if dynamicName != nil && len(names) == 0 {
			lockNames = []string{*dynamicName}
		}
		if err := d.sqlLockDynamicQueues(ctx, tx, lockNames); err != nil {
			return err
		}
		if dynamicName != nil {
			if _, err := d.sqlRegisterDynamicQueue(ctx, tx, *dynamicName, timeout); err != nil {
				return err
			}
		}
		for _, name := range names {
			if err := sqlTouchDynamicQueue(ctx, tx, name); err != nil {
				return err
			}
		}
		for _, args := range arguments {
			_, err := tx.ExecContext(ctx, `INSERT INTO baqueue_go_jobs
 (id,queue,status,created_at,updated_at,delay_until,started_at,batch_id,payload)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, args...)
			if sqlState(err) == "23505" || sqlErrorCode(err) == 1555 || sqlErrorCode(err) == 2067 {
				return fmt.Errorf("%w: %s", ErrDuplicate, args[0])
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (d *SQLDriver) Pop(ctx context.Context, queue string) (*JobPayload, error) {
	var claimed *JobPayload
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		claimed = nil
		now := Now()
		p, err := sqlScanJob(tx.QueryRowContext(ctx, `SELECT payload FROM baqueue_go_jobs
 WHERE queue=$1 AND status='pending' AND (delay_until IS NULL OR delay_until <= $2)
 ORDER BY created_at,id LIMIT 1`+d.sqlLock(true), queue, now))
		if err != nil || p == nil {
			return err
		}
		p.Status, p.StartedAt, p.UpdatedAt, p.DelayUntil = Processing, &now, now, nil
		p.Attempts++
		if err = sqlSaveJob(ctx, tx, p); err == nil {
			claimed = p
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func (d *SQLDriver) PopDelayed(ctx context.Context) ([]*JobPayload, error) {
	var promoted []*JobPayload
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		now := Now()
		where, args, err := d.sqlLockMatchingQueues(ctx, tx,
			"status='pending' AND delay_until IS NOT NULL AND delay_until <= $1", []any{now})
		if err != nil {
			return err
		}
		now = Now()
		rows, err := tx.QueryContext(ctx, "SELECT payload FROM baqueue_go_jobs WHERE "+where+
			" ORDER BY created_at,id"+d.sqlLock(true), args...)
		if err != nil {
			return err
		}
		promoted, err = sqlReadJobs(rows)
		if err != nil {
			return err
		}
		for _, p := range promoted {
			if err = sqlTouchDynamicQueue(ctx, tx, p.Queue); err != nil {
				return err
			}
			p.DelayUntil, p.UpdatedAt = nil, now
			if err = sqlSaveJob(ctx, tx, p); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return promoted, nil
}

func (d *SQLDriver) sqlTransition(ctx context.Context, incoming *JobPayload, processingOnly bool, update func(*JobPayload)) error {
	if incoming == nil {
		return ErrNotFound
	}
	var result *JobPayload
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		p, err := d.sqlReadLockedJob(ctx, tx, incoming.ID)
		if err != nil {
			return err
		}
		var history []Attempt
		if p != nil {
			history = append([]Attempt{}, p.History...)
		}
		if err = MergeAttempt(p, incoming); err != nil {
			return err
		}
		if processingOnly && p.Status != Processing {
			return ErrConflict
		}
		if p.Status == Processing {
			// An attempt owns its new history entry, not previous attempts.
			p.History = history
			for _, attempt := range incoming.History {
				if attempt.Attempt != p.Attempts || attempt.StartedAt != *p.StartedAt {
					continue
				}
				present := false
				for _, saved := range p.History {
					if saved.Attempt == attempt.Attempt && saved.StartedAt == attempt.StartedAt {
						present = true
						break
					}
				}
				if !present {
					p.History = append(p.History, attempt)
				}
			}
		}
		update(p)
		if p.Status == Pending {
			if err = sqlTouchDynamicQueue(ctx, tx, p.Queue); err != nil {
				return err
			}
		}
		if err = sqlSaveJob(ctx, tx, p); err != nil {
			return err
		}
		if p.BatchID != nil && (p.Status == Completed || p.Status == Failed) {
			data, err := sqlScanMap(tx.QueryRowContext(ctx, "SELECT data FROM baqueue_go_batches WHERE id=$1"+d.sqlLock(false), *p.BatchID))
			if err != nil {
				return err
			}
			if data != nil && ApplyBatchOutcome(data, p) {
				raw, err := json.Marshal(data)
				if err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, "UPDATE baqueue_go_batches SET data=$2 WHERE id=$1", *p.BatchID, string(raw)); err != nil {
					return err
				}
			}
		}
		result = p
		return nil
	})
	if err == nil {
		*incoming = *result
	}
	return err
}

func (d *SQLDriver) Complete(ctx context.Context, p *JobPayload) error {
	return d.sqlTransition(ctx, p, true, func(stored *JobPayload) {
		now := Now()
		stored.Status, stored.CompletedAt, stored.UpdatedAt = Completed, &now, now
		stored.DelayUntil, stored.FailedAt, stored.Error = nil, nil, nil
	})
}

func (d *SQLDriver) Fail(ctx context.Context, p *JobPayload, message string) error {
	return d.sqlTransition(ctx, p, true, func(stored *JobPayload) {
		now := Now()
		stored.Status, stored.FailedAt, stored.UpdatedAt, stored.Error = Failed, &now, now, &message
		stored.DelayUntil, stored.CompletedAt = nil, nil
	})
}

func (d *SQLDriver) Release(ctx context.Context, p *JobPayload, delay float64) error {
	if math.IsNaN(delay) || math.IsInf(delay, 0) || delay < 0 {
		return errors.New("baqueue: delay must be finite and nonnegative")
	}
	return d.sqlTransition(ctx, p, false, func(stored *JobPayload) {
		now := Now()
		stored.Status, stored.UpdatedAt = Pending, now
		stored.StartedAt, stored.CompletedAt, stored.FailedAt, stored.DelayUntil, stored.Error = nil, nil, nil, nil, nil
		if delay > 0 {
			until := now + delay
			stored.DelayUntil = &until
		}
	})
}

func (d *SQLDriver) RequeueStuckJobs(ctx context.Context, seconds float64, queue string) (int, error) {
	count := 0
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		now := Now()
		where := "status='processing' AND COALESCE(started_at,updated_at) <= $1"
		args := []any{now - seconds}
		if queue != "" {
			where += " AND queue=$2"
			args = append(args, queue)
		}
		where, args, err := d.sqlLockMatchingQueues(ctx, tx, where, args)
		if err != nil {
			return err
		}
		now = Now()
		rows, err := tx.QueryContext(ctx, "SELECT payload FROM baqueue_go_jobs WHERE "+where+" ORDER BY id"+d.sqlLock(true), args...)
		if err != nil {
			return err
		}
		jobs, err := sqlReadJobs(rows)
		if err != nil {
			return err
		}
		for _, p := range jobs {
			if err = sqlTouchDynamicQueue(ctx, tx, p.Queue); err != nil {
				return err
			}
			p.Status, p.StartedAt, p.DelayUntil, p.UpdatedAt = Pending, nil, nil, now
			if err = sqlSaveJob(ctx, tx, p); err != nil {
				return err
			}
		}
		count = len(jobs)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *SQLDriver) Delete(ctx context.Context, id string) error {
	_, err := d.BulkDeleteJobs(ctx, []string{id}, 0)
	return err
}

func (d *SQLDriver) Promote(ctx context.Context, id string) (bool, error) {
	promoted := false
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		promoted = false
		p, err := d.sqlReadLockedJob(ctx, tx, id)
		if err != nil || p == nil || p.Status != Pending {
			return err
		}
		if err = sqlTouchDynamicQueue(ctx, tx, p.Queue); err != nil {
			return err
		}
		p.DelayUntil, p.UpdatedAt = nil, Now()
		if err = sqlSaveJob(ctx, tx, p); err == nil {
			promoted = true
		}
		return err
	})
	return promoted && err == nil, err
}

func (d *SQLDriver) GetJob(ctx context.Context, id string) (*JobPayload, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	return sqlScanJob(db.QueryRowContext(ctx, "SELECT payload FROM baqueue_go_jobs WHERE id=$1", id))
}

func (d *SQLDriver) sqlWhere(filter JobFilter) (string, []any) {
	conditions := []string{"1=1"}
	args := make([]any, 0)
	add := func(column string, value any) {
		args = append(args, value)
		conditions = append(conditions, column+"$"+strconv.Itoa(len(args)))
	}
	if filter.Queue != "" {
		add("queue=", filter.Queue)
	}
	if filter.Status != "" {
		add("status=", filter.Status)
	}
	if filter.BatchID != "" {
		add("batch_id=", filter.BatchID)
	}
	if filter.CreatedFrom != nil {
		add("created_at>=", *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		add("created_at<=", *filter.CreatedTo)
	}
	if filter.Tag != "" {
		args = append(args, filter.Tag)
		param := "$" + strconv.Itoa(len(args))
		if d.postgres {
			conditions = append(conditions, "COALESCE(payload::jsonb->'tags','[]'::jsonb) ? "+param)
		} else {
			conditions = append(conditions, "EXISTS (SELECT 1 FROM json_each(baqueue_go_jobs.payload,'$.tags') WHERE value="+param+")")
		}
	}
	return strings.Join(conditions, " AND "), args
}

func (d *SQLDriver) GetJobs(ctx context.Context, filter JobFilter) ([]*JobPayload, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	where, args := d.sqlWhere(filter)
	limit := filter.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 0 {
		limit = math.MaxInt64
	}
	offset := max(filter.Offset, 0)
	query := "SELECT payload FROM baqueue_go_jobs WHERE " + where + " ORDER BY created_at DESC,id DESC LIMIT $" + strconv.Itoa(len(args)+1) + " OFFSET $" + strconv.Itoa(len(args)+2)
	args = append(args, limit, offset)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return sqlReadJobs(rows)
}

func (d *SQLDriver) CountJobs(ctx context.Context, filter JobFilter) (int, error) {
	db, err := d.sqlDB()
	if err != nil {
		return 0, err
	}
	where, args := d.sqlWhere(filter)
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM baqueue_go_jobs WHERE "+where, args...).Scan(&count)
	return count, err
}

func (d *SQLDriver) Size(ctx context.Context, queue string) (int, error) {
	return d.CountJobs(ctx, JobFilter{Queue: queue, Status: Pending})
}

func (d *SQLDriver) Queues(ctx context.Context) ([]string, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT queue FROM baqueue_go_jobs j WHERE NOT EXISTS
 (SELECT 1 FROM baqueue_go_dynamic_queues d WHERE d.name=j.queue AND NOT d.active)
 UNION SELECT name AS queue FROM baqueue_go_dynamic_queues WHERE active ORDER BY queue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var queue string
		if err = rows.Scan(&queue); err != nil {
			return nil, err
		}
		out = append(out, queue)
	}
	return out, rows.Err()
}

func (d *SQLDriver) RecordMetric(ctx context.Context, queue, metric string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return errors.New("baqueue: metric value must be finite")
	}
	id, now := NewID(), Now()
	return d.sqlWrite(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO baqueue_go_metrics(id,queue,metric,value,recorded_at) VALUES($1,$2,$3,$4,$5)", id, queue, metric, value, now)
		return err
	})
}

func (d *SQLDriver) GetMetrics(ctx context.Context, queue string) (map[string]any, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	query := "SELECT queue,status,COUNT(*) FROM baqueue_go_jobs"
	args := []any{}
	if queue != "" {
		query += " WHERE queue=$1"
		args = append(args, queue)
	}
	rows, err := db.QueryContext(ctx, query+" GROUP BY queue,status", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]any{}
	empty := func() map[string]int { return map[string]int{Pending: 0, Processing: 0, Completed: 0, Failed: 0} }
	if queue != "" {
		out[queue] = empty()
	}
	for rows.Next() {
		var q, status string
		var count int
		if err = rows.Scan(&q, &status, &count); err != nil {
			return nil, err
		}
		if out[q] == nil {
			out[q] = empty()
		}
		counts := out[q].(map[string]int)
		if _, ok := counts[status]; ok {
			counts[status] = count
		}
	}
	return out, rows.Err()
}

func (d *SQLDriver) RecentThroughput(ctx context.Context, seconds int, queue string) (map[string]int, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	query := "SELECT metric,COUNT(*) FROM baqueue_go_metrics WHERE recorded_at > $1"
	args := []any{Now() - float64(seconds)}
	if queue != "" {
		query += " AND queue=$2"
		args = append(args, queue)
	}
	rows, err := db.QueryContext(ctx, query+" GROUP BY metric", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{Processing: 0, Completed: 0, Failed: 0}
	for rows.Next() {
		var metric string
		var count int
		if err = rows.Scan(&metric, &count); err != nil {
			return nil, err
		}
		if _, ok := out[metric]; ok {
			out[metric] = count
		}
	}
	return out, rows.Err()
}

func (d *SQLDriver) ReportSupervisor(ctx context.Context, stats map[string]any) error {
	name, _ := stats["name"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	raw, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return d.sqlWrite(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO baqueue_go_supervisors(name,data,heartbeat_at) VALUES($1,$2,$3)
 ON CONFLICT(name) DO UPDATE SET data=excluded.data,heartbeat_at=excluded.heartbeat_at`, name, string(raw), Now())
		return err
	})
}

func (d *SQLDriver) GetSupervisorStats(ctx context.Context, staleAfter float64) ([]map[string]any, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT data FROM baqueue_go_supervisors WHERE heartbeat_at >= $1 ORDER BY name", Now()-staleAfter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var stats map[string]any
		if json.Unmarshal([]byte(raw), &stats) != nil {
			continue
		}
		if running, _ := stats["running"].(bool); running {
			out = append(out, stats)
		}
	}
	return out, rows.Err()
}

func (d *SQLDriver) StoreBatch(ctx context.Context, id string, data map[string]any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return d.sqlWrite(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO baqueue_go_batches(id,data) VALUES($1,$2)
 ON CONFLICT(id) DO UPDATE SET data=excluded.data`, id, string(raw))
		return err
	})
}

func sqlScanMap(row interface{ Scan(...any) error }) (map[string]any, error) {
	var raw string
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil, err
	}
	if data == nil {
		data = map[string]any{}
	}
	return data, nil
}

func (d *SQLDriver) GetBatch(ctx context.Context, id string) (map[string]any, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	return sqlScanMap(db.QueryRowContext(ctx, "SELECT data FROM baqueue_go_batches WHERE id=$1", id))
}

func (d *SQLDriver) GetPendingBatches(ctx context.Context, limit int) ([]map[string]any, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.QueryContext(ctx, "SELECT data FROM baqueue_go_batches ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		data, err := sqlScanMap(rows)
		if err != nil {
			return nil, err
		}
		if BatchNeedsReconciliation(data) {
			out = append(out, data)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, rows.Err()
}

func (d *SQLDriver) UpdateBatch(ctx context.Context, id string, data map[string]any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	var patch map[string]any
	if err = json.Unmarshal(raw, &patch); err != nil {
		return err
	}
	return d.sqlWrite(ctx, func(tx *sql.Tx) error {
		stored, err := sqlScanMap(tx.QueryRowContext(ctx, "SELECT data FROM baqueue_go_batches WHERE id=$1"+d.sqlLock(false), id))
		if err != nil {
			return err
		}
		if stored == nil {
			return ErrNotFound
		}
		for key, value := range patch {
			stored[key] = value
		}
		raw, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE baqueue_go_batches SET data=$2 WHERE id=$1", id, string(raw))
		return err
	})
}

func (d *SQLDriver) IncrementBatchCounter(ctx context.Context, id, field string, delta int) (map[string]any, error) {
	var result map[string]any
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		result = nil
		// The row lock (or SQLite's immediate writer lock) covers the read,
		// increment and returned snapshot, preventing lost counter updates.
		data, err := sqlScanMap(tx.QueryRowContext(ctx, "SELECT data FROM baqueue_go_batches WHERE id=$1"+d.sqlLock(false), id))
		if err != nil || data == nil {
			return err
		}
		value := float64(0)
		if existing, ok := data[field]; ok && existing != nil {
			var numeric bool
			value, numeric = existing.(float64)
			if !numeric || math.Trunc(value) != value {
				return fmt.Errorf("baqueue: batch field %q is not an integer", field)
			}
		}
		data[field] = value + float64(delta)
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE baqueue_go_batches SET data=$2 WHERE id=$1", id, string(raw)); err != nil {
			return err
		}
		result = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *SQLDriver) Prune(ctx context.Context, options PruneOptions) (int, error) {
	return d.sqlPrune(ctx, options, false)
}

func (d *SQLDriver) PruneTerminalJobs(ctx context.Context, options PruneOptions) (int, error) {
	return d.sqlPrune(ctx, options, true)
}

func (d *SQLDriver) sqlPrune(ctx context.Context, options PruneOptions, terminal bool) (int, error) {
	if err := options.Validate(); err != nil {
		return 0, err
	}
	where, args := d.sqlWhere(JobFilter{Queue: options.Queue, Status: options.Status, Tag: options.Tag})
	if terminal {
		where += " AND status IN ('completed','failed','cancelled')"
	}
	if options.OlderThanSeconds != nil {
		args = append(args, Now()-*options.OlderThanSeconds)
		where += " AND updated_at < $" + strconv.Itoa(len(args))
	}
	if where == "1=1" {
		return 0, nil
	}
	query := "DELETE FROM baqueue_go_jobs WHERE id IN (SELECT id FROM baqueue_go_jobs WHERE " + where + " ORDER BY updated_at,id"
	if options.Limit > 0 {
		args = append(args, options.Limit)
		query += " LIMIT $" + strconv.Itoa(len(args))
	}
	query += d.sqlLock(true) + ")"
	count := 0
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		count = int(n)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *SQLDriver) BulkDeleteJobs(ctx context.Context, ids []string, limit int) (int, error) {
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	count := 0
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		count = 0
		// Chunk parameters below SQLite's and PostgreSQL's limits. The entire
		// operation remains one transaction, even for very large input lists.
		for start := 0; start < len(ids); start += 500 {
			batch := ids[start:min(start+500, len(ids))]
			args := make([]any, len(batch))
			params := make([]string, len(batch))
			for i, id := range batch {
				args[i], params[i] = id, "$"+strconv.Itoa(i+1)
			}
			result, err := tx.ExecContext(ctx, "DELETE FROM baqueue_go_jobs WHERE id IN ("+strings.Join(params, ",")+")", args...)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			count += int(n)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *SQLDriver) ReconcileIndexes(ctx context.Context, _ int) (int, error) {
	return 0, ctx.Err()
}

func (d *SQLDriver) ReconcilePendingJobs(ctx context.Context, _ int) (int, error) {
	return 0, ctx.Err()
}

func (d *SQLDriver) Flush(ctx context.Context, queue string) error {
	return d.sqlWrite(ctx, func(tx *sql.Tx) error {
		if queue != "" {
			if err := d.sqlLockDynamicQueues(ctx, tx, []string{queue}); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM baqueue_go_jobs WHERE queue=$1", queue); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, "DELETE FROM baqueue_go_dynamic_queues WHERE name=$1", queue)
			return err
		}
		if d.postgres {
			if _, err := tx.ExecContext(ctx, "LOCK TABLE baqueue_go_dynamic_queues IN SHARE ROW EXCLUSIVE MODE"); err != nil {
				return err
			}
		}
		for _, table := range []string{"baqueue_go_jobs", "baqueue_go_batches", "baqueue_go_metrics", "baqueue_go_supervisors", "baqueue_go_dynamic_queues"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *SQLDriver) PruneMetrics(ctx context.Context, seconds float64) (int, error) {
	count := 0
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM baqueue_go_metrics WHERE recorded_at < $1", Now()-seconds)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		count = int(n)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
