package baqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type sqlTestFactory func(*testing.T) *SQLDriver

func sqlTestCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func sqlTestConnect(t *testing.T, d *SQLDriver) *SQLDriver {
	t.Helper()
	sqlTestCheck(t, d.Connect(t.Context()))
	t.Cleanup(func() { sqlTestCheck(t, d.Disconnect(context.Background())) })
	return d
}

func sqlTestPeer(t *testing.T, d *SQLDriver) *SQLDriver {
	t.Helper()
	return sqlTestConnect(t, &SQLDriver{driverName: d.driverName, dsn: d.dsn, postgres: d.postgres, AutoCleanupOnDiskFull: true})
}

func sqlTestSQLite(t *testing.T) *SQLDriver {
	t.Helper()
	return sqlTestConnect(t, NewSQLiteDriver(filepath.Join(t.TempDir(), "queue.db")))
}

func sqlTestPostgres(t *testing.T) *SQLDriver {
	t.Helper()
	dsn := os.Getenv("BAQUEUE_POSTGRES_URL")
	if dsn == "" {
		t.Skip("BAQUEUE_POSTGRES_URL is not configured")
	}
	admin, err := sql.Open("pgx", dsn)
	sqlTestCheck(t, err)
	schema := "baqueue_sql_test_" + NewID()
	if _, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		sqlTestCheck(t, err)
		sqlTestCheck(t, admin.Close())
	})
	u, err := url.Parse(dsn)
	sqlTestCheck(t, err)
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	return sqlTestConnect(t, NewPostgresDriver(u.String()))
}

func TestSQLiteContract(t *testing.T) {
	sqlTestContract(t, sqlTestSQLite)
}

// PostgreSQL tests create and remove only their own uniquely named schemas.
// The caller provides the database and owns its service lifecycle.
func TestPostgresContract(t *testing.T) {
	if os.Getenv("BAQUEUE_POSTGRES_URL") == "" {
		t.Skip("BAQUEUE_POSTGRES_URL is not configured")
	}
	sqlTestContract(t, sqlTestPostgres)
}

func sqlTestContract(t *testing.T, factory sqlTestFactory) {
	t.Run("PruneValidation", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		p := NewPayload("job", nil)
		p.Status = Completed
		_, err := d.Push(ctx, p)
		sqlTestCheck(t, err)
		invalid := []PruneOptions{{Status: Completed, Limit: -1}}
		for _, age := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			invalid = append(invalid, PruneOptions{Status: Completed, OlderThanSeconds: &age})
		}
		for _, prune := range []func(context.Context, PruneOptions) (int, error){d.Prune, d.PruneTerminalJobs} {
			for _, options := range invalid {
				if n, err := prune(ctx, options); err == nil || n != 0 {
					t.Fatalf("invalid prune options accepted: %+v, count=%d err=%v", options, n, err)
				}
			}
		}
		stored, err := d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if stored == nil {
			t.Fatal("invalid prune options deleted a stored job")
		}
	})

	t.Run("Indexes", func(t *testing.T) {
		d := factory(t)
		db, err := d.sqlDB()
		sqlTestCheck(t, err)
		query := "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='baqueue_go_jobs'"
		if d.postgres {
			query = "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND tablename='baqueue_go_jobs'"
		}
		rows, err := db.QueryContext(t.Context(), query)
		sqlTestCheck(t, err)
		defer rows.Close()
		indexes := map[string]bool{}
		for rows.Next() {
			var name string
			sqlTestCheck(t, rows.Scan(&name))
			indexes[name] = true
		}
		sqlTestCheck(t, rows.Err())
		for _, suffix := range []string{"queue_status_created", "status_created", "created", "delay", "batch", "updated"} {
			if !indexes["baqueue_go_jobs_"+suffix] {
				t.Errorf("missing job index: %s", suffix)
			}
		}
	})

	t.Run("DurableFullPayload", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		p := NewPayload("jobs.Send", map[string]any{"nested": map[string]any{"value": "persisted"}})
		batch, message, retry := "batch-a", "old failure", Now()+60
		p.Tags, p.BatchID, p.Backoff = []string{"a", `quote"%_`}, &batch, Backoff{Delays: []float64{1, 5, 30}}
		p.Attempts, p.MaxAttempts, p.Timeout, p.Error = 1, 7, 45, &message
		p.History = []Attempt{{Attempt: 1, StartedAt: Now() - 5, FinishedAt: Now() - 4, Duration: 1, Status: Failed, Error: &message, WillRetry: true, NextRetryAt: &retry}}
		_, err := d.Push(ctx, p)
		sqlTestCheck(t, err)
		want := p.Clone()
		p.Data["nested"] = "mutated"
		sqlTestCheck(t, d.StoreBatch(ctx, batch, map[string]any{"pending_jobs": 1, "name": "durable"}))
		sqlTestCheck(t, d.RecordMetric(ctx, p.Queue, Processing, 1))
		sqlTestCheck(t, d.ReportSupervisor(ctx, map[string]any{"name": "worker", "running": true, "workers": 2}))
		sqlTestCheck(t, d.Disconnect(ctx))
		d.ReconcileOnConnect = true
		sqlTestCheck(t, d.Connect(ctx))
		sqlTestCheck(t, d.Connect(ctx))
		got, err := d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("payload did not survive reconnect: got %#v, want %#v", got, want)
		}
		data, err := d.GetBatch(ctx, batch)
		sqlTestCheck(t, err)
		if data["name"] != "durable" {
			t.Fatalf("batch lost: %v", data)
		}
		stats, err := d.GetSupervisorStats(ctx, 60)
		sqlTestCheck(t, err)
		if len(stats) != 1 {
			t.Fatalf("heartbeat lost: %v", stats)
		}
		throughput, err := d.RecentThroughput(ctx, 60, p.Queue)
		sqlTestCheck(t, err)
		if throughput[Processing] != 1 {
			t.Fatalf("metric lost: %v", throughput)
		}
		for _, reconcile := range []func(context.Context, int) (int, error){d.ReconcileIndexes, d.ReconcilePendingJobs} {
			n, err := reconcile(ctx, 1)
			sqlTestCheck(t, err)
			if n != 0 {
				t.Fatalf("SQL reconciliation must be a no-op: %d", n)
			}
		}
	})

	t.Run("AtomicPushMany", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		existing := NewPayload("job", nil)
		_, err := d.Push(ctx, existing)
		sqlTestCheck(t, err)
		first := NewPayload("job", nil)
		ids, err := d.PushMany(ctx, []*JobPayload{first, existing})
		if !errors.Is(err, ErrDuplicate) || ids != nil {
			t.Fatalf("duplicate error: %v, ids=%v", err, ids)
		}
		got, err := d.GetJob(ctx, first.ID)
		sqlTestCheck(t, err)
		if got != nil {
			t.Fatal("partial batch survived rollback")
		}
		if _, err = d.PushMany(ctx, []*JobPayload{first, nil}); err == nil {
			t.Fatal("nil payload accepted")
		}
		invalid := NewPayload("job", map[string]any{"bad": make(chan int)})
		if _, err = d.PushMany(ctx, []*JobPayload{first, invalid}); err == nil {
			t.Fatal("non-JSON payload accepted")
		}
		n, err := d.CountJobs(ctx, JobFilter{})
		sqlTestCheck(t, err)
		if n != 1 {
			t.Fatalf("invalid batch wrote rows: %d", n)
		}
		ids, err = d.PushMany(ctx, []*JobPayload{first})
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(ids, []string{first.ID}) {
			t.Fatalf("incorrect IDs: %v", ids)
		}
	})

	t.Run("LifecycleFencing", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		p := NewPayload("job", nil)
		p.Backoff = Backoff{Delays: []float64{3, 7}}
		_, err := d.Push(ctx, p)
		sqlTestCheck(t, err)
		if err = d.Complete(ctx, p); !errors.Is(err, ErrConflict) {
			t.Fatalf("completed an unclaimed job: %v", err)
		}
		claimed, err := d.Pop(ctx, p.Queue)
		sqlTestCheck(t, err)
		if claimed == nil || claimed.Attempts != 1 || claimed.StartedAt == nil || claimed.Status != Processing {
			t.Fatalf("invalid claim: %#v", claimed)
		}
		stale := claimed.Clone()
		claimed.History = append(claimed.History, Attempt{Attempt: claimed.Attempts, StartedAt: *claimed.StartedAt, FinishedAt: Now(), Status: Failed, WillRetry: true})
		claimed.MaxAttempts, claimed.Backoff = 999, Backoff{Strategy: "fixed"}
		sqlTestCheck(t, d.Release(ctx, claimed, 60))
		stored, err := d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if stored.Status != Pending || stored.StartedAt != nil || stored.Attempts != 1 || stored.MaxAttempts != 3 || !reflect.DeepEqual(stored.Backoff, p.Backoff) || len(stored.History) != 1 {
			t.Fatalf("release lost persisted policy/history: %#v", stored)
		}
		for _, transition := range []func() error{
			func() error { return d.Complete(ctx, stale) },
			func() error { return d.Fail(ctx, stale, "stale") },
			func() error { return d.Release(ctx, stale, 0) },
		} {
			if err = transition(); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale attempt accepted: %v", err)
			}
		}
		if next, err := d.Pop(ctx, p.Queue); err != nil || next != nil {
			t.Fatalf("claimed delayed retry early: %#v, %v", next, err)
		}
		ok, err := d.Promote(ctx, p.ID)
		sqlTestCheck(t, err)
		if !ok {
			t.Fatal("retry was not promoted")
		}
		claimed, err = d.Pop(ctx, p.Queue)
		sqlTestCheck(t, err)
		if claimed.Attempts != 2 {
			t.Fatalf("attempt counter reset: %d", claimed.Attempts)
		}
		wrongOwner := claimed.Clone()
		wrongTime := *wrongOwner.StartedAt - 1
		wrongOwner.StartedAt = &wrongTime
		if err = d.Complete(ctx, wrongOwner); !errors.Is(err, ErrConflict) {
			t.Fatalf("wrong attempt timestamp accepted: %v", err)
		}
		claimed.History = append(claimed.History, Attempt{Attempt: 2, StartedAt: *claimed.StartedAt, FinishedAt: Now(), Status: Failed})
		sqlTestCheck(t, d.Fail(ctx, claimed, "permanent"))
		stored, err = d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if stored.Status != Failed || stored.FailedAt == nil || stored.Error == nil || *stored.Error != "permanent" || len(stored.History) != 2 {
			t.Fatalf("failure not persisted: %#v", stored)
		}
		sqlTestCheck(t, d.Release(ctx, stored, 0))
		claimed, err = d.Pop(ctx, p.Queue)
		sqlTestCheck(t, err)
		claimed.History = append(claimed.History, Attempt{Attempt: 3, StartedAt: *claimed.StartedAt, FinishedAt: Now(), Status: Completed})
		sqlTestCheck(t, d.Complete(ctx, claimed))
		stored, err = d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if stored.Status != Completed || stored.CompletedAt == nil || stored.Error != nil || len(stored.History) != 3 {
			t.Fatalf("completion not persisted: %#v", stored)
		}
		db, err := d.sqlDB()
		sqlTestCheck(t, err)
		var queue, status string
		var created, updated float64
		var delayed sql.NullFloat64
		sqlTestCheck(t, db.QueryRowContext(ctx, "SELECT queue,status,created_at,updated_at,delay_until FROM baqueue_go_jobs WHERE id=$1", p.ID).Scan(&queue, &status, &created, &updated, &delayed))
		if queue != stored.Queue || status != stored.Status || created != stored.CreatedAt || updated != stored.UpdatedAt || delayed.Valid {
			t.Fatal("indexed columns diverged from the JSON payload")
		}
		if err = d.Complete(ctx, stored); !errors.Is(err, ErrConflict) {
			t.Fatalf("completed a terminal job twice: %v", err)
		}
		if err = d.Release(ctx, NewPayload("missing", nil), 0); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing lifecycle result: %v", err)
		}
	})

	t.Run("HistoryOwnershipAndConcurrentTransitions", func(t *testing.T) {
		d := factory(t)
		peer := sqlTestPeer(t, d)
		ctx := t.Context()
		p := NewPayload("job", nil)
		p.Attempts = 1
		prior := Attempt{Attempt: 1, StartedAt: Now() - 5, FinishedAt: Now() - 4, Status: Failed, WillRetry: true}
		p.History = []Attempt{prior}
		_, err := d.Push(ctx, p)
		sqlTestCheck(t, err)
		claimed, err := d.Pop(ctx, p.Queue)
		sqlTestCheck(t, err)
		current := Attempt{Attempt: 2, StartedAt: *claimed.StartedAt, FinishedAt: Now(), Status: Completed}
		// Omission or forgery of a previous attempt must not rewrite stored history.
		claimed.History = []Attempt{{Attempt: 1, StartedAt: prior.StartedAt, Status: Completed}, current, current}
		other := claimed.Clone()
		results := make(chan error, 2)
		go func() { results <- d.Complete(ctx, claimed) }()
		go func() { results <- peer.Fail(ctx, other, "race") }()
		successes, conflicts := 0, 0
		for range 2 {
			err := <-results
			if err == nil {
				successes++
			} else if errors.Is(err, ErrConflict) {
				conflicts++
			} else {
				t.Error(err)
			}
		}
		if successes != 1 || conflicts != 1 {
			t.Fatalf("concurrent lifecycle transitions: successes=%d conflicts=%d", successes, conflicts)
		}
		stored, err := d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if len(stored.History) != 2 || !reflect.DeepEqual(stored.History[0], prior) || !reflect.DeepEqual(stored.History[1], current) {
			t.Fatalf("attempt ownership/deduplication failed: %#v", stored.History)
		}
	})

	t.Run("DelaysAndRecovery", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		future, past := Now()+3600, Now()-10
		later, due := NewPayload("future", nil), NewPayload("due", nil)
		later.DelayUntil, due.DelayUntil = &future, &past
		_, err := d.PushMany(ctx, []*JobPayload{later, due})
		sqlTestCheck(t, err)
		promoted, err := d.PopDelayed(ctx)
		sqlTestCheck(t, err)
		if len(promoted) != 1 || promoted[0].ID != due.ID || promoted[0].DelayUntil != nil || promoted[0].Status != Pending {
			t.Fatalf("incorrect delayed promotion: %#v", promoted)
		}
		claimed, err := d.Pop(ctx, due.Queue)
		sqlTestCheck(t, err)
		if claimed == nil || claimed.ID != due.ID {
			t.Fatalf("did not claim due job: %#v", claimed)
		}
		if got, err := d.Pop(ctx, later.Queue); err != nil || got != nil {
			t.Fatalf("early future claim: %#v, %v", got, err)
		}
		n, err := d.RequeueStuckJobs(ctx, 0, "other")
		sqlTestCheck(t, err)
		if n != 0 {
			t.Fatal("recovered another queue")
		}
		n, err = d.RequeueStuckJobs(ctx, 0, due.Queue)
		sqlTestCheck(t, err)
		if n != 1 {
			t.Fatalf("recovery count = %d", n)
		}
		reclaimed, err := d.Pop(ctx, due.Queue)
		sqlTestCheck(t, err)
		if reclaimed.Attempts != 2 {
			t.Fatalf("recovery reset attempts: %d", reclaimed.Attempts)
		}
		if err = d.Complete(ctx, claimed); !errors.Is(err, ErrConflict) {
			t.Fatalf("pre-recovery claim accepted: %v", err)
		}
		sqlTestCheck(t, d.Complete(ctx, reclaimed))
		ok, err := d.Promote(ctx, due.ID)
		sqlTestCheck(t, err)
		if ok {
			t.Fatal("promoted a terminal job")
		}
		fallback := NewPayload("fallback", nil)
		fallback.Status, fallback.UpdatedAt = Processing, Now()-100
		_, err = d.Push(ctx, fallback)
		sqlTestCheck(t, err)
		n, err = d.RequeueStuckJobs(ctx, 50, "")
		sqlTestCheck(t, err)
		if n != 1 {
			t.Fatalf("updated_at fallback failed: %d", n)
		}
	})

	t.Run("FilteringAndPruning", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		batch, tag, old := "group", `exact"%_\tag`, Now()-3600
		jobs := make([]*JobPayload, 55)
		for i := range jobs {
			p := NewPayload("job", nil)
			p.CreatedAt, p.UpdatedAt = old+float64(i), old+float64(i)
			p.Queue = "a"
			if i < 5 {
				p.Tags, p.BatchID, p.Status = []string{tag}, &batch, Completed
			}
			jobs[i] = p
		}
		jobs[5].Tags, jobs[5].Queue = []string{"exactXanything"}, "b"
		_, err := d.PushMany(ctx, jobs)
		sqlTestCheck(t, err)
		got, err := d.GetJobs(ctx, JobFilter{})
		sqlTestCheck(t, err)
		if len(got) != 50 || got[0].ID != jobs[54].ID {
			t.Fatalf("default pagination/order: %d", len(got))
		}
		from, to := jobs[1].CreatedAt, jobs[3].CreatedAt
		filter := JobFilter{Queue: "a", Status: Completed, Tag: tag, BatchID: batch, CreatedFrom: &from, CreatedTo: &to, Offset: 1, Limit: 1}
		got, err = d.GetJobs(ctx, filter)
		sqlTestCheck(t, err)
		if len(got) != 1 || got[0].ID != jobs[2].ID {
			t.Fatalf("combined filter/pagination mismatch: %#v", got)
		}
		n, err := d.CountJobs(ctx, filter)
		sqlTestCheck(t, err)
		if n != 3 {
			t.Fatalf("count applied pagination or dropped filter: %d", n)
		}
		got, err = d.GetJobs(ctx, JobFilter{Limit: -1, Offset: -1})
		sqlTestCheck(t, err)
		if len(got) != 55 {
			t.Fatalf("unbounded query = %d", len(got))
		}
		queues, err := d.Queues(ctx)
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(queues, []string{"a", "b"}) {
			t.Fatalf("queues: %v", queues)
		}
		n, err = d.Size(ctx, "a")
		sqlTestCheck(t, err)
		if n != 49 {
			t.Fatalf("size = %d", n)
		}
		age := float64(3000)
		n, err = d.Prune(ctx, PruneOptions{Queue: "a", Status: Completed, Tag: tag, OlderThanSeconds: &age, Limit: 2})
		sqlTestCheck(t, err)
		if n != 2 {
			t.Fatalf("capped prune = %d", n)
		}
		n, err = d.PruneTerminalJobs(ctx, PruneOptions{Status: Pending})
		sqlTestCheck(t, err)
		if n != 0 {
			t.Fatal("terminal prune deleted pending work")
		}
		n, err = d.PruneTerminalJobs(ctx, PruneOptions{})
		sqlTestCheck(t, err)
		if n != 3 {
			t.Fatalf("terminal prune = %d", n)
		}
		n, err = d.BulkDeleteJobs(ctx, []string{jobs[6].ID, jobs[6].ID, "missing", jobs[7].ID}, 3)
		sqlTestCheck(t, err)
		if n != 1 {
			t.Fatalf("bulk delete must count actual removed rows: %d", n)
		}
		sqlTestCheck(t, d.Delete(ctx, jobs[7].ID))
		sqlTestCheck(t, d.Delete(ctx, "missing"))
		if got, err := d.GetJob(ctx, jobs[7].ID); err != nil || got != nil {
			t.Fatalf("deleted job still exists: %#v, %v", got, err)
		}
	})

	t.Run("ConcurrentClaimsAndCounters", func(t *testing.T) {
		d := factory(t)
		peer := sqlTestPeer(t, d)
		ctx := t.Context()
		const count = 96
		jobs := make([]*JobPayload, count)
		for i := range jobs {
			jobs[i] = NewPayload("job", nil)
		}
		_, err := d.PushMany(ctx, jobs)
		sqlTestCheck(t, err)
		sqlTestCheck(t, d.StoreBatch(ctx, "batch", map[string]any{"name": "unchanged", "count": 0}))
		var wg sync.WaitGroup
		claims := make(chan string, count)
		counters := make(chan float64, count)
		errorsCh := make(chan error, 8)
		for worker := 0; worker < 8; worker++ {
			current := []*SQLDriver{d, peer}[worker%2]
			wg.Go(func() {
				for {
					p, err := current.Pop(ctx, "default")
					if err != nil {
						errorsCh <- err
						return
					}
					if p == nil {
						return
					}
					claims <- p.ID
					data, err := current.IncrementBatchCounter(ctx, "batch", "count", 1)
					if err != nil {
						errorsCh <- err
						return
					}
					counters <- data["count"].(float64)
				}
			})
		}
		wg.Wait()
		close(claims)
		close(counters)
		close(errorsCh)
		for err := range errorsCh {
			t.Error(err)
		}
		seen := map[string]bool{}
		for id := range claims {
			if seen[id] {
				t.Errorf("job claimed twice: %s", id)
			}
			seen[id] = true
		}
		if len(seen) != count {
			t.Fatalf("claimed %d jobs, want %d", len(seen), count)
		}
		values := map[float64]bool{}
		for value := range counters {
			values[value] = true
		}
		if len(values) != count || !values[count] || !values[1] {
			t.Fatalf("counter snapshots are not atomic: %v", values)
		}
		sqlTestCheck(t, d.UpdateBatch(ctx, "batch", map[string]any{"done": true}))
		data, err := peer.GetBatch(ctx, "batch")
		sqlTestCheck(t, err)
		if data["count"] != float64(count) || data["name"] != "unchanged" || data["done"] != true {
			t.Fatalf("batch patch lost fields: %v", data)
		}
		field := `a.b"[0]`
		data, err = d.IncrementBatchCounter(ctx, "batch", field, -2)
		sqlTestCheck(t, err)
		if data[field] != float64(-2) {
			t.Fatalf("counter field treated as JSON path: %v", data)
		}
		if _, err = d.IncrementBatchCounter(ctx, "batch", "name", 1); err == nil {
			t.Fatal("incremented a nonnumeric counter")
		}
		if data, err = d.IncrementBatchCounter(ctx, "missing", "count", 1); err != nil || data != nil {
			t.Fatalf("missing batch increment = %v, %v", data, err)
		}
		if err = d.UpdateBatch(ctx, "missing", map[string]any{}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing batch patch = %v", err)
		}
	})

	t.Run("MetricsHeartbeatsAndFlush", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		for _, status := range []string{Pending, Processing, Completed, Failed} {
			p := NewPayload("job", nil)
			p.Status, p.Queue = status, "a"
			_, err := d.Push(ctx, p)
			sqlTestCheck(t, err)
		}
		other := NewPayload("job", nil)
		other.Queue = "b"
		_, err := d.Push(ctx, other)
		sqlTestCheck(t, err)
		for _, metric := range []string{Processing, Completed, Completed, Failed, "duration"} {
			sqlTestCheck(t, d.RecordMetric(ctx, "a", metric, 500))
		}
		sqlTestCheck(t, d.RecordMetric(ctx, "b", Completed, 1))
		throughput, err := d.RecentThroughput(ctx, 60, "a")
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(throughput, map[string]int{Processing: 1, Completed: 2, Failed: 1}) {
			t.Fatalf("throughput must count events, not values: %v", throughput)
		}
		before, err := d.GetMetrics(ctx, "a")
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(before["a"], map[string]int{Pending: 1, Processing: 1, Completed: 1, Failed: 1}) {
			t.Fatalf("live status counts: %v", before)
		}
		n, err := d.PruneMetrics(ctx, 0)
		sqlTestCheck(t, err)
		if n != 6 {
			t.Fatalf("metric prune = %d", n)
		}
		after, err := d.GetMetrics(ctx, "a")
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("metric retention changed live status counts")
		}
		for _, stats := range []map[string]any{
			{"name": "z", "running": true}, {"name": "a", "running": true}, {"name": "stopped", "running": false},
		} {
			sqlTestCheck(t, d.ReportSupervisor(ctx, stats))
		}
		sqlTestCheck(t, d.ReportSupervisor(ctx, map[string]any{"name": "a", "running": true, "workers": 4}))
		stats, err := d.GetSupervisorStats(ctx, 60)
		sqlTestCheck(t, err)
		if len(stats) != 2 || stats[0]["name"] != "a" || stats[0]["workers"] != float64(4) {
			t.Fatalf("heartbeat filter/order/upsert: %v", stats)
		}
		stats, err = d.GetSupervisorStats(ctx, -1)
		sqlTestCheck(t, err)
		if len(stats) != 0 {
			t.Fatalf("stale heartbeats retained: %v", stats)
		}
		sqlTestCheck(t, d.StoreBatch(ctx, "batch", map[string]any{"count": 3}))
		sqlTestCheck(t, d.RecordMetric(ctx, "b", Completed, 1))
		sqlTestCheck(t, d.Flush(ctx, "a"))
		n, err = d.CountJobs(ctx, JobFilter{})
		sqlTestCheck(t, err)
		if n != 1 {
			t.Fatalf("scoped flush removed other queue: %d", n)
		}
		if data, err := d.GetBatch(ctx, "batch"); err != nil || data == nil {
			t.Fatalf("scoped flush removed batch: %v, %v", data, err)
		}
		sqlTestCheck(t, d.Flush(ctx, ""))
		n, err = d.CountJobs(ctx, JobFilter{})
		sqlTestCheck(t, err)
		if n != 0 {
			t.Fatalf("global flush left %d jobs", n)
		}
		if data, err := d.GetBatch(ctx, "batch"); err != nil || data != nil {
			t.Fatalf("global flush retained batch: %v, %v", data, err)
		}
		stats, err = d.GetSupervisorStats(ctx, 60)
		sqlTestCheck(t, err)
		if len(stats) != 0 {
			t.Fatal("global flush retained heartbeats")
		}
	})

	t.Run("CancellationAndChunkedDelete", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := d.Push(cancelled, NewPayload("job", nil)); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled write = %v", err)
		}
		if _, err := d.GetJobs(cancelled, JobFilter{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled query = %v", err)
		}
		jobs := make([]*JobPayload, 503)
		for i := range jobs {
			jobs[i] = NewPayload("job", nil)
		}
		ids, err := d.PushMany(ctx, jobs)
		sqlTestCheck(t, err)
		n, err := d.BulkDeleteJobs(ctx, ids, 0)
		sqlTestCheck(t, err)
		if n != len(jobs) {
			t.Fatalf("chunked delete = %d", n)
		}
	})
}

func TestSQLiteWALAndWriterContention(t *testing.T) {
	d := sqlTestSQLite(t)
	peer := sqlTestPeer(t, d)
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	var mode string
	var timeout int
	sqlTestCheck(t, db.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&mode))
	sqlTestCheck(t, db.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout))
	if strings.ToLower(mode) != "wal" || timeout != 5000 {
		t.Fatalf("SQLite configuration: mode=%s timeout=%d", mode, timeout)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	sqlTestCheck(t, err)
	defer tx.Rollback()
	finished := make(chan error, 1)
	go func() {
		_, err := peer.Push(t.Context(), NewPayload("job", nil))
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("writer bypassed transaction lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	sqlTestCheck(t, tx.Commit())
	select {
	case err := <-finished:
		sqlTestCheck(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not recover after lock release")
	}
}

func TestSQLiteStorageFullRecovery(t *testing.T) {
	d := sqlTestSQLite(t)
	ctx := t.Context()
	terminal := NewPayload("terminal", map[string]any{"large": strings.Repeat("x", 256*1024)})
	terminal.Status = Completed
	pending := NewPayload("pending", nil)
	_, err := d.PushMany(ctx, []*JobPayload{terminal, pending})
	sqlTestCheck(t, err)
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	var pages, capped int
	sqlTestCheck(t, db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages))
	sqlTestCheck(t, db.QueryRowContext(ctx, fmt.Sprintf("PRAGMA max_page_count=%d", pages)).Scan(&capped))
	if capped != pages {
		t.Fatalf("page cap was not applied: %d != %d", capped, pages)
	}
	replacement := NewPayload("replacement", map[string]any{"large": strings.Repeat("y", 128*1024)})
	d.AutoCleanupOnDiskFull = false
	if _, err = d.Push(ctx, replacement); !sqlStorageFull(err) {
		t.Fatalf("expected a real SQLITE_FULL, got %v", err)
	}
	d.AutoCleanupOnDiskFull = true
	_, err = d.Push(ctx, replacement)
	sqlTestCheck(t, err)
	if p, err := d.GetJob(ctx, terminal.ID); err != nil || p != nil {
		t.Fatalf("emergency cleanup did not remove terminal data: %#v, %v", p, err)
	}
	for _, id := range []string{pending.ID, replacement.ID} {
		p, err := d.GetJob(ctx, id)
		sqlTestCheck(t, err)
		if p == nil {
			t.Fatalf("pending or retried insert lost: %s", id)
		}
	}
}

func TestPostgresSkipLocked(t *testing.T) {
	d := sqlTestPostgres(t)
	ctx := t.Context()
	first, second := NewPayload("first", nil), NewPayload("second", nil)
	first.CreatedAt, second.CreatedAt = 1, 2
	_, err := d.PushMany(ctx, []*JobPayload{first, second})
	sqlTestCheck(t, err)
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	tx, err := db.BeginTx(ctx, nil)
	sqlTestCheck(t, err)
	defer tx.Rollback()
	var id string
	sqlTestCheck(t, tx.QueryRowContext(ctx, "SELECT id FROM baqueue_go_jobs WHERE id=$1 FOR UPDATE", first.ID).Scan(&id))
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	claimed, err := d.Pop(deadline, "default")
	sqlTestCheck(t, err)
	if claimed == nil || claimed.ID != second.ID {
		t.Fatalf("locked first row was not skipped: %#v", claimed)
	}
	sqlTestCheck(t, tx.Rollback())
	claimed, err = d.Pop(ctx, "default")
	sqlTestCheck(t, err)
	if claimed == nil || claimed.ID != first.ID {
		t.Fatalf("unlocked row was lost: %#v", claimed)
	}
}

type sqlTestCodeError int

func (e sqlTestCodeError) Error() string { return fmt.Sprintf("SQLite code %d", e) }
func (e sqlTestCodeError) Code() int     { return int(e) }

func TestSQLStorageRecoveryGuard(t *testing.T) {
	d := sqlTestSQLite(t)
	ctx := t.Context()
	terminal, pending := NewPayload("terminal", nil), NewPayload("pending", nil)
	terminal.Status = Completed
	_, err := d.PushMany(ctx, []*JobPayload{terminal, pending})
	sqlTestCheck(t, err)
	sqlTestCheck(t, d.RecordMetric(ctx, "default", Completed, 1))
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	calls := 0
	err = d.sqlRecover(ctx, db, func() error {
		calls++
		return sqlTestCodeError(13)
	})
	if !sqlStorageFull(err) || calls != 2 || d.cleaning.Load() {
		t.Fatalf("recovery recursed or leaked guard: calls=%d err=%v", calls, err)
	}
	if job, err := d.GetJob(ctx, terminal.ID); err != nil || job != nil {
		t.Fatalf("cleanup retained terminal job: %#v, %v", job, err)
	}
	if job, err := d.GetJob(ctx, pending.ID); err != nil || job == nil {
		t.Fatalf("cleanup removed pending work: %#v, %v", job, err)
	}
	metrics, err := d.RecentThroughput(ctx, 60, "")
	sqlTestCheck(t, err)
	if metrics[Completed] != 0 {
		t.Fatal("cleanup retained metrics")
	}
	for _, guarded := range []bool{false, true} {
		d.AutoCleanupOnDiskFull = guarded
		d.cleaning.Store(guarded)
		calls = 0
		err = d.sqlRecover(ctx, db, func() error { calls++; return sqlTestCodeError(13) })
		if calls != 1 || !sqlStorageFull(err) {
			t.Fatalf("disabled or reentrant cleanup retried: %d, %v", calls, err)
		}
	}
	d.cleaning.Store(false)
}

func TestSQLBusyRetry(t *testing.T) {
	d := NewSQLiteDriver(":memory:")
	calls := 0
	err := d.sqlRetry(t.Context(), func() error {
		calls++
		if calls < 3 {
			return sqlTestCodeError(5)
		}
		return nil
	})
	sqlTestCheck(t, err)
	if calls != 3 {
		t.Fatalf("busy retry count = %d", calls)
	}
	ctx, cancel := context.WithCancel(t.Context())
	calls = 0
	err = d.sqlRetry(ctx, func() error { calls++; cancel(); return sqlTestCodeError(6) })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("busy retry ignored cancellation: %d, %v", calls, err)
	}
}

func TestSQLiteBatchContract(t *testing.T) {
	sqlTestBatchContract(t, sqlTestSQLite)
}

func TestSQLitePoolOptions(t *testing.T) {
	d := NewSQLiteDriver(":memory:")
	d.MaxOpenConns, d.MaxIdleConns = 8, 6
	sqlTestConnect(t, d)
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	stats := db.Stats()
	if stats.MaxOpenConnections != 1 || stats.Idle != 1 {
		t.Fatalf("SQLite must retain its single connection: %+v", stats)
	}
}

func TestPostgresPoolOptions(t *testing.T) {
	if os.Getenv("BAQUEUE_POSTGRES_URL") == "" {
		t.Skip("BAQUEUE_POSTGRES_URL is not configured")
	}
	for _, test := range []struct {
		name               string
		maxOpen, maxIdle   int
		wantOpen, wantIdle int
	}{
		{"defaults", 0, 0, 16, 4},
		{"overrides", 6, 2, 6, 2},
		{"open-only", 2, 0, 2, 2},
		{"idle-only", 0, 1, 16, 1},
		{"nonpositive", -1, -1, 16, 4},
		{"idle-clamped-to-open", 2, 7, 2, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := sqlTestPostgres(t)
			sqlTestCheck(t, d.Disconnect(t.Context()))
			d.MaxOpenConns, d.MaxIdleConns = test.maxOpen, test.maxIdle
			sqlTestCheck(t, d.Connect(t.Context()))
			db, err := d.sqlDB()
			sqlTestCheck(t, err)
			if got := db.Stats().MaxOpenConnections; got != test.wantOpen {
				t.Fatalf("max open = %d, want %d", got, test.wantOpen)
			}
			connections := make([]*sql.Conn, 0)
			for range min(test.wantOpen, 6) {
				connection, err := db.Conn(t.Context())
				sqlTestCheck(t, err)
				t.Cleanup(func() { _ = connection.Close() })
				connections = append(connections, connection)
			}
			for _, connection := range connections {
				sqlTestCheck(t, connection.Close())
			}
			if got := db.Stats().Idle; got != test.wantIdle {
				t.Fatalf("retained idle = %d, want %d", got, test.wantIdle)
			}
		})
	}
}

func TestPostgresBatchContract(t *testing.T) {
	if os.Getenv("BAQUEUE_POSTGRES_URL") == "" {
		t.Skip("BAQUEUE_POSTGRES_URL is not configured")
	}
	sqlTestBatchContract(t, sqlTestPostgres)
}

func sqlTestBatchMetadata(id string, total int) map[string]any {
	return map[string]any{"id": id, "total": total, "status": Processing, "completed_count": 0, "failed_count": 0}
}

func sqlTestBatchContract(t *testing.T, factory sqlTestFactory) {
	t.Run("FirstTerminalOutcomeSurvivesManualRetry", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		batchID := "batch"
		sqlTestCheck(t, d.StoreBatch(ctx, batchID, sqlTestBatchMetadata(batchID, 2)))
		for _, status := range []string{Completed, Failed} {
			p := NewPayload("job", nil)
			p.BatchID, p.Queue = &batchID, status
			_, err := d.Push(ctx, p)
			sqlTestCheck(t, err)
			claimed, err := d.Pop(ctx, p.Queue)
			sqlTestCheck(t, err)
			stale := claimed.Clone()
			if status == Completed {
				sqlTestCheck(t, d.Complete(ctx, claimed))
			} else {
				sqlTestCheck(t, d.Fail(ctx, claimed, "first terminal failure"))
			}
			before, err := d.GetBatch(ctx, batchID)
			sqlTestCheck(t, err)
			outcomes, ok := before["member_outcomes"].(map[string]any)
			if !ok || outcomes[p.ID] != status {
				t.Fatalf("first terminal outcome not persisted: %v", before)
			}
			if err = d.Complete(ctx, stale); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale terminal transition = %v", err)
			}
			stored, err := d.GetJob(ctx, p.ID)
			sqlTestCheck(t, err)
			sqlTestCheck(t, d.Release(ctx, stored, 0))
			pending, err := d.GetBatch(ctx, batchID)
			sqlTestCheck(t, err)
			if !reflect.DeepEqual(pending, before) {
				t.Fatal("release reopened the original batch outcome")
			}
			claimed, err = d.Pop(ctx, p.Queue)
			sqlTestCheck(t, err)
			if status == Failed {
				sqlTestCheck(t, d.Complete(ctx, claimed))
			} else {
				sqlTestCheck(t, d.Fail(ctx, claimed, "manual retry failure"))
			}
			after, err := d.GetBatch(ctx, batchID)
			sqlTestCheck(t, err)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("manual retry changed original counters/outcome: before=%v after=%v", before, after)
			}
		}
		data, err := d.GetBatch(ctx, batchID)
		sqlTestCheck(t, err)
		if data["completed_count"] != float64(1) || data["failed_count"] != float64(1) {
			t.Fatalf("incorrect terminal counters: %v", data)
		}
		sqlTestCheck(t, d.Disconnect(ctx))
		sqlTestCheck(t, d.Connect(ctx))
		persisted, err := d.GetBatch(ctx, batchID)
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(data, persisted) {
			t.Fatalf("outcomes did not survive reconnect: %v", persisted)
		}
		missing := "missing"
		for _, fail := range []bool{false, true} {
			p := NewPayload("job", nil)
			p.BatchID = &missing
			_, err := d.Push(ctx, p)
			sqlTestCheck(t, err)
			claimed, err := d.Pop(ctx, p.Queue)
			sqlTestCheck(t, err)
			if fail {
				sqlTestCheck(t, d.Fail(ctx, claimed, "missing batch"))
			} else {
				sqlTestCheck(t, d.Complete(ctx, claimed))
			}
		}
	})

	t.Run("BatchWriteFailureRollsBackTerminalJob", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		batchID := "batch"
		sqlTestCheck(t, d.StoreBatch(ctx, batchID, sqlTestBatchMetadata(batchID, 1)))
		p := NewPayload("job", nil)
		p.BatchID = &batchID
		_, err := d.Push(ctx, p)
		sqlTestCheck(t, err)
		claimed, err := d.Pop(ctx, p.Queue)
		sqlTestCheck(t, err)
		before, err := d.GetBatch(ctx, batchID)
		sqlTestCheck(t, err)
		db, err := d.sqlDB()
		sqlTestCheck(t, err)
		createTrigger := `CREATE TRIGGER sql_test_reject_batch BEFORE UPDATE ON baqueue_go_batches
 BEGIN SELECT RAISE(ABORT, 'batch update blocked'); END`
		dropTrigger := "DROP TRIGGER sql_test_reject_batch"
		if d.postgres {
			_, err = db.ExecContext(ctx, `CREATE FUNCTION sql_test_reject_batch() RETURNS trigger AS $$
 BEGIN RAISE EXCEPTION 'batch update blocked'; END; $$ LANGUAGE plpgsql`)
			sqlTestCheck(t, err)
			createTrigger = "CREATE TRIGGER sql_test_reject_batch BEFORE UPDATE ON baqueue_go_batches FOR EACH ROW EXECUTE FUNCTION sql_test_reject_batch()"
			dropTrigger += " ON baqueue_go_batches"
		}
		_, err = db.ExecContext(ctx, createTrigger)
		sqlTestCheck(t, err)
		for _, fail := range []bool{false, true} {
			incoming := claimed.Clone()
			incoming.History = append(incoming.History, Attempt{Attempt: incoming.Attempts, StartedAt: *incoming.StartedAt, FinishedAt: Now(), Status: Completed})
			if fail {
				err = d.Fail(ctx, incoming, "failed")
			} else {
				err = d.Complete(ctx, incoming)
			}
			if err == nil {
				t.Fatal("terminal transition succeeded despite rejected batch update")
			}
			if incoming.Status != Processing {
				t.Fatal("failed transaction mutated the caller's lifecycle state")
			}
			stored, err := d.GetJob(ctx, p.ID)
			sqlTestCheck(t, err)
			if !reflect.DeepEqual(stored, claimed) {
				t.Fatalf("terminal job/history committed without its batch: %#v", stored)
			}
			after, err := d.GetBatch(ctx, batchID)
			sqlTestCheck(t, err)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("failed transaction changed batch metadata: %v", after)
			}
		}
		_, err = db.ExecContext(ctx, dropTrigger)
		sqlTestCheck(t, err)
		sqlTestCheck(t, d.Complete(ctx, claimed))
		data, err := d.GetBatch(ctx, batchID)
		sqlTestCheck(t, err)
		if data["completed_count"] != float64(1) || data["failed_count"] != float64(0) {
			t.Fatalf("retry after transaction rollback was miscounted: %v", data)
		}
	})

	t.Run("ConcurrentMembersDoNotLoseOutcomes", func(t *testing.T) {
		d := factory(t)
		peer := sqlTestPeer(t, d)
		ctx := t.Context()
		const count = 24
		batchID := "batch"
		sqlTestCheck(t, d.StoreBatch(ctx, batchID, sqlTestBatchMetadata(batchID, count)))
		jobs := make([]*JobPayload, count)
		for i := range jobs {
			jobs[i] = NewPayload("job", map[string]any{"fail": i%2 == 0})
			jobs[i].BatchID = &batchID
		}
		_, err := d.PushMany(ctx, jobs)
		sqlTestCheck(t, err)
		var wg sync.WaitGroup
		errorsCh := make(chan error, 6)
		for worker := 0; worker < 6; worker++ {
			current := []*SQLDriver{d, peer}[worker%2]
			wg.Go(func() {
				for {
					p, err := current.Pop(ctx, "default")
					if err != nil {
						errorsCh <- err
						return
					}
					if p == nil {
						return
					}
					if p.Data["fail"] == true {
						err = current.Fail(ctx, p, "terminal failure")
					} else {
						err = current.Complete(ctx, p)
					}
					if err != nil {
						errorsCh <- err
						return
					}
				}
			})
		}
		wg.Wait()
		close(errorsCh)
		for err := range errorsCh {
			t.Error(err)
		}
		data, err := d.GetBatch(ctx, batchID)
		sqlTestCheck(t, err)
		outcomes, ok := data["member_outcomes"].(map[string]any)
		if !ok || len(outcomes) != count || data["completed_count"] != float64(count/2) || data["failed_count"] != float64(count/2) {
			t.Fatalf("concurrent terminal outcomes were lost: %v", data)
		}
		for _, p := range jobs {
			want := Completed
			if p.Data["fail"] == true {
				want = Failed
			}
			if outcomes[p.ID] != want {
				t.Errorf("outcome for %s = %v, want %s", p.ID, outcomes[p.ID], want)
			}
		}
	})

	t.Run("PendingSelectionFiltersBeforeLimit", func(t *testing.T) {
		d := factory(t)
		ctx := t.Context()
		fixtures := []map[string]any{
			{"id": "00-active", "status": Processing, "total": 10, "completed_count": 1},
			{"id": "01-completed", "status": Completed, "total": 1, "completed_count": 1},
			{"id": "02-catch-sent", "status": Processing, "total": 10, "failed_count": 1, "catch_dispatched": true},
			{"id": "03-allowed-failure", "status": Processing, "total": 10, "failed_count": 1, "allow_failures": true},
			{"id": "10-catch-pending", "status": Processing, "total": 10, "failed_count": 1, "catch_job": "catch-handler"},
			{"id": "11-all-done", "status": Processing, "total": 2, "completed_count": 2},
			{"id": "12-over-count", "status": Processing, "total": 1, "completed_count": 1, "failed_count": 1},
			{"id": "13-empty", "status": Processing, "total": 0},
		}
		for _, data := range fixtures {
			sqlTestCheck(t, d.StoreBatch(ctx, data["id"].(string), data))
		}
		for i := 0; i < 101; i++ {
			id := fmt.Sprintf("20-ready-%03d", i)
			sqlTestCheck(t, d.StoreBatch(ctx, id, map[string]any{"id": id, "status": Processing, "total": 1, "completed_count": 1}))
		}
		for _, test := range []struct {
			limit int
			want  int
		}{{2, 2}, {0, 100}, {-1, 100}, {200, 105}} {
			batches, err := d.GetPendingBatches(ctx, test.limit)
			sqlTestCheck(t, err)
			if len(batches) != test.want {
				t.Fatalf("pending limit %d returned %d, want %d", test.limit, len(batches), test.want)
			}
			if batches[0]["id"] != "10-catch-pending" || batches[1]["id"] != "11-all-done" {
				t.Fatalf("inactive rows consumed limit or unfinished catch was skipped: %v", batches[:2])
			}
			for _, data := range batches {
				if !BatchNeedsReconciliation(data) {
					t.Fatalf("returned ineligible batch: %v", data)
				}
			}
		}
		sqlTestCheck(t, d.UpdateBatch(ctx, "10-catch-pending", map[string]any{"catch_dispatched": true}))
		batches, err := d.GetPendingBatches(ctx, 1)
		sqlTestCheck(t, err)
		if len(batches) != 1 || batches[0]["id"] != "11-all-done" {
			t.Fatalf("active batch remained eligible after catch dispatch: %v", batches)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err = d.GetPendingBatches(cancelled, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled pending query = %v", err)
		}
	})
}
