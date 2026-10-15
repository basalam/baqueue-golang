package baqueue

import (
	"context"
	"math"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynamicQueueContract(t *testing.T) {
	factories := map[string]func(*testing.T) Driver{
		"memory":   func(t *testing.T) Driver { return NewMemoryDriver() },
		"sqlite":   func(t *testing.T) Driver { return sqlTestSQLite(t) },
		"postgres": func(t *testing.T) Driver { return sqlTestPostgres(t) },
		"redis":    func(t *testing.T) Driver { d, _ := redisTestDriver(t); return d },
	}
	for backend, factory := range factories {
		t.Run(backend, func(t *testing.T) {
			d := factory(t)
			dd, ok := d.(DynamicQueueDriver)
			if !ok {
				t.Fatal("dynamic driver extension missing")
			}
			ctx := t.Context()
			t.Run("EmptyExpiryAndRecreation", func(t *testing.T) {
				info, err := dd.RegisterDynamicQueue(ctx, "empty", 0.02)
				redisOK(t, err)
				if !info.Active || info.IdleSince == nil {
					t.Fatal(info)
				}
				names, err := d.Queues(ctx)
				redisOK(t, err)
				if !slices.Contains(names, "empty") {
					t.Fatal(names)
				}
				time.Sleep(30 * time.Millisecond)
				expired, err := dd.ExpireDynamicQueues(ctx)
				redisOK(t, err)
				if !slices.Contains(expired, "empty") {
					t.Fatal(expired)
				}
				names, err = d.Queues(ctx)
				redisOK(t, err)
				if slices.Contains(names, "empty") {
					t.Fatal(names)
				}
				p := NewPayload("job", nil)
				p.Queue = "empty"
				_, err = dd.PushDynamic(ctx, []*JobPayload{p}, "empty", 0.02)
				redisOK(t, err)
				claimed, err := d.Pop(ctx, p.Queue)
				redisOK(t, err)
				if claimed == nil || claimed.ID != p.ID {
					t.Fatal("recreated job not runnable")
				}
			})
			t.Run("DelayedProcessingHistoryAndRetry", func(t *testing.T) {
				p := NewPayload("job", map[string]any{"item": 42})
				p.Queue = "history"
				p.Backoff = Backoff{Delays: []float64{2, 4, 8}}
				later := Now() + 100
				p.DelayUntil = &later
				_, err := dd.PushDynamic(ctx, []*JobPayload{p}, p.Queue, 0.02)
				redisOK(t, err)
				time.Sleep(30 * time.Millisecond)
				expired, err := dd.ExpireDynamicQueues(ctx)
				redisOK(t, err)
				if slices.Contains(expired, p.Queue) {
					t.Fatal("delayed job expired")
				}
				claimed, err := d.Pop(ctx, p.Queue)
				redisOK(t, err)
				if claimed != nil {
					t.Fatal("delayed job claimed early")
				}
				promoted, err := d.Promote(ctx, p.ID)
				redisOK(t, err)
				if !promoted {
					t.Fatal("not promoted")
				}
				claimed, err = d.Pop(ctx, p.Queue)
				redisOK(t, err)
				if claimed == nil {
					t.Fatal("missing claim")
				}
				time.Sleep(30 * time.Millisecond)
				expired, err = dd.ExpireDynamicQueues(ctx)
				redisOK(t, err)
				if slices.Contains(expired, p.Queue) {
					t.Fatal("processing queue expired")
				}
				claimed.History = append(claimed.History, Attempt{Attempt: 1, StartedAt: *claimed.StartedAt, FinishedAt: Now(), Status: Failed})
				redisOK(t, d.Fail(ctx, claimed, "failure"))
				before, err := d.GetJob(ctx, p.ID)
				redisOK(t, err)
				_, err = dd.ExpireDynamicQueues(ctx)
				redisOK(t, err)
				time.Sleep(30 * time.Millisecond)
				expired, err = dd.ExpireDynamicQueues(ctx)
				redisOK(t, err)
				if !slices.Contains(expired, p.Queue) {
					t.Fatal("terminal queue not expired")
				}
				after, err := d.GetJob(ctx, p.ID)
				redisOK(t, err)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("expiry changed retained job")
				}
				redisOK(t, d.Release(ctx, after, 100))
				if after.Attempts != 1 || after.MaxAttempts != 3 || !reflect.DeepEqual(after.History, before.History) || !reflect.DeepEqual(after.Backoff, before.Backoff) {
					t.Fatal("retry changed policy/history")
				}
				infos, err := dd.GetDynamicQueues(ctx)
				redisOK(t, err)
				if !slices.ContainsFunc(infos, func(q DynamicQueueInfo) bool { return q.Name == p.Queue && q.Active && q.IdleSince == nil }) {
					t.Fatal(infos)
				}
			})
			t.Run("StaticConflictAndValidation", func(t *testing.T) {
				p := NewPayload("job", nil)
				p.Queue = "static"
				_, err := d.Push(ctx, p)
				redisOK(t, err)
				if _, err = dd.RegisterDynamicQueue(ctx, p.Queue, 30); err == nil {
					t.Fatal("converted ordinary queue")
				}
				for _, ttl := range []float64{0, -1, math.NaN(), math.Inf(1)} {
					if _, err = dd.RegisterDynamicQueue(ctx, "invalid", ttl); err == nil {
						t.Fatal("accepted invalid timeout")
					}
				}
				_, err = dd.RegisterDynamicQueue(ctx, "conflict", 30)
				redisOK(t, err)
				p = NewPayload("job", nil)
				p.Queue = "conflict"
				if _, err = dd.PushDynamic(ctx, []*JobPayload{p}, p.Queue, 40); err == nil {
					t.Fatal("accepted changed policy")
				}
				stored, err := d.GetJob(ctx, p.ID)
				redisOK(t, err)
				if stored != nil {
					t.Fatal("partial conflicting enqueue")
				}
			})
			t.Run("ConcurrentExpiryDispatchAndClaim", func(t *testing.T) {
				_, err := dd.RegisterDynamicQueue(ctx, "race", 0.01)
				redisOK(t, err)
				time.Sleep(20 * time.Millisecond)
				p := NewPayload("job", nil)
				p.Queue = "race"
				var wg sync.WaitGroup
				errs := make(chan error, 2)
				wg.Add(2)
				go func() { defer wg.Done(); _, e := dd.ExpireDynamicQueues(ctx); errs <- e }()
				go func() { defer wg.Done(); _, e := dd.PushDynamic(ctx, []*JobPayload{p}, p.Queue, 0.01); errs <- e }()
				wg.Wait()
				close(errs)
				for e := range errs {
					redisOK(t, e)
				}
				var claims atomic.Int32
				wg.Add(2)
				for range 2 {
					go func() {
						defer wg.Done()
						job, e := d.Pop(ctx, p.Queue)
						if e != nil {
							t.Error(e)
						}
						if job != nil {
							claims.Add(1)
						}
					}()
				}
				wg.Wait()
				if claims.Load() != 1 {
					t.Fatal(claims.Load())
				}
			})
			t.Run("RegistrationRefreshAndFlush", func(t *testing.T) {
				info, err := dd.RegisterDynamicQueue(ctx, "refresh", 30)
				redisOK(t, err)
				time.Sleep(time.Millisecond)
				next, err := dd.RegisterDynamicQueue(ctx, "refresh", 30)
				redisOK(t, err)
				if next.CreatedAt != info.CreatedAt || next.IdleSince == nil || *next.IdleSince <= *info.IdleSince {
					t.Fatal("registration did not refresh idle clock")
				}
				redisOK(t, d.Flush(ctx, "refresh"))
				infos, err := dd.GetDynamicQueues(ctx)
				redisOK(t, err)
				if slices.ContainsFunc(infos, func(q DynamicQueueInfo) bool { return q.Name == "refresh" }) {
					t.Fatal(infos)
				}
				redisOK(t, d.Flush(ctx, ""))
				infos, err = dd.GetDynamicQueues(ctx)
				redisOK(t, err)
				if len(infos) != 0 {
					t.Fatal(infos)
				}
			})
		})
	}
}

func dynamicWait(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDynamicSupervisorDiscoveryScaleAndExpiry(t *testing.T) {
	q := testQueue(t)
	var processed atomic.Int32
	gate := make(chan struct{})
	redisOK(t, q.RegisterFunc("job", func(ctx context.Context, data map[string]any) (any, error) {
		select {
		case <-gate:
			processed.Add(1)
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	c := DefaultSupervisorConfig()
	c.Queues = []string{}
	c.MinWorkers = 0
	c.MaxWorkers = 2
	c.Sleep = 0.005
	c.DynamicQueuePollSeconds = 0.01
	s := NewSupervisor(q, c)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	defer func() { cancel(); redisOK(t, <-done) }()
	handle, err := q.CreateDynamicQueue(ctx, "session:live", 30*time.Millisecond)
	redisOK(t, err)
	jobs := make([]JobRequest, 24)
	for i := range jobs {
		jobs[i] = JobRequest{Name: "job"}
	}
	_, err = handle.Bulk(ctx, jobs)
	redisOK(t, err)
	dynamicWait(t, func() bool { return s.Stats()["workers"].(int) == 2 })
	if !slices.Contains(s.Stats()["queues"].([]string), handle.Name) {
		t.Fatal("not discovered")
	}
	s.Scale(0)
	s.Scale(2)
	s.mu.Lock()
	physical := len(s.workers) + len(s.retiring)
	s.mu.Unlock()
	if physical > 2 {
		t.Fatal("retiring workers exceeded cap")
	}
	close(gate)
	dynamicWait(t, func() bool { return processed.Load() == 24 })
	dynamicWait(t, func() bool { return !slices.Contains(s.Stats()["queues"].([]string), handle.Name) })
	dynamicWait(t, func() bool { return s.Stats()["workers"].(int) == 0 })
	count, err := q.Driver.CountJobs(ctx, JobFilter{Queue: handle.Name, Status: Completed})
	redisOK(t, err)
	if count != 24 {
		t.Fatal(count)
	}
}

func TestDynamicFacadeAndSubscriptionSnapshots(t *testing.T) {
	q := testQueue(t)
	ctx := t.Context()
	h, err := q.CreateDynamicQueue(ctx, "")
	redisOK(t, err)
	if h.Name == "" || h.IdleTimeoutSeconds != 30 {
		t.Fatal(h)
	}
	id, err := h.Later(ctx, "job", time.Hour, nil, OnQueue("wrong"), WithMaxAttempts(7))
	redisOK(t, err)
	p, err := q.Driver.GetJob(ctx, id)
	redisOK(t, err)
	if p.Queue != h.Name || p.MaxAttempts != 7 || p.DelayUntil == nil {
		t.Fatal(p)
	}
	c := DefaultSupervisorConfig()
	c.Queues = []string{"static"}
	c.DynamicQueuePrefixes = []string{"tenant:"}
	s := NewSupervisor(q, c)
	_, err = q.CreateDynamicQueue(ctx, "tenant:a")
	redisOK(t, err)
	_, err = q.CreateDynamicQueue(ctx, "other:a")
	redisOK(t, err)
	redisOK(t, s.refreshDynamicQueues(ctx))
	if !reflect.DeepEqual(s.queueSnapshot(), []string{"static", "tenant:a"}) {
		t.Fatal(s.queueSnapshot())
	}
	w := NewWorker(q, WorkerOptions{Queues: []string{}})
	worked, err := w.RunOnce(ctx)
	redisOK(t, err)
	if worked {
		t.Fatal("empty subscriptions consumed default queue")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			w.SetQueues([]string{"empty"})
			w.SetQueues(nil)
		}
	}()
	for range 100 {
		_, err = w.RunOnce(ctx)
		redisOK(t, err)
		_ = w.Stats()
	}
	wg.Wait()
}
