package baqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testQueue(t *testing.T) *Queue {
	t.Helper()
	d := NewMemoryDriver()
	if err := d.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	return NewQueue(d)
}
func mustPush(t *testing.T, q *Queue, name string, opts ...JobOption) string {
	t.Helper()
	id, err := q.Push(context.Background(), name, map[string]any{"key": "value"}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func getJob(t *testing.T, q *Queue, id string) *JobPayload {
	t.Helper()
	p, err := q.Driver.GetJob(context.Background(), id)
	if err != nil || p == nil {
		t.Fatalf("get job: %v %v", p, err)
	}
	return p
}

func TestPayloadPythonWireFormat(t *testing.T) {
	raw := `{"id":"abc","job_class":"myapp.Email","data":{"to":"a@b"},"queue":"emails","attempts":2,"max_attempts":3,"backoff":[0,10,60],"timeout":60,"tags":["mail"],"batch_id":null,"delay_until":null,"created_at":123.25,"updated_at":124.5,"started_at":124.5,"completed_at":null,"failed_at":null,"status":"processing","error":null,"history":[{"attempt":1,"started_at":123.5,"finished_at":124,"status":"failed","error":"oops","will_retry":true,"next_retry_at":124.5}]}`
	var p JobPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatal(err)
	}
	if p.MaxAttempts != 3 || p.Attempts != 2 || p.Backoff.Delays[2] != 60 || !p.History[0].WillRetry {
		t.Fatal(p)
	}
	copy := p.Clone()
	copy.Data["to"] = "changed"
	if p.Data["to"] != "a@b" {
		t.Fatal("clone shares data")
	}
	for _, b := range []Backoff{{Strategy: "fixed"}, {Delays: []float64{1, 2}}, {Strategy: "exponential"}} {
		encoded, _ := json.Marshal(b)
		var decoded Backoff
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPayloadPreservesLargeIntegerData(t *testing.T) {
	p := NewPayload("job", map[string]any{"id": int64(9223372036854775807)})
	copy := p.Clone()
	if fmt.Sprint(copy.Data["id"]) != "9223372036854775807" {
		t.Fatal(copy.Data)
	}
	encoded, err := json.Marshal(copy)
	if err != nil || !strings.Contains(string(encoded), `"id":9223372036854775807`) {
		t.Fatal(string(encoded), err)
	}
}
func TestBackoffAndMaxAttempts(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		exp := ComputeDelay(Backoff{Strategy: "exponential"}, attempt)
		base := 5 * float64(uint64(1)<<uint(attempt-1))
		if exp < min(base, 3600) || exp > min(base*1.1, 3600) {
			t.Fatal(exp)
		}
		if ComputeDelay(Backoff{Strategy: "linear"}, attempt) != float64(attempt*5) {
			t.Fatal("linear")
		}
		if ComputeDelay(Backoff{Strategy: "fixed"}, attempt) != 5 {
			t.Fatal("fixed")
		}
	}
	if ComputeDelay(Backoff{Delays: []float64{1, 3}}, 9) != 3 || ShouldRetry(3, 3) || !ShouldRetry(2, 3) {
		t.Fatal("retry policy")
	}
}
func TestWorkerRetryHistoryAndPolicies(t *testing.T) {
	ctx := context.Background()
	q := testQueue(t)
	calls := 0
	if err := q.RegisterFunc("retry", func(context.Context, map[string]any) (any, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("temporary")
		}
		return "done", nil
	}, WithMaxAttempts(3), WithBackoff(Backoff{Delays: []float64{0}})); err != nil {
		t.Fatal(err)
	}
	id := mustPush(t, q, "retry")
	w := NewWorker(q, WorkerOptions{})
	for i := 1; i <= 3; i++ {
		worked, err := w.RunOnce(ctx)
		if err != nil || !worked {
			t.Fatalf("run %d: %v", i, err)
		}
		p := getJob(t, q, id)
		if p.Attempts != i || p.MaxAttempts != 3 || len(p.History) != i || len(p.Backoff.Delays) != 1 {
			t.Fatalf("policy/history changed: %+v", p)
		}
		if i < 3 && p.Status != Pending {
			t.Fatal(p.Status)
		}
	}
	p := getJob(t, q, id)
	if p.Status != Completed || !p.History[0].WillRetry || p.History[2].WillRetry {
		t.Fatal(p)
	}
	m, _ := q.Driver.RecentThroughput(ctx, 60, "")
	if m[Processing] != 3 || m[Completed] != 1 || m[Failed] != 0 {
		t.Fatal(m)
	}
}
func TestWorkerFailurePanicTimeoutAndUnknownHandler(t *testing.T) {
	for _, kind := range []string{"error", "panic", "timeout", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			q := testQueue(t)
			if kind != "unknown" {
				_ = q.RegisterFunc(kind, func(ctx context.Context, _ map[string]any) (any, error) {
					switch kind {
					case "panic":
						panic("boom")
					case "timeout":
						<-ctx.Done()
						return nil, ctx.Err()
					default:
						return nil, errors.New("failed")
					}
				})
			}
			id := mustPush(t, q, kind, WithMaxAttempts(1), WithTimeout(10*time.Millisecond))
			w := NewWorker(q, WorkerOptions{})
			if _, err := w.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			p := getJob(t, q, id)
			if p.Status != Failed || p.Attempts != 1 || len(p.History) != 1 || p.Error == nil {
				t.Fatal(p)
			}
			if worked, _ := w.RunOnce(context.Background()); worked {
				t.Fatal("max attempts ignored")
			}
		})
	}
}
func TestMemoryConcurrentClaimsAndCopyIsolation(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	jobs := make([]JobRequest, 300)
	for i := range jobs {
		jobs[i] = JobRequest{Name: "job"}
	}
	ids, err := q.Bulk(ctx, jobs)
	if err != nil {
		t.Fatal(err)
	}
	var seen sync.Map
	var count atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				p, err := q.Driver.Pop(ctx, "default")
				if err != nil {
					t.Error(err)
					return
				}
				if p == nil {
					return
				}
				if _, loaded := seen.LoadOrStore(p.ID, true); loaded {
					t.Error("duplicate claim")
				}
				if p.Attempts != 1 {
					t.Error("multiple attempts")
				}
				if err = q.Driver.Complete(ctx, p); err != nil {
					t.Error(err)
				}
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if int(count.Load()) != len(ids) {
		t.Fatal(count.Load())
	}
	p := getJob(t, q, ids[0])
	p.MaxAttempts = 100
	p.Data["x"] = 1
	if getJob(t, q, ids[0]).MaxAttempts != 3 {
		t.Fatal("query leaked mutable payload")
	}
}
func TestDelayedPromoteAndStaleAttempt(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	id, err := q.Later(ctx, "job", time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := q.Driver.Pop(ctx, "default"); p != nil {
		t.Fatal("early claim")
	}
	if ok, err := q.Driver.Promote(ctx, id); err != nil || !ok {
		t.Fatal(err)
	}
	_, _ = q.Driver.Promote(ctx, id)
	p, err := q.Driver.Pop(ctx, "default")
	if err != nil || p == nil {
		t.Fatal(err)
	}
	if dup, _ := q.Driver.Pop(ctx, "default"); dup != nil {
		t.Fatal("duplicate promotion")
	}
	if n, err := q.Driver.RequeueStuckJobs(ctx, 0, "default"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	newClaim, _ := q.Driver.Pop(ctx, "default")
	if newClaim.Attempts != 2 {
		t.Fatal(newClaim)
	}
	if err = q.Driver.Complete(ctx, p); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale completion accepted: %v", err)
	}
	newClaim.MaxAttempts = 1
	newClaim.Backoff = Backoff{Strategy: "fixed"}
	newClaim.History = append(newClaim.History, Attempt{Attempt: 2, StartedAt: *newClaim.StartedAt})
	if err = q.Driver.Release(ctx, newClaim, 0); err != nil {
		t.Fatal(err)
	}
	stored := getJob(t, q, id)
	if stored.MaxAttempts != 3 || stored.Backoff.Strategy != "exponential" || stored.Attempts != 2 || len(stored.History) != 1 {
		t.Fatal(stored)
	}
}
func TestQueueFairnessAndEventListeners(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	seen := []string{}
	unsubscribe := q.Events.On("job.completed", func(_ context.Context, e Event) error {
		seen = append(seen, e.Data["payload"].(*JobPayload).Queue)
		return nil
	})
	_ = q.RegisterFunc("job", func(context.Context, map[string]any) (any, error) { return nil, nil })
	for range 3 {
		mustPush(t, q, "job", OnQueue("a"))
		mustPush(t, q, "job", OnQueue("b"))
	}
	w := NewWorker(q, WorkerOptions{Queues: []string{"a", "b"}})
	for range 4 {
		_, _ = w.RunOnce(ctx)
	}
	unsubscribe()
	_, _ = w.RunOnce(ctx)
	if strings.Join(seen, ",") != "a,b,a,b" {
		t.Fatal(seen)
	}
}
func TestBatchCallbacksAcrossQueueInstances(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			producer := testQueue(t)
			ctx := context.Background()
			jobs := []JobRequest{{Name: "job", Options: []JobOption{WithMaxAttempts(1)}}, {Name: "job", Options: []JobOption{WithMaxAttempts(1)}}}
			b, err := producer.Batch(jobs).Name("test").Then("then").Catch("catch").Finally("finally").Dispatch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			consumer := NewQueue(producer.Driver)
			calls := 0
			_ = consumer.RegisterFunc("job", func(context.Context, map[string]any) (any, error) {
				calls++
				if fail && calls == 2 {
					return nil, errors.New("oops")
				}
				return nil, nil
			})
			w := NewWorker(consumer, WorkerOptions{})
			for range 2 {
				if _, err = w.RunOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			m, _ := producer.Driver.GetBatch(ctx, b.BatchID)
			if m["status"] != Completed {
				t.Fatal(m)
			}
			pending, _ := producer.Driver.GetJobs(ctx, JobFilter{Status: Pending})
			names := map[string]bool{}
			for _, p := range pending {
				names[p.JobClass] = true
			}
			if !names["finally"] || names["then"] == fail || names["catch"] != fail {
				t.Fatal(names)
			}
		})
	}
}
func TestSchedulerDistinctEntriesAndPruner(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	s, err := NewScheduler(q, []ScheduleEntry{{JobClass: "job", Every: 60, Queue: "a"}, {JobClass: "job", Every: 60, Queue: "b"}, {JobClass: "cron", Cron: "*/5 * * * *"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = s.Tick(ctx, now); err != nil {
		t.Fatal(err)
	}
	_ = s.Tick(ctx, now)
	n, _ := q.Driver.CountJobs(ctx, JobFilter{})
	if n != 3 {
		t.Fatal(n)
	}
	_ = s.Tick(ctx, now.Add(time.Minute))
	n, _ = q.Driver.CountJobs(ctx, JobFilter{})
	if n < 5 {
		t.Fatal(n)
	}
	p, _ := q.Driver.Pop(ctx, "a")
	_ = q.Driver.Complete(ctx, p)
	cfg := DefaultConfig()
	cfg.PruneCompletedSeconds = 0.000001
	cfg.PruneOtherSeconds = 0
	time.Sleep(time.Millisecond)
	pr := Pruner{q, cfg}
	n, err = pr.RunOnce(ctx)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if _, err = NewScheduler(q, []ScheduleEntry{{JobClass: "bad", Cron: "invalid"}}); err == nil {
		t.Fatal("accepted invalid cron")
	}
}
func TestSupervisorRecoveryScalingAndShutdown(t *testing.T) {
	q := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.RegisterFunc("job", func(context.Context, map[string]any) (any, error) { return nil, nil })
	id := mustPush(t, q, "job")
	p, _ := q.Driver.Pop(ctx, "default")
	_ = p
	c := DefaultSupervisorConfig()
	c.Name = "test"
	c.Sleep = .005
	c.MaxWorkers = 3
	c.StuckProcessingSeconds = .001
	c.StuckCheckIntervalSeconds = .01
	time.Sleep(2 * time.Millisecond)
	s := NewSupervisor(q, c)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if getJob(t, q, id).Status == Completed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if getJob(t, q, id).Status != Completed {
		t.Fatal("stuck recovery failed")
	}
	s.Scale(3)
	if s.Stats()["workers"] != 3 {
		t.Fatal(s.Stats())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown hung")
	}
	stats, _ := q.Driver.GetSupervisorStats(context.Background(), 10)
	if len(stats) != 0 {
		t.Fatal(stats)
	}
}
func TestConfigPartialDefaultsAndValidation(t *testing.T) {
	c, err := LoadConfig(strings.NewReader(`{"supervisors":[{"name":"emails","queues":["emails"]}],"auto_prune":false}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.AutoPrune || c.Supervisors[0].MaxWorkers != 10 || !c.Supervisors[0].RecoverStuckJobs {
		t.Fatal(c)
	}
	if _, err = LoadConfig(strings.NewReader(`{"surprise":true}`)); err == nil {
		t.Fatal("unknown option accepted")
	}
}

func TestSchedulerListenersCanAddSchedules(t *testing.T) {
	q := testQueue(t)
	s, err := NewScheduler(q, []ScheduleEntry{{JobClass: "first", Every: 60}})
	if err != nil {
		t.Fatal(err)
	}
	q.Events.On("job.pushed", func(_ context.Context, _ Event) error {
		return s.Add(ScheduleEntry{JobClass: "added", Every: 60})
	})
	done := make(chan error, 1)
	go func() { done <- s.Tick(context.Background(), time.Now()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("event callback deadlocked scheduler")
	}
}

func TestLifecycleEventsUsePersistedState(t *testing.T) {
	q := testQueue(t)
	_ = q.RegisterFunc("job", func(context.Context, map[string]any) (any, error) { return nil, nil })
	var seen *JobPayload
	q.Events.On("job.completed", func(_ context.Context, e Event) error { seen = e.Data["payload"].(*JobPayload); return nil })
	id := mustPush(t, q, "job")
	if _, err := NewWorker(q, WorkerOptions{}).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if seen == nil || seen.Status != Completed || seen.CompletedAt == nil || *seen.CompletedAt != *getJob(t, q, id).CompletedAt {
		t.Fatalf("stale event payload: %+v", seen)
	}
}

func TestMemoryHistoryCannotRewriteEarlierAttempts(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	id := mustPush(t, q, "job")
	p, _ := q.Driver.Pop(ctx, "default")
	p.History = append(p.History, Attempt{Attempt: 1, StartedAt: *p.StartedAt, Status: Failed})
	if err := q.Driver.Release(ctx, p, 0); err != nil {
		t.Fatal(err)
	}
	p, _ = q.Driver.Pop(ctx, "default")
	p.History = []Attempt{{Attempt: 1, Status: Completed}, {Attempt: 2, StartedAt: *p.StartedAt, Status: Completed}}
	if err := q.Driver.Complete(ctx, p); err != nil {
		t.Fatal(err)
	}
	stored := getJob(t, q, id)
	if len(stored.History) != 2 || stored.History[0].Status != Failed {
		t.Fatal(stored.History)
	}
}

func TestSupervisorRespectsLimitWhileWorkersRetire(t *testing.T) {
	q := testQueue(t)
	var active, peak atomic.Int32
	gate := make(chan struct{})
	_ = q.RegisterFunc("blocked", func(ctx context.Context, _ map[string]any) (any, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, n) {
				break
			}
		}
		select {
		case <-gate:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	for range 6 {
		mustPush(t, q, "blocked")
	}
	c := DefaultSupervisorConfig()
	c.MinWorkers = 1
	c.MaxWorkers = 3
	c.Balance = "false"
	c.Sleep = .001
	s := NewSupervisor(q, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for !s.Stats()["running"].(bool) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	s.Scale(3)
	for active.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	s.Scale(1)
	s.Scale(3)
	time.Sleep(20 * time.Millisecond)
	close(gate)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown stuck")
	}
	if peak.Load() > 3 {
		t.Fatalf("max workers exceeded: %d", peak.Load())
	}
}

type callbackFailureDriver struct {
	Driver
	failures atomic.Int32
}

func (d *callbackFailureDriver) Push(ctx context.Context, p *JobPayload) (string, error) {
	if strings.HasSuffix(p.ID, ":finally") && d.failures.CompareAndSwap(0, 1) {
		return "", errors.New("temporary callback storage failure")
	}
	return d.Driver.Push(ctx, p)
}

func TestBatchCallbackRecoveryAfterTerminalCommit(t *testing.T) {
	ctx := context.Background()
	d := &callbackFailureDriver{Driver: NewMemoryDriver()}
	q := NewQueue(d)
	_ = q.RegisterFunc("job", func(context.Context, map[string]any) (any, error) { return nil, nil })
	b, err := q.Batch([]JobRequest{{Name: "job"}}).Then("then").Finally("finally").Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewWorker(q, WorkerOptions{}).RunOnce(ctx); err == nil {
		t.Fatal("expected injected callback error")
	}
	job := getJob(t, q, b.JobIDs[0])
	if job.Status != Completed {
		t.Fatal(job)
	}
	data, _ := d.GetBatch(ctx, b.BatchID)
	if valueNumber(data["completed_count"]) != 1 || data["status"] == Completed {
		t.Fatal(data)
	}
	if n, err := q.ReconcileBatches(ctx, 100); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := q.ReconcileBatches(ctx, 100); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	jobs, _ := d.GetJobs(ctx, JobFilter{Status: Pending})
	if len(jobs) != 2 {
		t.Fatal("callbacks duplicated", len(jobs))
	}
}

func TestManualRetryDoesNotFinishBatchEarly(t *testing.T) {
	ctx := context.Background()
	q := testQueue(t)
	calls := 0
	_ = q.RegisterFunc("a", func(context.Context, map[string]any) (any, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("failed")
		}
		return nil, nil
	})
	_ = q.RegisterFunc("b", func(context.Context, map[string]any) (any, error) { return nil, nil })
	b, err := q.Batch([]JobRequest{{Name: "a", Options: []JobOption{OnQueue("a"), WithMaxAttempts(1)}}, {Name: "b", Options: []JobOption{OnQueue("b")}}}).Finally("finally").Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(q, WorkerOptions{Queues: []string{"a"}})
	_, err = w.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.RetryFailed(ctx, JobFilter{Queue: "a"}); err != nil {
		t.Fatal(err)
	}
	_, err = w.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := q.Driver.GetBatch(ctx, b.BatchID)
	if valueNumber(data["failed_count"]) != 1 || valueNumber(data["completed_count"]) != 0 || data["status"] == Completed {
		t.Fatal("retry double counted", data)
	}
	if _, err = NewWorker(q, WorkerOptions{Queues: []string{"b"}}).RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	data, _ = q.Driver.GetBatch(ctx, b.BatchID)
	if valueNumber(data["completed_count"]) != 1 || data["status"] != Completed {
		t.Fatal(data)
	}
}

func TestRejectInvalidTimerDurations(t *testing.T) {
	for _, seconds := range []float64{math.NaN(), math.Inf(1), -1, 1e-15, 1e20} {
		config := DefaultSupervisorConfig()
		config.Sleep = seconds
		if config.Validate() == nil {
			t.Fatalf("accepted sleep %v", seconds)
		}
		config = DefaultSupervisorConfig()
		config.Timeout = seconds
		if config.Validate() == nil {
			t.Fatalf("accepted timeout %v", seconds)
		}
		if _, err := NewScheduler(testQueue(t), []ScheduleEntry{{JobClass: "job", Every: seconds}}); err == nil {
			t.Fatalf("accepted every %v", seconds)
		}
	}
}

func TestDriverOptionsAreAppliedOrRejected(t *testing.T) {
	d, err := NewRedisDriver("redis://127.0.0.1:16389/0", "options-test")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Disconnect(context.Background())
	if err = configureDriverOptions(d, map[string]any{"pool_size": 7, "socket_timeout": 2.5, "db": 2}); err != nil {
		t.Fatal(err)
	}
	if d.options.PoolSize != 7 || d.options.ReadTimeout != 2500*time.Millisecond || d.options.DB != 2 || d.options.MaxRetries != -1 || d.client().Options().MaxRetries != 0 {
		t.Fatal("Redis options not applied")
	}
	if err = d.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = d.Connect(cancelled)
	if d.client().Options().MaxRetries != 0 {
		t.Fatal("reconnect enabled transaction retries")
	}
	sql := NewPostgresDriver("postgres://localhost/test")
	if err = configureDriverOptions(sql, map[string]any{"max_size": 5, "min_size": 2, "command_timeout": 3}); err != nil {
		t.Fatal(err)
	}
	if sql.MaxOpenConns != 5 || sql.MaxIdleConns != 2 || !strings.Contains(sql.dsn, "statement_timeout=3000") {
		t.Fatal("SQL options not applied")
	}
	if err = configureDriverOptions(d, map[string]any{"max_retries": 5}); err == nil {
		t.Fatal("unsafe option ignored")
	}
	if err = configureDriverOptions(NewMemoryDriver(), map[string]any{"unknown": true}); err == nil {
		t.Fatal("unknown option ignored")
	}
}
