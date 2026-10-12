package baqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func redisTestDriver(t *testing.T) (*RedisDriver, context.Context) {
	t.Helper()
	url := os.Getenv("BAQUEUE_REDIS_URL")
	if url == "" {
		t.Skip("BAQUEUE_REDIS_URL is not set; real Redis integration test")
	}
	d, err := NewRedisDriver(url, "baqueue-go-test:"+NewID())
	redisOK(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	redisOK(t, d.Connect(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// Remove only this test's namespace, including intentionally corrupt keys.
		keys, err := d.scanKeys(ctx, "", 500)
		if err == nil && len(keys) > 0 {
			err = d.client().Del(ctx, keys...).Err()
		}
		if err != nil {
			t.Errorf("Redis cleanup: %v", err)
		}
		_ = d.Disconnect(ctx)
	})
	return d, ctx
}

func redisStored(t *testing.T, d *RedisDriver, ctx context.Context, id string) *JobPayload {
	t.Helper()
	job, err := d.GetJob(ctx, id)
	redisOK(t, err)
	if job == nil {
		t.Fatalf("job %s missing", id)
	}
	return job
}

func redisSeedPayload(t *testing.T, d *RedisDriver, ctx context.Context, job *JobPayload) {
	t.Helper()
	raw, err := json.Marshal(job)
	redisOK(t, err)
	redisOK(t, d.client().HSet(ctx, d.key("job", job.ID), "data", string(raw)).Err())
}

func redisAssertIndexes(t *testing.T, d *RedisDriver, ctx context.Context, job *JobPayload) {
	t.Helper()
	for _, key := range []string{d.index("", ""), d.index(job.Queue, ""), d.index("", job.Status), d.index(job.Queue, job.Status)} {
		score, err := d.client().ZScore(ctx, key, job.ID).Result()
		redisOK(t, err)
		if score != job.CreatedAt {
			t.Fatalf("index %s score = %v, want %v", key, score, job.CreatedAt)
		}
	}
	for _, status := range redisStatuses {
		if status == job.Status {
			continue
		}
		for _, key := range []string{d.index("", status), d.index(job.Queue, status)} {
			if _, err := d.client().ZScore(ctx, key, job.ID).Result(); !errors.Is(err, redis.Nil) {
				t.Fatalf("stale status index %s: %v", key, err)
			}
		}
	}
}

func TestRedisConstructor(t *testing.T) {
	d, err := NewRedisDriver("", "")
	redisOK(t, err)
	defer d.Disconnect(context.Background())
	if !d.AutoCleanupOnDiskFull || d.ReconcileOnConnect || d.prefix != "baqueue" || d.client().Options().MaxRetries != 0 {
		t.Fatalf("unexpected defaults: %+v", d.options)
	}
	if _, err := NewRedisDriver("https://localhost", "x"); err == nil {
		t.Fatal("invalid scheme accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Connect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled connect: %v", err)
	}
}

func TestRedisLifecycleFencingAndPolicy(t *testing.T) {
	d, ctx := redisTestDriver(t)
	p := NewPayload("example.Job", map[string]any{"value": "python-compatible"})
	p.MaxAttempts, p.Backoff = 7, Backoff{Delays: []float64{1, 3, 9}}
	p.Tags = []string{"red", "audit"}
	p.History = []Attempt{{Attempt: 0, Status: "imported"}}
	_, err := d.Push(ctx, p)
	redisOK(t, err)
	if _, err = d.Push(ctx, p); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate push: %v", err)
	}
	claimed, err := d.Pop(ctx, p.Queue)
	redisOK(t, err)
	if claimed == nil || claimed.Attempts != 1 || claimed.StartedAt == nil {
		t.Fatalf("claim: %+v", claimed)
	}
	stale := claimed.Clone()
	claimed.MaxAttempts, claimed.Backoff = 99, Backoff{Strategy: "fixed"}
	claimed.Queue = "forged"
	claimed.Tags = []string{"forged"}
	claimed.History = []Attempt{{Attempt: 0, Status: "forged"}, {Attempt: 1, StartedAt: *claimed.StartedAt, Status: Failed, WillRetry: true}, {Attempt: 1, StartedAt: -1, Status: "forged"}}
	redisOK(t, d.Release(ctx, claimed, 0))
	stored := redisStored(t, d, ctx, p.ID)
	if claimed.Status != Pending || !reflect.DeepEqual(claimed, stored) {
		t.Fatalf("release did not update incoming payload: %+v", claimed)
	}
	if stored.MaxAttempts != 7 || !reflect.DeepEqual(stored.Backoff, p.Backoff) || stored.Queue != p.Queue || !reflect.DeepEqual(stored.Tags, p.Tags) {
		t.Fatalf("stored policy overwritten: %+v", stored)
	}
	if len(stored.History) != 2 || stored.History[0].Status != "imported" || !stored.History[1].WillRetry {
		t.Fatalf("history lost or rewritten: %+v", stored.History)
	}
	for name, action := range map[string]func() error{
		"complete": func() error { return d.Complete(ctx, stale) },
		"fail":     func() error { return d.Fail(ctx, stale, "stale") },
		"release":  func() error { return d.Release(ctx, stale, 60) },
	} {
		if err := action(); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale %s accepted: %v", name, err)
		}
	}
	second, err := d.Pop(ctx, p.Queue)
	redisOK(t, err)
	if second == nil || second.Attempts != 2 {
		t.Fatalf("retry claim: %+v", second)
	}
	second.History = append(second.History, Attempt{Attempt: 2, StartedAt: *second.StartedAt, Status: Failed})
	redisOK(t, d.Fail(ctx, second, "failure"))
	failed := redisStored(t, d, ctx, p.ID)
	if !reflect.DeepEqual(second, failed) {
		t.Fatal("fail did not update incoming payload")
	}
	if failed.Status != Failed || failed.Error == nil || *failed.Error != "failure" || failed.FailedAt == nil {
		t.Fatalf("failed payload: %+v", failed)
	}
	redisOK(t, d.Release(ctx, failed, 0))
	third, err := d.Pop(ctx, p.Queue)
	redisOK(t, err)
	third.History = append(third.History, Attempt{Attempt: 3, StartedAt: *third.StartedAt, Status: Completed})
	redisOK(t, d.Complete(ctx, third))
	completed := redisStored(t, d, ctx, p.ID)
	if !reflect.DeepEqual(third, completed) {
		t.Fatal("complete did not update incoming payload")
	}
	if completed.Status != Completed || completed.CompletedAt == nil || len(completed.History) != 4 {
		t.Fatalf("completed payload: %+v", completed)
	}
	redisAssertIndexes(t, d, ctx, completed)
	redisOK(t, d.Delete(ctx, p.ID))
	if err := d.Complete(ctx, third); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted job resurrected: %v", err)
	}
}

func TestRedisRequeueFencesAllOldWorkerTransitions(t *testing.T) {
	d, ctx := redisTestDriver(t)
	p := NewPayload("example.Job", nil)
	_, err := d.Push(ctx, p)
	redisOK(t, err)
	first, err := d.Pop(ctx, p.Queue)
	redisOK(t, err)
	n, err := d.RequeueStuckJobs(ctx, 0, p.Queue)
	redisOK(t, err)
	if n != 1 {
		t.Fatalf("recovered %d, want 1", n)
	}
	for _, action := range []func() error{
		func() error { return d.Complete(ctx, first) },
		func() error { return d.Fail(ctx, first, "late") },
		func() error { return d.Release(ctx, first, 100) },
	} {
		if err := action(); !errors.Is(err, ErrConflict) {
			t.Fatalf("old worker accepted after recovery: %v", err)
		}
	}
	second, err := d.Pop(ctx, p.Queue)
	redisOK(t, err)
	if second == nil || second.Attempts != first.Attempts+1 {
		t.Fatalf("recovery reset attempts: %+v", second)
	}
	if err := d.Complete(ctx, first); !errors.Is(err, ErrConflict) {
		t.Fatalf("old worker completed new claim: %v", err)
	}
	redisOK(t, d.Complete(ctx, second))
	redisAssertIndexes(t, d, ctx, redisStored(t, d, ctx, p.ID))
}

func TestRedisConcurrentClaims(t *testing.T) {
	d, ctx := redisTestDriver(t)
	jobs := make([]*JobPayload, 100)
	for i := range jobs {
		jobs[i] = NewPayload("example.Job", map[string]any{"n": i})
	}
	_, err := d.PushMany(ctx, jobs)
	redisOK(t, err)
	// Separate connection pools exercise cross-process serialization semantics.
	peer, err := NewRedisDriver(os.Getenv("BAQUEUE_REDIS_URL"), d.prefix)
	redisOK(t, err)
	redisOK(t, peer.Connect(ctx))
	defer peer.Disconnect(ctx)
	var wg sync.WaitGroup
	claims := make(chan string, len(jobs))
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(driver *RedisDriver) {
			defer wg.Done()
			for {
				job, err := driver.Pop(ctx, "default")
				if err != nil {
					errs <- err
					return
				}
				if job == nil {
					return
				}
				claims <- job.ID
				if job.Attempts != 1 {
					errs <- fmt.Errorf("attempts = %d", job.Attempts)
					return
				}
				if err = driver.Complete(ctx, job); err != nil {
					errs <- err
					return
				}
			}
		}([]*RedisDriver{d, peer}[i%2])
	}
	wg.Wait()
	close(errs)
	close(claims)
	for err := range errs {
		redisOK(t, err)
	}
	seen := map[string]bool{}
	for id := range claims {
		if seen[id] {
			t.Fatalf("duplicate claim %s", id)
		}
		seen[id] = true
	}
	if len(seen) != len(jobs) {
		t.Fatalf("claimed %d of %d", len(seen), len(jobs))
	}
	n, err := d.CountJobs(ctx, JobFilter{Status: Completed})
	redisOK(t, err)
	if n != len(jobs) {
		t.Fatalf("completed count %d", n)
	}
}

func TestRedisDelayedRepairAndPromotion(t *testing.T) {
	d, ctx := redisTestDriver(t)
	future := Now() + 3600
	delayed := NewPayload("example.Delayed", nil)
	delayed.DelayUntil = &future
	ready := NewPayload("example.Ready", nil)
	_, err := d.PushMany(ctx, []*JobPayload{delayed, ready})
	redisOK(t, err)
	redisOK(t, d.client().LPush(ctx, d.key("queue", "default"), delayed.ID).Err())
	redisOK(t, d.client().ZRem(ctx, d.key("delayed"), delayed.ID).Err())
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	if claimed == nil || claimed.ID != ready.ID {
		t.Fatalf("future job claimed: %+v", claimed)
	}
	score, err := d.client().ZScore(ctx, d.key("delayed"), delayed.ID).Result()
	redisOK(t, err)
	if score != future {
		t.Fatalf("future delay not restored: %v", score)
	}
	redisOK(t, d.client().ZAdd(ctx, d.key("delayed"), redis.Z{Score: 0, Member: delayed.ID}).Err())
	moved, err := d.PopDelayed(ctx)
	redisOK(t, err)
	if len(moved) != 0 {
		t.Fatal("stale delayed score caused early promotion")
	}
	past := Now() - 1
	stored := redisStored(t, d, ctx, delayed.ID)
	stored.DelayUntil = &past
	redisSeedPayload(t, d, ctx, stored)
	redisOK(t, d.client().ZAdd(ctx, d.key("delayed"), redis.Z{Score: past, Member: delayed.ID}).Err())
	var wg sync.WaitGroup
	var total atomic.Int32
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := d.PopDelayed(ctx)
			errs <- err
			total.Add(int32(len(jobs)))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		redisOK(t, err)
	}
	if total.Load() != 1 {
		t.Fatalf("promotion committed %d times", total.Load())
	}
	redisOK(t, d.client().RPush(ctx, d.key("queue", "default"), delayed.ID, delayed.ID).Err())
	claimed, err = d.Pop(ctx, "default")
	redisOK(t, err)
	if claimed == nil || claimed.ID != delayed.ID || claimed.DelayUntil != nil || claimed.Attempts != 1 {
		t.Fatalf("delayed claim: %+v", claimed)
	}
	if extra, err := d.Pop(ctx, "default"); err != nil || extra != nil {
		t.Fatalf("duplicate entry claimed: %+v %v", extra, err)
	}
}

func TestRedisPromoteKeepsFIFOAndRestoresMissingReady(t *testing.T) {
	d, ctx := redisTestDriver(t)
	a, b := NewPayload("example.A", nil), NewPayload("example.B", nil)
	_, err := d.PushMany(ctx, []*JobPayload{a, b})
	redisOK(t, err)
	for i := 0; i < 2; i++ {
		ok, err := d.Promote(ctx, a.ID)
		redisOK(t, err)
		if !ok {
			t.Fatal("ready promotion rejected")
		}
	}
	ids, err := d.client().LRange(ctx, d.key("queue", "default"), 0, -1).Result()
	redisOK(t, err)
	if !reflect.DeepEqual(ids, []string{a.ID, b.ID}) {
		t.Fatalf("promotion reordered FIFO: %v", ids)
	}
	redisOK(t, d.client().LRem(ctx, d.key("queue", "default"), 0, a.ID).Err())
	ok, err := d.Promote(ctx, a.ID)
	redisOK(t, err)
	if !ok {
		t.Fatal("orphan ready job not restored")
	}
	n, err := d.Size(ctx, "default")
	redisOK(t, err)
	if n != 2 {
		t.Fatalf("ready size %d", n)
	}
}

func TestRedisBatchAtomicCountersAndPatch(t *testing.T) {
	d, ctx := redisTestDriver(t)
	redisOK(t, d.StoreBatch(ctx, "batch", map[string]any{"processed": 0, "name": "original", "items": []any{1, "two"}}))
	const workers, increments = 8, 20
	var wg sync.WaitGroup
	errs := make(chan error, workers+1)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				if _, err := d.IncrementBatchCounter(ctx, "batch", "processed", 1); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- d.UpdateBatch(ctx, "batch", map[string]any{"name": "updated"})
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		redisOK(t, err)
	}
	data, err := d.GetBatch(ctx, "batch")
	redisOK(t, err)
	if valueNumber(data["processed"]) != workers*increments || data["name"] != "updated" || len(data["items"].([]any)) != 2 {
		t.Fatalf("lost batch updates: %#v", data)
	}
	if data, err = d.IncrementBatchCounter(ctx, "missing", "n", 1); err != nil || data != nil {
		t.Fatalf("missing batch: %#v %v", data, err)
	}
	if err = d.UpdateBatch(ctx, "missing", map[string]any{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing batch update: %v", err)
	}
}

type redisTestHook struct {
	pipeline func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error
}

func (h redisTestHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h redisTestHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h redisTestHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error { return h.pipeline(ctx, commands, next) }
}

func redisTestExecWrites(commands []redis.Cmder, key string) bool {
	if len(commands) == 0 || commands[0].Name() != "multi" {
		return false
	}
	for _, command := range commands {
		args := command.Args()
		if command.Name() == "hset" && len(args) > 1 && fmt.Sprint(args[1]) == key {
			return true
		}
	}
	return false
}

type redisTestServerError string

func (e redisTestServerError) Error() string { return string(e) }
func (e redisTestServerError) RedisError()   {}

func TestRedisClaimFailureBeforeAndAfterEXEC(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after=%t", after), func(t *testing.T) {
			d, ctx := redisTestDriver(t)
			job := NewPayload("example.Job", nil)
			_, err := d.Push(ctx, job)
			redisOK(t, err)
			var calls atomic.Int32
			injected := errors.New("injected lost connection")
			d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				if redisTestExecWrites(cmds, d.key("job", job.ID)) && calls.Add(1) == 1 {
					if after {
						if err := next(ctx, cmds); err != nil {
							return err
						}
					} else {
						for _, cmd := range cmds {
							cmd.SetErr(injected)
						}
					}
					return injected
				}
				return next(ctx, cmds)
			}})
			if claimed, err := d.Pop(ctx, job.Queue); !errors.Is(err, injected) || claimed != nil {
				t.Fatalf("claim failure hidden: %+v %v", claimed, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("ambiguous EXEC retried %d times", calls.Load())
			}
			stored := redisStored(t, d, ctx, job.ID)
			if after {
				if stored.Status != Processing || stored.Attempts != 1 {
					t.Fatalf("committed claim missing: %+v", stored)
				}
				n, err := d.RequeueStuckJobs(ctx, 0, job.Queue)
				redisOK(t, err)
				if n != 1 {
					t.Fatal("ambiguous claim not recoverable")
				}
			} else if stored.Status != Pending || stored.Attempts != 0 {
				t.Fatalf("failed EXEC mutated job: %+v", stored)
			}
			n, err := d.Size(ctx, job.Queue)
			redisOK(t, err)
			if n != 1 {
				t.Fatalf("ready reference lost: %d", n)
			}
			claimed, err := d.Pop(ctx, job.Queue)
			redisOK(t, err)
			if claimed == nil || claimed.ID != job.ID {
				t.Fatal("job not claimable after failure")
			}
		})
	}
}

func TestRedisWrongTypeDoesNotPartiallyRemoveClaim(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	redisOK(t, d.client().Set(ctx, d.index("", Processing), "corrupt", 0).Err())
	if _, err = d.Pop(ctx, job.Queue); err == nil {
		t.Fatal("wrong-type index accepted")
	}
	if stored := redisStored(t, d, ctx, job.ID); stored.Status != Pending || stored.Attempts != 0 {
		t.Fatalf("partial claim persisted: %+v", stored)
	}
	n, err := d.Size(ctx, job.Queue)
	redisOK(t, err)
	if n != 1 {
		t.Fatalf("ready entry removed before successful claim: %d", n)
	}
}

func TestRedisStorageFullCleanupAndSingleRetry(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprintf("permanent=%t", permanent), func(t *testing.T) {
			d, ctx := redisTestDriver(t)
			terminal, survivor := NewPayload("example.Done", nil), NewPayload("example.Pending", nil)
			_, err := d.PushMany(ctx, []*JobPayload{terminal, survivor})
			redisOK(t, err)
			claimed, err := d.Pop(ctx, "default")
			redisOK(t, err)
			redisOK(t, d.Complete(ctx, claimed))
			redisOK(t, d.RecordMetric(ctx, "default", Completed, 1))
			target := NewPayload("example.New", nil)
			var calls atomic.Int32
			oom := redisTestServerError("OOM command not allowed when used memory > maxmemory")
			d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				if redisTestExecWrites(cmds, d.key("job", target.ID)) {
					call := calls.Add(1)
					if call == 1 || permanent {
						for _, cmd := range cmds {
							cmd.SetErr(oom)
						}
						return oom
					}
				}
				return next(ctx, cmds)
			}})
			_, err = d.Push(ctx, target)
			if permanent && !d.IsStorageFullError(err) {
				t.Fatalf("permanent OOM not returned: %v", err)
			}
			if !permanent {
				redisOK(t, err)
			}
			if calls.Load() != 2 || d.cleaning.Load() {
				t.Fatalf("retry/cleanup guard: calls=%d active=%t", calls.Load(), d.cleaning.Load())
			}
			if job, err := d.GetJob(ctx, terminal.ID); err != nil || job != nil {
				t.Fatalf("terminal job not cleaned: %+v %v", job, err)
			}
			if job := redisStored(t, d, ctx, survivor.ID); job.Status != Pending {
				t.Fatalf("cleanup damaged pending job: %+v", job)
			}
			n, err := d.client().LLen(ctx, d.key("metrics", "default")).Result()
			redisOK(t, err)
			if n != 0 {
				t.Fatalf("old metrics survived emergency cleanup: %d", n)
			}
		})
	}
}

func TestRedisPartialEXECDoesNotRetryCounter(t *testing.T) {
	d, ctx := redisTestDriver(t)
	redisOK(t, d.StoreBatch(ctx, "batch", map[string]any{"processed": 0}))
	var calls atomic.Int32
	oom := redisTestServerError("OOM command not allowed when used memory > maxmemory")
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if redisTestExecWrites(cmds, d.key("batch", "batch")) {
			calls.Add(1)
			if err := next(ctx, cmds); err != nil {
				return err
			}
			return oom
		}
		return next(ctx, cmds)
	}})
	data, err := d.IncrementBatchCounter(ctx, "batch", "processed", 1)
	var unsafe *redisUnsafeExecError
	if !errors.As(err, &unsafe) || data != nil || d.IsStorageFullError(err) {
		t.Fatalf("partial EXEC deemed retryable: %#v %v", data, err)
	}
	stored, err := d.GetBatch(ctx, "batch")
	redisOK(t, err)
	if calls.Load() != 1 || valueNumber(stored["processed"]) != 1 {
		t.Fatalf("counter replayed: calls=%d data=%#v", calls.Load(), stored)
	}
}

func TestRedisPendingRecoveryAndIndexReconciliation(t *testing.T) {
	d, ctx := redisTestDriver(t)
	ready, delayed, processing := NewPayload("example.Ready", nil), NewPayload("example.Delayed", nil), NewPayload("example.Processing", nil)
	future := Now() + 3600
	delayed.DelayUntil = &future
	_, err := d.PushMany(ctx, []*JobPayload{processing, ready, delayed})
	redisOK(t, err)
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	if claimed == nil || claimed.ID != processing.ID {
		t.Fatal("unexpected initial claim")
	}
	redisOK(t, d.client().LRem(ctx, d.key("queue", "default"), 0, ready.ID).Err())
	redisOK(t, d.client().ZRem(ctx, d.key("delayed"), delayed.ID).Err())
	redisOK(t, d.client().RPush(ctx, d.key("queue", "wrong"), ready.ID, ready.ID, processing.ID, "missing", "malformed").Err())
	redisOK(t, d.client().HSet(ctx, d.key("job", "malformed"), "data", "{invalid").Err())
	redisOK(t, d.client().ZAdd(ctx, d.index("", Pending), redis.Z{Score: 1, Member: processing.ID}, redis.Z{Score: 1, Member: "missing"}, redis.Z{Score: 1, Member: "malformed"}).Err())
	// This Python-shaped hash has no index or ready membership at all.
	raw := `{"id":"python","job_class":"examples.Job","data":{"nested":[1,true]},"queue":"python-q","status":"pending","attempts":2,"max_attempts":8,"backoff":[1,2,4],"timeout":60,"created_at":1700000000,"updated_at":1700000000,"delay_until":null,"tags":["python"],"history":[]}`
	redisOK(t, d.client().HSet(ctx, d.key("job", "python"), "data", raw).Err())
	_, err = d.ReconcilePendingJobs(ctx, 1)
	redisOK(t, err)
	_, err = d.ReconcilePendingJobs(ctx, 1)
	redisOK(t, err)
	ids, err := d.client().LRange(ctx, d.key("queue", "default"), 0, -1).Result()
	redisOK(t, err)
	if !reflect.DeepEqual(ids, []string{ready.ID}) {
		t.Fatalf("pending recovery duplicates or processing resurrection: %v", ids)
	}
	wrong, err := d.client().LLen(ctx, d.key("queue", "wrong")).Result()
	redisOK(t, err)
	if wrong != 0 {
		t.Fatalf("wrong queue references remain: %d", wrong)
	}
	if exists := d.client().Exists(ctx, d.key("job", "malformed")).Val(); exists != 1 {
		t.Fatal("malformed hash should remain for inspection")
	}
	for _, job := range []*JobPayload{ready, delayed, claimed, redisStored(t, d, ctx, "python")} {
		redisAssertIndexes(t, d, ctx, job)
	}
	python, err := d.Pop(ctx, "python-q")
	redisOK(t, err)
	if python == nil || python.Attempts != 3 || python.MaxAttempts != 8 || !reflect.DeepEqual(python.Backoff.Delays, []float64{1, 2, 4}) {
		t.Fatalf("Python payload compatibility: %+v", python)
	}
	redisOK(t, d.client().ZAdd(ctx, d.index("abandoned", Failed), redis.Z{Score: 1, Member: "orphan"}, redis.Z{Score: 1, Member: ready.ID}).Err())
	redisOK(t, d.client().ZAdd(ctx, d.index("", ""), redis.Z{Score: -1, Member: ready.ID}).Err())
	redisOK(t, d.client().Del(ctx, d.index("default", Pending)).Err())
	removed, err := d.ReconcileIndexes(ctx, 1)
	redisOK(t, err)
	if removed != 2 {
		t.Fatalf("removed stale memberships %d, want 2", removed)
	}
	redisAssertIndexes(t, d, ctx, ready)
	redisAssertIndexes(t, d, ctx, delayed)
	n, err := d.client().ZCard(ctx, d.index("abandoned", Failed)).Result()
	redisOK(t, err)
	if n != 0 {
		t.Fatalf("unknown-queue drift remains: %d", n)
	}
}

func TestRedisReconcileCannotResurrectConcurrentClaim(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	peer, err := NewRedisDriver(os.Getenv("BAQUEUE_REDIS_URL"), d.prefix)
	redisOK(t, err)
	defer peer.Disconnect(ctx)
	var fired atomic.Bool
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if redisTestExecWrites(cmds, d.key("job", job.ID)) && fired.CompareAndSwap(false, true) {
			claimed, err := peer.Pop(ctx, job.Queue)
			if err != nil || claimed == nil {
				return fmt.Errorf("concurrent claim: %+v %v", claimed, err)
			}
		}
		return next(ctx, cmds)
	}})
	_, err = d.ReconcilePendingJobs(ctx, 1)
	redisOK(t, err)
	stored := redisStored(t, d, ctx, job.ID)
	if !fired.Load() || stored.Status != Processing || stored.Attempts != 1 {
		t.Fatalf("concurrent claim overwritten: %+v", stored)
	}
	n, err := d.Size(ctx, job.Queue)
	redisOK(t, err)
	if n != 0 {
		t.Fatalf("processing job requeued: %d", n)
	}
	redisAssertIndexes(t, d, ctx, stored)
}

func TestRedisQueryPruneAndFlush(t *testing.T) {
	d, ctx := redisTestDriver(t)
	batch := "batch"
	jobs := make([]*JobPayload, 300)
	for i := range jobs {
		job := NewPayload("example.Job", nil)
		job.CreatedAt = float64(i)
		if i < 3 {
			job.Tags, job.BatchID = []string{"rare"}, &batch
		}
		jobs[i] = job
	}
	_, err := d.PushMany(ctx, jobs)
	redisOK(t, err)
	from, to := float64(0), float64(2)
	f := JobFilter{Tag: "rare", BatchID: batch, Offset: 1, Limit: 1, CreatedFrom: &from, CreatedTo: &to}
	results, err := d.GetJobs(ctx, f)
	redisOK(t, err)
	if len(results) != 1 || results[0].ID != jobs[1].ID {
		t.Fatalf("filtered pagination: %+v", results)
	}
	results, err = d.GetJobs(ctx, JobFilter{Tag: "rare", Limit: -1})
	redisOK(t, err)
	if len(results) != 3 {
		t.Fatalf("sparse filter truncated after first page: %d", len(results))
	}
	n, err := d.CountJobs(ctx, f)
	redisOK(t, err)
	if n != 3 {
		t.Fatalf("count honored pagination: %d", n)
	}
	for i := 0; i < 3; i++ {
		job, err := d.Pop(ctx, "default")
		redisOK(t, err)
		redisOK(t, d.Complete(ctx, job))
	}
	metrics, err := d.GetMetrics(ctx, "default")
	redisOK(t, err)
	counts := metrics["default"].(map[string]int)
	if counts[Pending] != 297 || counts[Completed] != 3 {
		t.Fatalf("live metrics: %#v", metrics)
	}
	age := float64(3600)
	n, err = d.PruneTerminalJobs(ctx, PruneOptions{OlderThanSeconds: &age})
	redisOK(t, err)
	if n != 0 {
		t.Fatalf("pruned recent terminal jobs: %d", n)
	}
	n, err = d.PruneTerminalJobs(ctx, PruneOptions{Limit: 2})
	redisOK(t, err)
	if n != 2 {
		t.Fatalf("Go prune limit is per-call: %d", n)
	}
	n, err = d.PruneTerminalJobs(ctx, PruneOptions{})
	redisOK(t, err)
	if n != 1 {
		t.Fatalf("terminal-only prune deleted pending: %d", n)
	}
	other := NewPayload("example.Other", nil)
	other.Queue = "other"
	_, err = d.Push(ctx, other)
	redisOK(t, err)
	redisOK(t, d.Flush(ctx, "default"))
	if job := redisStored(t, d, ctx, other.ID); job.Queue != "other" {
		t.Fatal("queue flush deleted other queue")
	}
	queues, err := d.Queues(ctx)
	redisOK(t, err)
	if !reflect.DeepEqual(queues, []string{"other"}) {
		t.Fatalf("queues after flush: %v", queues)
	}
	redisOK(t, d.StoreBatch(ctx, "batch", map[string]any{"n": 1}))
	redisOK(t, d.ReportSupervisor(ctx, map[string]any{"name": "supervisor", "running": true}))
	redisOK(t, d.RecordMetric(ctx, "other", Completed, 1))
	redisOK(t, d.Flush(ctx, ""))
	keys, err := d.scanKeys(ctx, "", 500)
	redisOK(t, err)
	if len(keys) != 0 {
		t.Fatalf("namespace not flushed: %v", keys)
	}
}

func TestRedisPruneFencesConcurrentRetry(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	redisOK(t, d.Fail(ctx, claimed, "retry me"))
	failed := redisStored(t, d, ctx, job.ID)
	peer, err := NewRedisDriver(os.Getenv("BAQUEUE_REDIS_URL"), d.prefix)
	redisOK(t, err)
	defer peer.Disconnect(ctx)
	var fired atomic.Bool
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if len(cmds) > 0 && cmds[0].Name() == "multi" {
			for _, command := range cmds {
				if command.Name() == "del" && fmt.Sprint(command.Args()[1]) == d.key("job", job.ID) && fired.CompareAndSwap(false, true) {
					if err := peer.Release(ctx, failed, 0); err != nil {
						return err
					}
				}
			}
		}
		return next(ctx, cmds)
	}})
	n, err := d.PruneTerminalJobs(ctx, PruneOptions{Status: Failed})
	redisOK(t, err)
	if !fired.Load() || n != 0 || redisStored(t, d, ctx, job.ID).Status != Pending {
		t.Fatalf("concurrent retry deleted: removed=%d", n)
	}
}

func TestRedisMetricsSupervisorAndBackfill(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	redisSeedPayload(t, d, ctx, job)
	redisOK(t, d.Connect(ctx))
	redisAssertIndexes(t, d, ctx, job)
	redisOK(t, d.client().ZAdd(ctx, d.index("old", Completed), redis.Z{Score: 1, Member: "orphan"}).Err())
	d.ReconcileOnConnect = true
	redisOK(t, d.Connect(ctx))
	if n := d.client().ZCard(ctx, d.index("old", Completed)).Val(); n != 0 {
		t.Fatal("reconcile_on_connect ignored")
	}
	for _, name := range []string{Processing, Completed, Failed} {
		redisOK(t, d.RecordMetric(ctx, "default", name, 99))
	}
	old := `{"metric":"completed","value":1,"time":1}`
	redisOK(t, d.client().RPush(ctx, d.key("metrics", "default"), old, "not-json").Err())
	throughput, err := d.RecentThroughput(ctx, 60, "default")
	redisOK(t, err)
	if !reflect.DeepEqual(throughput, map[string]int{Processing: 1, Completed: 1, Failed: 1}) {
		t.Fatalf("throughput must count events, not values: %#v", throughput)
	}
	n, err := d.PruneMetrics(ctx, 60)
	redisOK(t, err)
	if n != 2 {
		t.Fatalf("pruned metric records %d", n)
	}
	for _, name := range []string{"live", "stale", "stopped"} {
		redisOK(t, d.ReportSupervisor(ctx, map[string]any{"name": name, "running": name != "stopped", "workers": 2}))
	}
	redisOK(t, d.client().HSet(ctx, d.key("supervisor", "stale"), "heartbeat_at", 1).Err())
	stats, err := d.GetSupervisorStats(ctx, 60)
	redisOK(t, err)
	if len(stats) != 1 || stats[0]["name"] != "live" {
		t.Fatalf("supervisor freshness: %#v", stats)
	}
	if err := d.ReportSupervisor(ctx, map[string]any{"running": true}); err == nil {
		t.Fatal("nameless supervisor accepted")
	}
	redisOK(t, d.Disconnect(ctx))
	redisOK(t, d.Disconnect(ctx))
	redisOK(t, d.Connect(ctx))
	if d.client().Options().MaxRetries != 0 {
		t.Fatal("reconnect enabled unsafe automatic retries")
	}
	redisStored(t, d, ctx, job.ID)
}

func TestRedisRejectsInvalidDelayWithoutMutation(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	before := claimed.Clone()
	for _, delay := range []float64{-1, math.NaN(), math.Inf(1)} {
		if err := d.Release(ctx, claimed, delay); err == nil {
			t.Fatalf("invalid delay accepted: %v", delay)
		}
		if !reflect.DeepEqual(before, claimed) || !reflect.DeepEqual(before, redisStored(t, d, ctx, claimed.ID)) {
			t.Fatal("failed release changed payload")
		}
	}
}

func TestRedisLifecycleFailureDoesNotMutateCaller(t *testing.T) {
	for _, status := range []string{Completed, Failed, Pending} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", status, after), func(t *testing.T) {
				d, ctx := redisTestDriver(t)
				job := NewPayload("example.Job", nil)
				_, err := d.Push(ctx, job)
				redisOK(t, err)
				claimed, err := d.Pop(ctx, "default")
				redisOK(t, err)
				before := claimed.Clone()
				injected := errors.New("injected lifecycle disconnect")
				var calls atomic.Int32
				d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
					if redisTestExecWrites(cmds, d.key("job", job.ID)) {
						calls.Add(1)
						if after {
							if err := next(ctx, cmds); err != nil {
								return err
							}
						} else {
							for _, cmd := range cmds {
								cmd.SetErr(injected)
							}
						}
						return injected
					}
					return next(ctx, cmds)
				}})
				switch status {
				case Completed:
					err = d.Complete(ctx, claimed)
				case Failed:
					err = d.Fail(ctx, claimed, "failure")
				case Pending:
					err = d.Release(ctx, claimed, 60)
				}
				if !errors.Is(err, injected) || calls.Load() != 1 || !reflect.DeepEqual(before, claimed) {
					t.Fatalf("failed lifecycle mutated caller or replayed: %+v calls=%d err=%v", claimed, calls.Load(), err)
				}
				stored := redisStored(t, d, ctx, job.ID)
				want := Processing
				if after {
					want = status
				}
				if stored.Status != want {
					t.Fatalf("stored status %s, want %s", stored.Status, want)
				}
			})
		}
	}
}

func TestRedisCleanupFailureDoesNotRecurse(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Done", nil)
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	redisOK(t, d.Complete(ctx, claimed))
	var calls atomic.Int32
	oom := redisTestServerError("OOM command not allowed when used memory > maxmemory")
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if len(cmds) > 0 && cmds[0].Name() == "multi" {
			calls.Add(1)
			for _, cmd := range cmds {
				cmd.SetErr(oom)
			}
			return oom
		}
		return next(ctx, cmds)
	}})
	_, err = d.Push(ctx, NewPayload("example.New", nil))
	if !d.IsStorageFullError(err) || calls.Load() != 3 || d.cleaning.Load() {
		t.Fatalf("recursive cleanup or excessive retries: calls=%d guard=%t err=%v", calls.Load(), d.cleaning.Load(), err)
	}
	if job := redisStored(t, d, ctx, job.ID); job.Status != Completed {
		t.Fatalf("failed cleanup partially deleted job: %+v", job)
	}
}

func TestRedisPrefixGlobIsolation(t *testing.T) {
	d, ctx := redisTestDriver(t)
	literal, err := NewRedisDriver(os.Getenv("BAQUEUE_REDIS_URL"), d.prefix+"[*]?")
	redisOK(t, err)
	defer literal.Disconnect(ctx)
	redisOK(t, literal.StoreBatch(ctx, "literal", map[string]any{"keep": true}))
	redisOK(t, d.StoreBatch(ctx, "other", map[string]any{"keep": true}))
	keys, err := literal.scanKeys(ctx, "", 1)
	redisOK(t, err)
	if len(keys) != 1 || !strings.HasPrefix(keys[0], literal.prefix+":") || slices.Contains(keys, d.key("batch", "other")) {
		t.Fatalf("prefix interpreted as glob: %v", keys)
	}
	redisOK(t, literal.Flush(ctx, ""))
	data, err := d.GetBatch(ctx, "other")
	redisOK(t, err)
	if data == nil {
		t.Fatal("literal-prefix flush affected another namespace")
	}
}

func TestRedisPopQuarantinesMalformedHeadAtomically(t *testing.T) {
	d, ctx := redisTestDriver(t)
	valid := NewPayload("example.Valid", nil)
	_, err := d.Push(ctx, valid)
	redisOK(t, err)
	const id, raw = "corrupt-head", "{malformed"
	redisOK(t, d.client().HSet(ctx, d.key("job", id), "data", raw).Err())
	redisOK(t, d.client().LPush(ctx, d.key("queue", "default"), id, id).Err())
	redisOK(t, d.client().RPush(ctx, d.key("queue", "other"), id).Err())
	for _, key := range []string{d.key("delayed"), d.index("", ""), d.index("other", Pending)} {
		redisOK(t, d.client().ZAdd(ctx, key, redis.Z{Score: 1, Member: id}).Err())
	}
	injected := errors.New("quarantine EXEC rejected")
	var fired atomic.Bool
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if len(cmds) > 0 && cmds[0].Name() == "multi" && fired.CompareAndSwap(false, true) {
			for _, cmd := range cmds {
				cmd.SetErr(injected)
			}
			return injected
		}
		return next(ctx, cmds)
	}})
	if claimed, err := d.Pop(ctx, "default"); !errors.Is(err, injected) || claimed != nil {
		t.Fatalf("quarantine failure hidden: %+v %v", claimed, err)
	}
	if n := d.client().LLen(ctx, d.key("queue", "default")).Val(); n != 3 {
		t.Fatalf("quarantine partially removed ready references: %d", n)
	}
	if _, err := d.client().ZScore(ctx, d.key("delayed"), id).Result(); err != nil {
		t.Fatalf("quarantine partially removed delayed reference: %v", err)
	}
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	if claimed == nil || claimed.ID != valid.ID {
		t.Fatalf("malformed head blocked healthy job: %+v", claimed)
	}
	for _, queue := range []string{"default", "other"} {
		if n := d.client().LLen(ctx, d.key("queue", queue)).Val(); n != 0 {
			t.Fatalf("quarantined job remains runnable in %s", queue)
		}
	}
	for _, key := range []string{d.key("delayed"), d.index("", ""), d.index("other", Pending)} {
		if _, err := d.client().ZScore(ctx, key, id).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("quarantine left reference %s: %v", key, err)
		}
	}
	if got := d.client().HGet(ctx, d.key("job", id), "data").Val(); got != raw {
		t.Fatalf("quarantine altered malformed hash: %q", got)
	}
}

func TestRedisDelayedPromotionQuarantinesMalformedHash(t *testing.T) {
	d, ctx := redisTestDriver(t)
	redisOK(t, d.client().HSet(ctx, d.key("job", "corrupt"), "data", "null").Err())
	redisOK(t, d.client().ZAdd(ctx, d.key("delayed"), redis.Z{Score: 0, Member: "corrupt"}).Err())
	past := Now() - 1
	valid := NewPayload("example.Delayed", nil)
	valid.DelayUntil = &past
	redisSeedPayload(t, d, ctx, valid)
	redisOK(t, d.client().ZAdd(ctx, d.key("delayed"), redis.Z{Score: past, Member: valid.ID}).Err())
	moved, err := d.PopDelayed(ctx)
	redisOK(t, err)
	if len(moved) != 1 || moved[0].ID != valid.ID {
		t.Fatalf("corrupt delayed payload blocked promotion: %+v", moved)
	}
	if raw := d.client().HGet(ctx, d.key("job", "corrupt"), "data").Val(); raw != "null" {
		t.Fatal("delayed quarantine deleted evidence")
	}
}

func TestRedisExplicitDeletionRemovesMalformedHashes(t *testing.T) {
	for _, mode := range []string{"delete", "bulk", "flush"} {
		t.Run(mode, func(t *testing.T) {
			d, ctx := redisTestDriver(t)
			const id = "corrupt"
			redisOK(t, d.client().HSet(ctx, d.key("job", id), "data", "invalid-json").Err())
			redisOK(t, d.client().RPush(ctx, d.key("queue", "default"), id).Err())
			for _, key := range []string{d.key("delayed"), d.index("", ""), d.index("default", Failed)} {
				redisOK(t, d.client().ZAdd(ctx, key, redis.Z{Score: 1, Member: id}).Err())
			}
			n, err := d.PruneTerminalJobs(ctx, PruneOptions{})
			redisOK(t, err)
			if n != 0 || d.client().Exists(ctx, d.key("job", id)).Val() != 1 {
				t.Fatal("filtered prune must preserve unclassifiable hash")
			}
			switch mode {
			case "delete":
				err = d.Delete(ctx, id)
			case "bulk":
				n, err = d.BulkDeleteJobs(ctx, []string{id}, 1)
				if n != 1 {
					t.Fatalf("bulk delete count %d", n)
				}
			case "flush":
				err = d.Flush(ctx, "")
			}
			redisOK(t, err)
			keys, err := d.scanKeys(ctx, "", 100)
			redisOK(t, err)
			if len(keys) != 0 {
				t.Fatalf("malformed hash/references survive explicit deletion: %v", keys)
			}
		})
	}
}

func TestRedisRejectsAmbiguousQueueNames(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	job.Queue = "default:status:pending"
	if _, err := d.Push(ctx, job); err == nil {
		t.Fatal("ambiguous queue accepted by Push")
	}
	if _, err := d.Pop(ctx, job.Queue); err == nil {
		t.Fatal("ambiguous queue accepted by Pop")
	}
	keys, err := d.scanKeys(ctx, "", 100)
	redisOK(t, err)
	if len(keys) != 0 {
		t.Fatalf("invalid queue operations wrote keys: %v", keys)
	}
}

func TestRedisThroughputIncludesMetricOnlyAndFlushedQueues(t *testing.T) {
	d, ctx := redisTestDriver(t)
	redisOK(t, d.RecordMetric(ctx, "metrics-only", Completed, 7))
	job := NewPayload("example.Job", nil)
	job.Queue = "flushed"
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	redisOK(t, d.RecordMetric(ctx, "flushed", Failed, 2))
	redisOK(t, d.Flush(ctx, "flushed"))
	queues, err := d.Queues(ctx)
	redisOK(t, err)
	if len(queues) != 0 {
		t.Fatalf("test requires no registered queues: %v", queues)
	}
	throughput, err := d.RecentThroughput(ctx, 60, "")
	redisOK(t, err)
	if !reflect.DeepEqual(throughput, map[string]int{Processing: 0, Completed: 1, Failed: 1}) {
		t.Fatalf("all-queue throughput omitted metric streams: %v", throughput)
	}
	throughput, err = d.RecentThroughput(ctx, 60, "metrics-only")
	redisOK(t, err)
	if throughput[Completed] != 1 || throughput[Failed] != 0 {
		t.Fatalf("queue-specific throughput leaked metrics: %v", throughput)
	}
}

func TestRedisTerminalBatchOutcomeAtomic(t *testing.T) {
	for _, status := range []string{Completed, Failed} {
		for _, failure := range []string{"none", "before", "after"} {
			t.Run(status+"/"+failure, func(t *testing.T) {
				d, ctx := redisTestDriver(t)
				batchID := "batch"
				redisOK(t, d.StoreBatch(ctx, batchID, map[string]any{"id": batchID, "status": Processing, "total": 1, "completed_count": 0, "failed_count": 0}))
				job := NewPayload("example.Job", nil)
				job.BatchID = &batchID
				_, err := d.Push(ctx, job)
				redisOK(t, err)
				claimed, err := d.Pop(ctx, "default")
				redisOK(t, err)
				before := claimed.Clone()
				injected := errors.New("terminal batch disconnect")
				var calls atomic.Int32
				d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
					if redisTestExecWrites(cmds, d.key("job", job.ID)) {
						calls.Add(1)
						if !redisTestExecWrites(cmds, d.key("batch", batchID)) {
							return errors.New("terminal job and batch outcome must share EXEC")
						}
						if failure == "before" {
							for _, cmd := range cmds {
								cmd.SetErr(injected)
							}
							return injected
						}
						if err := next(ctx, cmds); err != nil {
							return err
						}
						if failure == "after" {
							return injected
						}
						return nil
					}
					return next(ctx, cmds)
				}})
				if status == Completed {
					err = d.Complete(ctx, claimed)
				} else {
					err = d.Fail(ctx, claimed, "failed")
				}
				if failure == "none" {
					redisOK(t, err)
				} else if !errors.Is(err, injected) || !reflect.DeepEqual(claimed, before) {
					t.Fatalf("ambiguous/failed lifecycle mutated caller: %v", err)
				}
				if calls.Load() != 1 {
					t.Fatalf("terminal EXEC replayed %d times", calls.Load())
				}
				batch, err := d.GetBatch(ctx, batchID)
				redisOK(t, err)
				stored := redisStored(t, d, ctx, job.ID)
				done := valueNumber(batch["completed_count"]) + valueNumber(batch["failed_count"])
				if failure == "before" {
					if done != 0 || stored.Status != Processing || batch["member_outcomes"] != nil {
						t.Fatalf("partial terminal transaction: job=%s batch=%#v", stored.Status, batch)
					}
				} else {
					outcomes, _ := batch["member_outcomes"].(map[string]any)
					if done != 1 || stored.Status != status || outcomes[job.ID] != status {
						t.Fatalf("terminal outcome not durable with job: job=%s batch=%#v", stored.Status, batch)
					}
				}
			})
		}
	}
}

func TestRedisBatchFirstOutcomeSurvivesManualRetry(t *testing.T) {
	d, ctx := redisTestDriver(t)
	batchID := "batch"
	redisOK(t, d.StoreBatch(ctx, batchID, map[string]any{"id": batchID, "status": Processing, "total": 1}))
	job := NewPayload("example.Job", nil)
	job.BatchID = &batchID
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	claimed, err := d.Pop(ctx, "default")
	redisOK(t, err)
	stale := claimed.Clone()
	redisOK(t, d.Fail(ctx, claimed, "first terminal outcome"))
	redisOK(t, d.Release(ctx, claimed, 0))
	retry, err := d.Pop(ctx, "default")
	redisOK(t, err)
	if err := d.Complete(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale attempt changed batch: %v", err)
	}
	redisOK(t, d.Complete(ctx, retry))
	batch, err := d.GetBatch(ctx, batchID)
	redisOK(t, err)
	outcomes, _ := batch["member_outcomes"].(map[string]any)
	if valueNumber(batch["failed_count"]) != 1 || valueNumber(batch["completed_count"]) != 0 || len(outcomes) != 1 || outcomes[job.ID] != Failed {
		t.Fatalf("manual retry counted member twice: %#v", batch)
	}
	missingID := "missing"
	missing := NewPayload("example.OrphanBatch", nil)
	missing.BatchID = &missingID
	_, err = d.Push(ctx, missing)
	redisOK(t, err)
	missing, err = d.Pop(ctx, "default")
	redisOK(t, err)
	redisOK(t, d.Complete(ctx, missing))
	if batch, err := d.GetBatch(ctx, missingID); err != nil || batch != nil {
		t.Fatalf("missing batch must remain absent: %#v %v", batch, err)
	}
}

func TestRedisConcurrentBatchTerminalOutcomes(t *testing.T) {
	d, ctx := redisTestDriver(t)
	const count = 24
	batchID := "batch"
	redisOK(t, d.StoreBatch(ctx, batchID, map[string]any{"id": batchID, "status": Processing, "total": count}))
	jobs := make([]*JobPayload, count)
	for i := range jobs {
		jobs[i] = NewPayload("example.Job", nil)
		jobs[i].BatchID = &batchID
	}
	_, err := d.PushMany(ctx, jobs)
	redisOK(t, err)
	for i := range jobs {
		jobs[i], err = d.Pop(ctx, "default")
		redisOK(t, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i, job := range jobs {
		wg.Add(1)
		go func(i int, job *JobPayload) {
			defer wg.Done()
			if i%2 == 0 {
				errs <- d.Complete(ctx, job)
			} else {
				errs <- d.Fail(ctx, job, "failed")
			}
		}(i, job)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		redisOK(t, err)
	}
	batch, err := d.GetBatch(ctx, batchID)
	redisOK(t, err)
	outcomes, _ := batch["member_outcomes"].(map[string]any)
	if valueNumber(batch["completed_count"]) != count/2 || valueNumber(batch["failed_count"]) != count/2 || len(outcomes) != count {
		t.Fatalf("concurrent batch terminal outcomes lost: %#v", batch)
	}
}

func TestRedisPendingBatchCallbackDiscovery(t *testing.T) {
	d, ctx := redisTestDriver(t)
	fixtures := map[string]map[string]any{
		"done":       {"total": 2, "completed_count": 1, "failed_count": 1},
		"catch":      {"total": 3, "failed_count": 1},
		"empty":      {"total": 0},
		"processing": {"total": 3, "completed_count": 1},
		"allowed":    {"total": 3, "failed_count": 1, "allow_failures": true},
		"catch-sent": {"total": 3, "failed_count": 1, "catch_dispatched": true},
		"completed":  {"total": 1, "completed_count": 1, "status": Completed},
	}
	for id, data := range fixtures {
		data["id"] = id
		if data["status"] == nil {
			data["status"] = Processing
		}
		redisOK(t, d.StoreBatch(ctx, id, data))
	}
	redisOK(t, d.client().HSet(ctx, d.key("batch", "malformed"), "data", "not-json").Err())
	batches, err := d.GetPendingBatches(ctx, 100)
	redisOK(t, err)
	ids := []string{}
	for _, batch := range batches {
		ids = append(ids, batch["id"].(string))
	}
	slices.Sort(ids)
	if !reflect.DeepEqual(ids, []string{"catch", "done", "empty"}) {
		t.Fatalf("callback discovery: %v", ids)
	}
	batches, err = d.GetPendingBatches(ctx, 2)
	redisOK(t, err)
	if len(batches) != 2 {
		t.Fatalf("callback discovery limit ignored: %d", len(batches))
	}
	redisOK(t, d.UpdateBatch(ctx, "catch", map[string]any{"catch_dispatched": true}))
	redisOK(t, d.UpdateBatch(ctx, "done", map[string]any{"status": Completed}))
	batches, err = d.GetPendingBatches(ctx, 0)
	redisOK(t, err)
	if len(batches) != 1 || batches[0]["id"] != "empty" {
		t.Fatalf("dispatched callbacks still pending: %#v", batches)
	}
	for i := 0; i < 105; i++ {
		id := fmt.Sprintf("ready-%03d", i)
		redisOK(t, d.StoreBatch(ctx, id, map[string]any{"id": id, "total": 1, "completed_count": 1, "status": Processing}))
	}
	batches, err = d.GetPendingBatches(ctx, 0)
	redisOK(t, err)
	if len(batches) != 100 {
		t.Fatalf("default callback discovery limit = %d, want 100", len(batches))
	}
}

func TestRedisPruneRejectsNaNAge(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := NewPayload("example.Job", nil)
	_, err := d.Push(ctx, job)
	redisOK(t, err)
	age := math.NaN()
	for _, prune := range []func(context.Context, PruneOptions) (int, error){d.Prune, d.PruneTerminalJobs} {
		if count, err := prune(ctx, PruneOptions{OlderThanSeconds: &age}); err == nil || count != 0 {
			t.Fatalf("NaN prune accepted: count=%d err=%v", count, err)
		}
	}
	redisStored(t, d, ctx, job.ID)
}
