package baqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

func redisDynamicPeer(t *testing.T, d *RedisDriver, ctx context.Context) *RedisDriver {
	t.Helper()
	peer, err := NewRedisDriver(os.Getenv("BAQUEUE_REDIS_URL"), d.prefix)
	redisOK(t, err)
	redisOK(t, peer.Connect(ctx))
	t.Cleanup(func() { _ = peer.Disconnect(context.Background()) })
	return peer
}

func redisDynamicPolicy(t *testing.T, d *RedisDriver, ctx context.Context, name string) *DynamicQueueInfo {
	t.Helper()
	info, err := d.readDynamicQueue(ctx, d.client(), name)
	redisOK(t, err)
	return info
}

func redisDynamicDue(t *testing.T, d *RedisDriver, ctx context.Context, name string) {
	t.Helper()
	info := redisDynamicPolicy(t, d, ctx, name)
	if info == nil {
		t.Fatal("missing dynamic policy")
	}
	idle := Now() - info.IdleTimeoutSeconds - 10
	info.IdleSince = &idle
	raw, err := json.Marshal(info)
	redisOK(t, err)
	redisOK(t, d.client().HSet(ctx, d.dynamicKey(name), "data", raw).Err())
}

func redisDynamicFailed(t *testing.T, d *RedisDriver, ctx context.Context) *JobPayload {
	t.Helper()
	job := NewPayload("example.Job", nil)
	job.Queue, job.MaxAttempts = "dynamic", 7
	job.Backoff = Backoff{Delays: []float64{2, 4, 8}}
	_, err := d.PushDynamic(ctx, []*JobPayload{job}, job.Queue, 30)
	redisOK(t, err)
	job, err = d.Pop(ctx, job.Queue)
	redisOK(t, err)
	job.History = append(job.History, Attempt{Attempt: 1, StartedAt: *job.StartedAt, FinishedAt: Now(), Status: Failed})
	redisOK(t, d.Fail(ctx, job, "retained"))
	redisDynamicDue(t, d, ctx, job.Queue)
	return job
}

func redisDynamicExecTouches(commands []redis.Cmder, key string) bool {
	if len(commands) == 0 || commands[0].Name() != "multi" {
		return false
	}
	for _, command := range commands {
		args := command.Args()
		if len(args) > 1 && fmt.Sprint(args[1]) == key {
			return true
		}
	}
	return false
}

func TestRedisDynamicUncertainEXECNeverReplayed(t *testing.T) {
	for _, operation := range []string{"push", "release", "expire"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", operation, after), func(t *testing.T) {
				d, ctx := redisTestDriver(t)
				job := NewPayload("example.Job", nil)
				job.Queue = "dynamic"
				if operation != "push" {
					job = redisDynamicFailed(t, d, ctx)
					if operation == "release" {
						_, err := d.ExpireDynamicQueues(ctx)
						redisOK(t, err)
					}
				}
				before := job.Clone()
				var calls atomic.Int32
				injected := errors.New("dynamic EXEC reply lost")
				d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
					if redisDynamicExecTouches(cmds, d.dynamicKey(job.Queue)) {
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
				var err error
				switch operation {
				case "push":
					_, err = d.PushDynamic(ctx, []*JobPayload{job}, job.Queue, 30)
				case "release":
					err = d.Release(ctx, job, 0)
				case "expire":
					_, err = d.ExpireDynamicQueues(ctx)
				}
				if !errors.Is(err, injected) || calls.Load() != 1 {
					t.Fatalf("uncertain EXEC replayed or hidden: calls=%d error=%v", calls.Load(), err)
				}
				if !reflect.DeepEqual(job, before) {
					t.Fatal("failed operation mutated caller payload")
				}
				info := redisDynamicPolicy(t, d, ctx, job.Queue)
				stored, err := d.GetJob(ctx, job.ID)
				redisOK(t, err)
				if operation == "push" && !after {
					if info != nil || stored != nil {
						t.Fatal("pre-EXEC failure left policy or work")
					}
					return
				}
				if info == nil || stored == nil {
					t.Fatal("committed policy or retained job missing")
				}
				active := operation == "push" || (operation == "release" && after) || (operation == "expire" && !after)
				if info.Active != active {
					t.Fatalf("policy activity mismatch: %+v", info)
				}
				wantStatus := Failed
				if operation == "push" || (operation == "release" && after) {
					wantStatus = Pending
				}
				if stored.Status != wantStatus || !reflect.DeepEqual(stored.History, before.History) || stored.MaxAttempts != before.MaxAttempts {
					t.Fatalf("retained job changed unexpectedly: %+v", stored)
				}
				redisAssertIndexes(t, d, ctx, stored)
				size, err := d.Size(ctx, job.Queue)
				redisOK(t, err)
				wantSize := 0
				if wantStatus == Pending {
					wantSize = 1
				}
				if size != wantSize {
					t.Fatalf("ready references=%d want %d", size, wantSize)
				}
			})
		}
	}
}

func TestRedisDynamicExpiryFencesConcurrentWrites(t *testing.T) {
	for _, operation := range []string{"push", "bulk", "dynamic", "register", "release"} {
		t.Run(operation, func(t *testing.T) {
			d, ctx := redisTestDriver(t)
			job := redisDynamicFailed(t, d, ctx)
			peer := redisDynamicPeer(t, d, ctx)
			newcomer := NewPayload("example.New", nil)
			newcomer.Queue = job.Queue
			var fired atomic.Bool
			d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				if redisDynamicExecTouches(cmds, d.dynamicKey(job.Queue)) && fired.CompareAndSwap(false, true) {
					var err error
					switch operation {
					case "push":
						_, err = peer.Push(ctx, newcomer)
					case "bulk":
						_, err = peer.PushMany(ctx, []*JobPayload{newcomer})
					case "dynamic":
						_, err = peer.PushDynamic(ctx, []*JobPayload{newcomer}, job.Queue, 30)
					case "register":
						_, err = peer.RegisterDynamicQueue(ctx, job.Queue, 30)
					case "release":
						err = peer.Release(ctx, job, 0)
					}
					if err != nil {
						return err
					}
				}
				return next(ctx, cmds)
			}})
			expired, err := d.ExpireDynamicQueues(ctx)
			redisOK(t, err)
			if len(expired) != 0 || !fired.Load() {
				t.Fatalf("concurrent write lost: expired=%v fired=%t", expired, fired.Load())
			}
			info := redisDynamicPolicy(t, d, ctx, job.Queue)
			if !info.Active || (operation != "register" && info.IdleSince != nil) {
				t.Fatalf("policy not refreshed: %+v", info)
			}
			ready, err := d.client().LRange(ctx, d.key("queue", job.Queue), 0, -1).Result()
			redisOK(t, err)
			want := []string{newcomer.ID}
			if operation == "register" {
				want = []string{}
			} else if operation == "release" {
				want = []string{job.ID}
			}
			if !slices.Equal(ready, want) {
				t.Fatalf("ready=%v want=%v", ready, want)
			}
		})
	}
}

func TestRedisDynamicDispatchRecreatesAfterConcurrentGC(t *testing.T) {
	d, ctx := redisTestDriver(t)
	_, err := d.RegisterDynamicQueue(ctx, "dynamic", 30)
	redisOK(t, err)
	redisDynamicDue(t, d, ctx, "dynamic")
	peer := redisDynamicPeer(t, d, ctx)
	job := NewPayload("example.Job", nil)
	job.Queue = "dynamic"
	var fired atomic.Bool
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if redisTestExecWrites(cmds, d.key("job", job.ID)) && fired.CompareAndSwap(false, true) {
			_, err := peer.ExpireDynamicQueues(ctx)
			if err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}})
	_, err = d.PushDynamic(ctx, []*JobPayload{job}, job.Queue, 30)
	redisOK(t, err)
	info := redisDynamicPolicy(t, d, ctx, job.Queue)
	if !fired.Load() || info == nil || !info.Active || info.IdleSince != nil {
		t.Fatalf("dispatch lost GC race: %+v", info)
	}
	ready, err := d.client().LRange(ctx, d.key("queue", job.Queue), 0, -1).Result()
	redisOK(t, err)
	if !slices.Equal(ready, []string{job.ID}) {
		t.Fatalf("duplicate or missing dispatch: %v", ready)
	}
	redisAssertIndexes(t, d, ctx, redisStored(t, d, ctx, job.ID))
}

func TestRedisDynamicRegistrationFencesOrdinaryPush(t *testing.T) {
	d, ctx := redisTestDriver(t)
	peer := redisDynamicPeer(t, d, ctx)
	ordinary, rejected := NewPayload("example.Ordinary", nil), NewPayload("example.Rejected", nil)
	ordinary.Queue, rejected.Queue = "dynamic", "dynamic"
	var fired atomic.Bool
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if redisTestExecWrites(cmds, d.dynamicKey(ordinary.Queue)) && fired.CompareAndSwap(false, true) {
			if _, err := peer.Push(ctx, ordinary); err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}})
	if _, err := d.PushDynamic(ctx, []*JobPayload{rejected}, rejected.Queue, 30); err == nil {
		t.Fatal("ordinary queue converted after WATCH conflict")
	}
	if !fired.Load() || redisDynamicPolicy(t, d, ctx, ordinary.Queue) != nil {
		t.Fatal("conflicting registration persisted")
	}
	stored, err := d.GetJob(ctx, rejected.ID)
	redisOK(t, err)
	if stored != nil {
		t.Fatal("rejected batch partially persisted")
	}
	redisAssertIndexes(t, d, ctx, redisStored(t, d, ctx, ordinary.ID))
}

func TestRedisDynamicNoopSweepersPreserveProducerWatch(t *testing.T) {
	for _, state := range []string{"busy", "not_due", "inactive_retained"} {
		t.Run(state, func(t *testing.T) {
			d, ctx := redisTestDriver(t)
			if state == "inactive_retained" {
				redisDynamicFailed(t, d, ctx)
				_, err := d.ExpireDynamicQueues(ctx)
				redisOK(t, err)
			} else {
				_, err := d.RegisterDynamicQueue(ctx, "dynamic", 3600)
				redisOK(t, err)
				if state == "busy" {
					job := NewPayload("example.Job", nil)
					job.Queue = "dynamic"
					_, err = d.Push(ctx, job)
					redisOK(t, err)
				}
			}
			peer := redisDynamicPeer(t, d, ctx)
			var executions atomic.Int32
			hook := redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				if len(cmds) != 0 && cmds[0].Name() == "multi" {
					executions.Add(1)
				}
				return next(ctx, cmds)
			}}
			d.client().AddHook(hook)
			peer.client().AddHook(hook)
			err := d.client().Watch(ctx, func(tx *redis.Tx) error {
				for range 2 {
					for _, supervisor := range []*RedisDriver{d, peer} {
						if _, err := supervisor.ExpireDynamicQueues(ctx); err != nil {
							return err
						}
					}
				}
				if executions.Load() != 0 {
					t.Fatalf("no-op sweepers executed %d transactions", executions.Load())
				}
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Ping(ctx)
					return nil
				})
				return err
			}, d.dynamicKey("dynamic"), d.key("queues"), d.key("dynamic_queues"), d.key("queue", "dynamic"))
			redisOK(t, err)
		})
	}
}

func TestRedisDynamicTypeFailureDoesNotOrphanBatch(t *testing.T) {
	d, ctx := redisTestDriver(t)
	redisOK(t, d.client().Set(ctx, d.key("dynamic_queues"), "wrong type", 0).Err())
	job := NewPayload("example.Job", nil)
	job.Queue = "dynamic"
	if _, err := d.PushDynamic(ctx, []*JobPayload{job}, job.Queue, 30); err == nil {
		t.Fatal("invalid registry type accepted")
	}
	if redisDynamicPolicy(t, d, ctx, job.Queue) != nil {
		t.Fatal("failed batch registered a policy")
	}
	stored, err := d.GetJob(ctx, job.ID)
	redisOK(t, err)
	if stored != nil {
		t.Fatal("failed batch orphaned work")
	}
}

func TestRedisDynamicIndexRepairFencesExpiryAndRetainsMetrics(t *testing.T) {
	d, ctx := redisTestDriver(t)
	job := redisDynamicFailed(t, d, ctx)
	redisOK(t, d.RecordMetric(ctx, job.Queue, Failed, 1))
	peer := redisDynamicPeer(t, d, ctx)
	var fired atomic.Bool
	d.client().AddHook(redisTestHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if redisDynamicExecTouches(cmds, d.key("queues")) && fired.CompareAndSwap(false, true) {
			_, err := peer.ExpireDynamicQueues(ctx)
			if err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}})
	_, err := d.ReconcileIndexes(ctx, 100)
	redisOK(t, err)
	if !fired.Load() {
		t.Fatal("repair interleaving not exercised")
	}
	known, err := d.client().SIsMember(ctx, d.key("queues"), job.Queue).Result()
	redisOK(t, err)
	if known || redisDynamicPolicy(t, d, ctx, job.Queue).Active {
		t.Fatal("index repair revived inactive history queue")
	}
	metrics, err := d.GetMetrics(ctx, "")
	redisOK(t, err)
	if metrics[job.Queue].(map[string]int)[Failed] != 1 {
		t.Fatalf("retained global metric missing: %v", metrics)
	}
	redisOK(t, d.Delete(ctx, job.ID))
	_, err = d.ExpireDynamicQueues(ctx)
	redisOK(t, err)
	if redisDynamicPolicy(t, d, ctx, job.Queue) != nil {
		t.Fatal("unused tombstone retained")
	}
	throughput, err := d.RecentThroughput(ctx, 60, "")
	redisOK(t, err)
	if throughput[Failed] != 1 {
		t.Fatalf("policy GC hid retained metric history: %v", throughput)
	}
}
