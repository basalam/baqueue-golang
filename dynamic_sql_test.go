package baqueue

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestSQLiteDynamicSQL(t *testing.T) {
	sqlDynamicContract(t, sqlTestSQLite)
}

func TestPostgresDynamicSQL(t *testing.T) {
	sqlDynamicContract(t, sqlTestPostgres)
}

func sqlDynamicAge(t *testing.T, d *SQLDriver, name string) {
	t.Helper()
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	_, err = db.ExecContext(t.Context(), "UPDATE baqueue_go_dynamic_queues SET idle_since=$2 WHERE name=$1", name, Now()-120)
	sqlTestCheck(t, err)
}

func sqlDynamicPolicy(t *testing.T, d *SQLDriver, name string) DynamicQueueInfo {
	t.Helper()
	db, err := d.sqlDB()
	sqlTestCheck(t, err)
	info, err := sqlScanDynamicQueue(db.QueryRowContext(t.Context(), "SELECT "+sqlDynamicColumns+" FROM baqueue_go_dynamic_queues WHERE name=$1", name))
	sqlTestCheck(t, err)
	return info
}

func sqlDynamicExpiredJob(t *testing.T, d *SQLDriver, name string) *JobPayload {
	t.Helper()
	p := NewPayload("job", nil)
	p.Queue, p.Status, p.Attempts, p.MaxAttempts = name, Failed, 2, 7
	p.Backoff = Backoff{Delays: []float64{2, 10}}
	p.History = []Attempt{{Attempt: 1, Status: Failed}, {Attempt: 2, Status: Failed}}
	_, err := d.PushDynamic(t.Context(), []*JobPayload{p}, name, 30)
	sqlTestCheck(t, err)
	sqlDynamicAge(t, d, name)
	expired, err := d.ExpireDynamicQueues(t.Context())
	sqlTestCheck(t, err)
	if !reflect.DeepEqual(expired, []string{name}) {
		t.Fatalf("expected expiration of %q, got %v", name, expired)
	}
	return p
}

func sqlDynamicContract(t *testing.T, factory sqlTestFactory) {
	t.Run("BusyExpiryAvoidsNoopUpdates", func(t *testing.T) {
		d, ctx := factory(t), t.Context()
		p := NewPayload("job", nil)
		p.Queue = "dynamic"
		_, err := d.PushDynamic(ctx, []*JobPayload{p}, p.Queue, 30)
		sqlTestCheck(t, err)
		db, err := d.sqlDB()
		sqlTestCheck(t, err)
		_, err = db.ExecContext(ctx, "CREATE TABLE dynamic_policy_updates (count INTEGER NOT NULL)")
		sqlTestCheck(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO dynamic_policy_updates VALUES (0)")
		sqlTestCheck(t, err)
		trigger := `CREATE TRIGGER count_dynamic_updates AFTER UPDATE ON baqueue_go_dynamic_queues
 BEGIN UPDATE dynamic_policy_updates SET count=count+1; END`
		if d.postgres {
			_, err = db.ExecContext(ctx, `CREATE FUNCTION record_dynamic_policy_update() RETURNS trigger
 LANGUAGE plpgsql AS $$ BEGIN UPDATE dynamic_policy_updates SET count=count+1; RETURN NEW; END; $$`)
			sqlTestCheck(t, err)
			trigger = `CREATE TRIGGER count_dynamic_updates AFTER UPDATE ON baqueue_go_dynamic_queues
 FOR EACH ROW EXECUTE FUNCTION record_dynamic_policy_update()`
		}
		_, err = db.ExecContext(ctx, trigger)
		sqlTestCheck(t, err)
		old := Now() - 120
		for _, state := range []struct {
			name    string
			active  bool
			idle    *float64
			updates int
		}{
			{"already-busy", true, nil, 0},
			{"stale-idle", true, &old, 1},
			{"inactive", false, nil, 1},
			{"inactive-with-idle", false, &old, 1},
		} {
			_, err = db.ExecContext(ctx, "UPDATE baqueue_go_dynamic_queues SET active=$1,idle_since=$2 WHERE name=$3", state.active, state.idle, p.Queue)
			sqlTestCheck(t, err)
			_, err = db.ExecContext(ctx, "UPDATE dynamic_policy_updates SET count=0")
			sqlTestCheck(t, err)
			expired, err := d.ExpireDynamicQueues(ctx)
			sqlTestCheck(t, err)
			var updates int
			sqlTestCheck(t, db.QueryRowContext(ctx, "SELECT count FROM dynamic_policy_updates").Scan(&updates))
			if updates != state.updates || len(expired) != 0 {
				t.Fatalf("%s: updates=%d, want=%d, expired=%v", state.name, updates, state.updates, expired)
			}
			info := sqlDynamicPolicy(t, d, p.Queue)
			if !info.Active || info.IdleSince != nil {
				t.Fatalf("%s: busy policy not repaired: %+v", state.name, info)
			}
		}
	})

	t.Run("RegistrationValidationAndRefresh", func(t *testing.T) {
		d, ctx := factory(t), t.Context()
		for _, name := range []string{"", " \t", "unsafe:status:failed"} {
			if _, err := d.RegisterDynamicQueue(ctx, name, 30); err == nil {
				t.Fatalf("invalid name accepted: %q", name)
			}
		}
		for _, timeout := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			if _, err := d.RegisterDynamicQueue(ctx, "dynamic", timeout); err == nil {
				t.Fatalf("invalid timeout accepted: %v", timeout)
			}
		}
		first, err := d.RegisterDynamicQueue(ctx, "dynamic", 30)
		sqlTestCheck(t, err)
		if !first.Active || first.IdleSince == nil {
			t.Fatalf("new empty policy is not active/idle: %+v", first)
		}
		sqlDynamicAge(t, d, "dynamic")
		before := Now()
		refreshed, err := d.RegisterDynamicQueue(ctx, "dynamic", 30)
		sqlTestCheck(t, err)
		if refreshed.CreatedAt != first.CreatedAt || refreshed.IdleSince == nil || *refreshed.IdleSince < before {
			t.Fatalf("registration did not refresh only idle time: %+v", refreshed)
		}
		sqlDynamicAge(t, d, "dynamic")
		ids, err := d.PushDynamic(ctx, nil, "dynamic", 30)
		sqlTestCheck(t, err)
		if len(ids) != 0 || *sqlDynamicPolicy(t, d, "dynamic").IdleSince < before {
			t.Fatal("empty dynamic push did not refresh registration")
		}
		if _, err = d.RegisterDynamicQueue(ctx, "dynamic", 31); err == nil {
			t.Fatal("conflicting timeout accepted")
		}
		_, err = d.RegisterDynamicQueue(ctx, "a", 30)
		sqlTestCheck(t, err)
		infos, err := d.GetDynamicQueues(ctx)
		sqlTestCheck(t, err)
		if len(infos) != 2 || infos[0].Name != "a" || infos[1].Name != "dynamic" {
			t.Fatalf("active definitions not sorted: %+v", infos)
		}
		queues, err := d.Queues(ctx)
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(queues, []string{"a", "dynamic"}) {
			t.Fatalf("empty definitions missing: %v", queues)
		}
	})

	t.Run("OrdinaryConflictAndBatchRollback", func(t *testing.T) {
		d, ctx := factory(t), t.Context()
		ordinary := NewPayload("job", nil)
		ordinary.Queue, ordinary.Status = "ordinary", Completed
		_, err := d.Push(ctx, ordinary)
		sqlTestCheck(t, err)
		if _, err = d.RegisterDynamicQueue(ctx, "ordinary", 30); err == nil {
			t.Fatal("retained ordinary queue was converted")
		}
		p := NewPayload("job", nil)
		p.Queue = "dynamic"
		if ids, err := d.PushDynamic(ctx, []*JobPayload{p, p}, "dynamic", 30); !errors.Is(err, ErrDuplicate) || ids != nil {
			t.Fatalf("expected atomic duplicate failure: ids=%v err=%v", ids, err)
		}
		infos, err := d.GetDynamicQueues(ctx)
		sqlTestCheck(t, err)
		stored, err := d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if len(infos) != 0 || stored != nil {
			t.Fatal("failed batch left a policy or partial work")
		}
		if _, err = d.PushDynamic(ctx, []*JobPayload{ordinary}, "dynamic", 30); err == nil {
			t.Fatal("mismatched queue accepted")
		}
		if _, err = d.PushDynamic(ctx, []*JobPayload{nil}, "dynamic", 30); err == nil {
			t.Fatal("nil payload accepted")
		}
		retained := sqlDynamicExpiredJob(t, d, "dynamic")
		before := sqlDynamicPolicy(t, d, "dynamic")
		if _, err = d.PushDynamic(ctx, []*JobPayload{p, retained}, "dynamic", 30); !errors.Is(err, ErrDuplicate) {
			t.Fatalf("expected duplicate error, got %v", err)
		}
		if got := sqlDynamicPolicy(t, d, "dynamic"); !reflect.DeepEqual(got, before) {
			t.Fatalf("rollback changed inactive policy: %+v", got)
		}
		stored, err = d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		if stored != nil {
			t.Fatal("partial batch survived reactivation failure")
		}
	})

	t.Run("BusyIncludesDelayedProcessingAndBackoff", func(t *testing.T) {
		d, ctx := factory(t), t.Context()
		p := NewPayload("job", nil)
		future := Now() + 3600
		p.Queue, p.DelayUntil = "dynamic", &future
		_, err := d.PushDynamic(ctx, []*JobPayload{p}, p.Queue, 30)
		sqlTestCheck(t, err)
		for _, state := range []string{"delayed", "processing", "backoff"} {
			sqlDynamicAge(t, d, p.Queue)
			expired, err := d.ExpireDynamicQueues(ctx)
			sqlTestCheck(t, err)
			if len(expired) != 0 || sqlDynamicPolicy(t, d, p.Queue).IdleSince != nil {
				t.Fatalf("busy %s queue expired or gained idle clock", state)
			}
			if state == "delayed" {
				_, err = d.Promote(ctx, p.ID)
				sqlTestCheck(t, err)
				p, err = d.Pop(ctx, p.Queue)
				sqlTestCheck(t, err)
				if p == nil {
					t.Fatal("promoted job was not available")
				}
			} else if state == "processing" {
				sqlTestCheck(t, d.Release(ctx, p, 3600))
			}
		}
	})

	t.Run("IdleObservationHistoryRetryAndGC", func(t *testing.T) {
		d, ctx := factory(t), t.Context()
		p := NewPayload("job", nil)
		p.Queue, p.MaxAttempts = "dynamic", 7
		_, err := d.PushDynamic(ctx, []*JobPayload{p}, p.Queue, 30)
		sqlTestCheck(t, err)
		claimed, err := d.Pop(ctx, p.Queue)
		sqlTestCheck(t, err)
		claimed.History = []Attempt{{Attempt: 1, StartedAt: *claimed.StartedAt, FinishedAt: Now(), Status: Failed}}
		sqlTestCheck(t, d.Fail(ctx, claimed, "retained"))
		sqlTestCheck(t, d.RecordMetric(ctx, p.Queue, Failed, 1))
		expired, err := d.ExpireDynamicQueues(ctx)
		sqlTestCheck(t, err)
		if len(expired) != 0 || sqlDynamicPolicy(t, d, p.Queue).IdleSince == nil {
			t.Fatal("first empty observation must start an idle window")
		}
		sqlDynamicAge(t, d, p.Queue)
		expired, err = d.ExpireDynamicQueues(ctx)
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(expired, []string{p.Queue}) {
			t.Fatalf("expected expiration: %v", expired)
		}
		queues, err := d.Queues(ctx)
		sqlTestCheck(t, err)
		if len(queues) != 0 || sqlDynamicPolicy(t, d, p.Queue).Active {
			t.Fatal("inactive history-only queue still listed")
		}
		metrics, err := d.GetMetrics(ctx, "")
		sqlTestCheck(t, err)
		if metrics[p.Queue].(map[string]int)[Failed] != 1 {
			t.Fatal("expiry hid retained history from global metrics")
		}
		throughput, err := d.RecentThroughput(ctx, 60, p.Queue)
		sqlTestCheck(t, err)
		if throughput[Failed] != 1 {
			t.Fatal("expiry deleted metrics")
		}
		stored, err := d.GetJob(ctx, p.ID)
		sqlTestCheck(t, err)
		wantHistory := append([]Attempt(nil), stored.History...)
		stored.Queue, stored.MaxAttempts = "stale-client-queue", 999
		sqlTestCheck(t, d.Release(ctx, stored, 3600))
		if stored.Queue != p.Queue || stored.MaxAttempts != 7 || !reflect.DeepEqual(stored.History, wantHistory) {
			t.Fatalf("retry changed stored policy/history: %+v", stored)
		}
		info := sqlDynamicPolicy(t, d, p.Queue)
		if !info.Active || info.IdleSince != nil || info.IdleTimeoutSeconds != 30 {
			t.Fatalf("manual retry did not reactivate policy: %+v", info)
		}
		if err = d.Release(ctx, claimed, 0); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale lifecycle write accepted: %v", err)
		}
		sqlTestCheck(t, d.Delete(ctx, p.ID))
		sqlDynamicAge(t, d, p.Queue)
		_, err = d.ExpireDynamicQueues(ctx)
		sqlTestCheck(t, err)
		db, err := d.sqlDB()
		sqlTestCheck(t, err)
		var count int
		sqlTestCheck(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM baqueue_go_dynamic_queues").Scan(&count))
		if count != 0 {
			t.Fatal("unretained policy was not garbage collected")
		}
		_, err = d.PushDynamic(ctx, nil, p.Queue, 60)
		sqlTestCheck(t, err)
		if sqlDynamicPolicy(t, d, p.Queue).IdleTimeoutSeconds != 60 {
			t.Fatal("garbage-collected definition was not recreated")
		}
	})

	for _, operation := range []string{"PushDynamic", "PushMany", "Release", "Register"} {
		t.Run("CrossConnectionExpiry"+operation, func(t *testing.T) {
			d := factory(t)
			peer := sqlTestPeer(t, d)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			for range 5 {
				sqlTestCheck(t, d.Flush(ctx, ""))
				_, err := d.RegisterDynamicQueue(ctx, "dynamic", 30)
				sqlTestCheck(t, err)
				p := NewPayload("job", nil)
				p.Queue = "dynamic"
				if operation == "PushMany" || operation == "Release" {
					p.Status = Failed
					_, err = d.Push(ctx, p)
					sqlTestCheck(t, err)
				}
				sqlDynamicAge(t, d, "dynamic")
				results := make(chan error, 2)
				start := make(chan struct{})
				go func() {
					<-start
					_, err := d.ExpireDynamicQueues(ctx)
					results <- err
				}()
				go func() {
					<-start
					var err error
					switch operation {
					case "PushDynamic":
						_, err = peer.PushDynamic(ctx, []*JobPayload{p}, "dynamic", 30)
					case "PushMany":
						next := NewPayload("job", nil)
						next.Queue = "dynamic"
						_, err = peer.PushMany(ctx, []*JobPayload{next})
					case "Release":
						err = peer.Release(ctx, p, 600)
					case "Register":
						_, err = peer.RegisterDynamicQueue(ctx, "dynamic", 30)
					}
					results <- err
				}()
				close(start)
				for range 2 {
					sqlTestCheck(t, <-results)
				}
				info := sqlDynamicPolicy(t, d, "dynamic")
				if !info.Active || (operation != "Register" && info.IdleSince != nil) {
					t.Fatalf("expiry race orphaned queue: %+v", info)
				}
				if operation != "Register" {
					n, err := d.Size(ctx, "dynamic")
					sqlTestCheck(t, err)
					if n != 1 {
						t.Fatalf("race lost pending work: %d", n)
					}
				}
			}
		})
	}

	t.Run("MultiQueueLockOrder", func(t *testing.T) {
		d := factory(t)
		peer := sqlTestPeer(t, d)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		for _, name := range []string{"a", "z"} {
			_, err := d.RegisterDynamicQueue(ctx, name, 30)
			sqlTestCheck(t, err)
		}
		results := make(chan error, 2)
		for i, names := range [][]string{{"a", "z"}, {"z", "a"}} {
			writer := []*SQLDriver{d, peer}[i]
			go func() {
				payloads := make([]*JobPayload, 0, len(names))
				for _, name := range names {
					p := NewPayload("job", nil)
					p.Queue = name
					payloads = append(payloads, p)
				}
				_, err := writer.PushMany(ctx, payloads)
				results <- err
			}()
		}
		for range 2 {
			sqlTestCheck(t, <-results)
		}
	})

	t.Run("FlushAndAdditiveUpgrade", func(t *testing.T) {
		d, ctx := factory(t), t.Context()
		ordinary := NewPayload("job", nil)
		_, err := d.Push(ctx, ordinary)
		sqlTestCheck(t, err)
		db, err := d.sqlDB()
		sqlTestCheck(t, err)
		_, err = db.ExecContext(ctx, "DROP TABLE baqueue_go_dynamic_queues")
		sqlTestCheck(t, err)
		sqlTestCheck(t, d.Disconnect(ctx))
		sqlTestCheck(t, d.Connect(ctx))
		stored, err := d.GetJob(ctx, ordinary.ID)
		sqlTestCheck(t, err)
		if stored == nil {
			t.Fatal("schema upgrade lost an existing job")
		}
		sqlDynamicExpiredJob(t, d, "expired")
		_, err = d.RegisterDynamicQueue(ctx, "empty", 30)
		sqlTestCheck(t, err)
		sqlTestCheck(t, d.Flush(ctx, "expired"))
		queues, err := d.Queues(ctx)
		sqlTestCheck(t, err)
		if !reflect.DeepEqual(queues, []string{"default", "empty"}) {
			t.Fatalf("queue flush changed unrelated queues: %v", queues)
		}
		_, err = d.RegisterDynamicQueue(ctx, "expired", 60)
		sqlTestCheck(t, err)
		sqlTestCheck(t, d.Flush(ctx, ""))
		infos, err := d.GetDynamicQueues(ctx)
		sqlTestCheck(t, err)
		if len(infos) != 0 {
			t.Fatal("full flush retained empty definitions")
		}
	})
}
