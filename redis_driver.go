package baqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var redisStatuses = []string{Pending, Processing, Completed, Failed, Cancelled}
var errRedisPayload = errors.New("baqueue: invalid Redis payload")

// RedisDriver uses the Python driver's hashes, lists and sorted-set indexes.
// Configure the exported options before Connect or concurrent use. Redis must
// retain queue keys (use noeviction); this driver targets standalone Redis.
// Concurrent external key-type/schema changes are unsupported. Type preflight
// detects existing corruption, but is not a lock against out-of-band tampering.
type RedisDriver struct {
	AutoCleanupOnDiskFull bool
	ReconcileOnConnect    bool

	prefix   string
	options  *redis.Options
	mu       sync.Mutex
	redis    *redis.Client
	closed   bool
	cleaning atomic.Bool
}

var _ Driver = (*RedisDriver)(nil)

func NewRedisDriver(url, prefix string) (*RedisDriver, error) {
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	if prefix == "" {
		prefix = "baqueue"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("baqueue: invalid Redis URL: %w", err)
	}
	// Replaying an EXEC after a lost reply could duplicate non-idempotent writes.
	options.MaxRetries = -1
	options.ContextTimeoutEnabled = true
	clientOptions := *options
	return &RedisDriver{prefix: prefix, options: options, redis: redis.NewClient(&clientOptions), AutoCleanupOnDiskFull: true}, nil
}

func (d *RedisDriver) client() *redis.Client {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.redis
}

func (d *RedisDriver) key(parts ...string) string {
	return d.prefix + ":" + strings.Join(parts, ":")
}

func (d *RedisDriver) index(queue, status string) string {
	if queue != "" && status != "" {
		return d.key("jobs", "queue", queue, "status", status)
	}
	if queue != "" {
		return d.key("jobs", "queue", queue)
	}
	if status != "" {
		return d.key("jobs", "status", status)
	}
	return d.key("jobs", "all")
}

func (d *RedisDriver) Connect(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		options := *d.options
		d.redis = redis.NewClient(&options)
		d.closed = false
	}
	d.mu.Unlock()
	if err := d.client().Ping(ctx).Err(); err != nil {
		return err
	}
	n, err := d.client().Exists(ctx, d.index("", "")).Result()
	if err != nil {
		return err
	}
	if n == 0 || d.ReconcileOnConnect {
		_, err = d.ReconcileIndexes(ctx, 500)
	}
	return err
}

func (d *RedisDriver) Disconnect(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.redis.Close()
}

func (d *RedisDriver) IsStorageFullError(err error) bool {
	var unsafe *redisUnsafeExecError
	if errors.As(err, &unsafe) {
		return false
	}
	var server redis.Error
	if !errors.As(err, &server) {
		return false
	}
	message := strings.ToLower(server.Error())
	return strings.Contains(message, "oom") || strings.Contains(message, "out of memory") ||
		strings.Contains(message, "maxmemory") || strings.Contains(message, "no space")
}

type redisUnsafeExecError struct{ err error }

func (e *redisUnsafeExecError) Error() string {
	return "baqueue: Redis EXEC may have partially committed; not retrying: " + e.err.Error()
}
func (e *redisUnsafeExecError) Unwrap() error { return e.err }

func (d *RedisDriver) storageRejected(ctx context.Context, err error) bool {
	if d.IsStorageFullError(err) {
		return true
	}
	var unsafe *redisUnsafeExecError
	var server redis.Error
	if errors.As(err, &unsafe) || !errors.As(err, &server) || !strings.HasPrefix(server.Error(), "EXECABORT ") {
		return false
	}
	// go-redis discards the QUEUED OOM reply and exposes only EXECABORT.
	// Confirm memory pressure before treating this non-committed EXEC as OOM.
	info, infoErr := d.client().Info(ctx, "memory").Result()
	if infoErr != nil {
		return false
	}
	values := map[string]int64{}
	for _, line := range strings.Split(info, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && (key == "used_memory" || key == "maxmemory") {
			values[key], _ = strconv.ParseInt(value, 10, 64)
		}
	}
	return values["maxmemory"] > 0 && values["used_memory"] >= values["maxmemory"]
}

func (d *RedisDriver) write(ctx context.Context, fn func() error) error {
	err := fn()
	if err == nil || !d.AutoCleanupOnDiskFull || !d.storageRejected(ctx, err) || !d.cleaning.CompareAndSwap(false, true) {
		return err
	}
	defer d.cleaning.Store(false)
	// Cleanup failures do not cause recursion or mask the final write error.
	_, _ = d.PruneTerminalJobs(ctx, PruneOptions{})
	_, _ = d.PruneMetrics(ctx, 0)
	return fn()
}

func (d *RedisDriver) watch(ctx context.Context, keys []string, fn func(*redis.Tx) error) error {
	return d.write(ctx, func() error {
		for attempt := 0; attempt < 128; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			err := d.client().Watch(ctx, fn, keys...)
			if !errors.Is(err, redis.TxFailedErr) {
				return err
			}
			timer := time.NewTimer(time.Duration(1+attempt%5) * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		return fmt.Errorf("baqueue: Redis transaction contention: %w", ErrConflict)
	})
}

// A plan checks key types before EXEC: Redis transactions do not roll back a
// runtime WRONGTYPE error. Shared indexes are not WATCHed solely for type checks.
type redisPlan struct {
	commands [][]any
	types    map[string]string
}

func (p *redisPlan) add(command, key string, args ...any) {
	if p.types == nil {
		p.types = make(map[string]string)
	}
	switch command {
	case "HSET":
		p.types[key] = "hash"
	case "LREM", "RPUSH", "LTRIM":
		p.types[key] = "list"
	case "ZADD", "ZREM":
		p.types[key] = "zset"
	case "SADD", "SREM":
		p.types[key] = "set"
	}
	p.commands = append(p.commands, append([]any{command, key}, args...))
}

func (p *redisPlan) commit(ctx context.Context, tx *redis.Tx) error {
	if len(p.commands) == 0 {
		return nil
	}
	checks := make(map[string]*redis.StatusCmd, len(p.types))
	_, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for key := range p.types {
			checks[key] = pipe.Type(ctx, key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for key, check := range checks {
		if got := check.Val(); got != "none" && got != p.types[key] {
			return fmt.Errorf("baqueue: Redis key %q has type %s, want %s", key, got, p.types[key])
		}
	}
	commands, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, command := range p.commands {
			pipe.Do(ctx, command...)
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.TxFailedErr) {
		for _, command := range commands {
			if command.Err() == nil {
				return &redisUnsafeExecError{err: err}
			}
		}
	}
	return err
}

func (d *RedisDriver) addIndexes(plan *redisPlan, job *JobPayload) {
	for _, key := range []string{d.index("", ""), d.index(job.Queue, ""), d.index("", job.Status), d.index(job.Queue, job.Status)} {
		plan.add("ZADD", key, job.CreatedAt, job.ID)
	}
}

func (d *RedisDriver) removeIndexes(plan *redisPlan, id, queue string) {
	plan.add("ZREM", d.index("", ""), id)
	for _, status := range redisStatuses {
		plan.add("ZREM", d.index("", status), id)
		if queue != "" {
			plan.add("ZREM", d.index(queue, status), id)
		}
	}
	if queue != "" {
		plan.add("ZREM", d.index(queue, ""), id)
	}
}

func (d *RedisDriver) save(ctx context.Context, tx *redis.Tx, plan *redisPlan, job *JobPayload) error {
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err = d.saveQueueMembership(ctx, tx, plan, job.Queue, job.Status == Pending || job.Status == Processing); err != nil {
		return err
	}
	plan.add("HSET", d.key("job", job.ID), "data", string(raw))
	d.removeIndexes(plan, job.ID, job.Queue)
	d.addIndexes(plan, job)
	return nil
}

func (d *RedisDriver) ready(plan *redisPlan, job *JobPayload) {
	plan.add("LREM", d.key("queue", job.Queue), 0, job.ID)
	plan.add("ZREM", d.key("delayed"), job.ID)
	if job.Status != Pending {
		return
	}
	if job.DelayUntil != nil && *job.DelayUntil > Now() {
		plan.add("ZADD", d.key("delayed"), *job.DelayUntil, job.ID)
	} else {
		plan.add("RPUSH", d.key("queue", job.Queue), job.ID)
	}
}

func redisReadJob(ctx context.Context, client redis.Cmdable, key, id string) (*JobPayload, error) {
	raw, err := client.HGet(ctx, key, "data").Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var job JobPayload
	if err = json.Unmarshal([]byte(raw), &job); err != nil {
		return nil, fmt.Errorf("%w %q: %v", errRedisPayload, id, err)
	}
	if job.ID != id || job.Queue == "" || strings.Contains(job.Queue, ":status:") || !slices.Contains(redisStatuses, job.Status) {
		return nil, fmt.Errorf("%w identity or state for %q", errRedisPayload, id)
	}
	return &job, nil
}

func (d *RedisDriver) GetJob(ctx context.Context, id string) (*JobPayload, error) {
	return redisReadJob(ctx, d.client(), d.key("job", id), id)
}

func (d *RedisDriver) Push(ctx context.Context, payload *JobPayload) (string, error) {
	ids, err := d.PushMany(ctx, []*JobPayload{payload})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func (d *RedisDriver) PushMany(ctx context.Context, payloads []*JobPayload) ([]string, error) {
	return d.pushMany(ctx, payloads, nil)
}

func (d *RedisDriver) pushMany(ctx context.Context, payloads []*JobPayload, dynamic *DynamicQueueInfo) ([]string, error) {
	ids := make([]string, 0, len(payloads))
	if len(payloads) == 0 && dynamic == nil {
		return ids, ctx.Err()
	}
	jobs := make([]*JobPayload, 0, len(payloads))
	keys := make([]string, 0, len(payloads))
	seen := make(map[string]bool)
	for _, payload := range payloads {
		if err := payload.Validate(); err != nil {
			return nil, err
		}
		if seen[payload.ID] {
			return nil, ErrDuplicate
		}
		seen[payload.ID] = true
		job := payload.Clone()
		job.Status, job.UpdatedAt = Pending, Now()
		if job.DelayUntil != nil && *job.DelayUntil <= job.UpdatedAt {
			job.DelayUntil = nil
		}
		jobs = append(jobs, job)
		ids = append(ids, job.ID)
		keys = append(keys, d.key("job", job.ID))
	}
	if dynamic != nil {
		keys = append(keys, d.dynamicKeys(dynamic.Name)...)
	}
	err := d.watch(ctx, keys, func(tx *redis.Tx) error {
		for _, job := range jobs {
			n, err := tx.Exists(ctx, d.key("job", job.ID)).Result()
			if err != nil {
				return err
			}
			if n != 0 {
				return ErrDuplicate
			}
		}
		plan := &redisPlan{}
		if dynamic != nil {
			info, err := d.registeredDynamicQueue(ctx, tx, dynamic.Name, dynamic.IdleTimeoutSeconds)
			if err != nil {
				return err
			}
			if len(jobs) != 0 {
				info.IdleSince = nil
			}
			if err = d.storeDynamicQueue(plan, info); err != nil {
				return err
			}
		}
		for _, job := range jobs {
			if err := d.save(ctx, tx, plan, job); err != nil {
				return err
			}
			d.ready(plan, job)
		}
		return plan.commit(ctx, tx)
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (d *RedisDriver) Pop(ctx context.Context, queue string) (*JobPayload, error) {
	if strings.Contains(queue, ":status:") {
		return nil, errors.New("baqueue: queue names cannot contain :status:")
	}
	queueKey := d.key("queue", queue)
	for stale := 0; stale < 1000; stale++ {
		var claimed *JobPayload
		var found bool
		err := d.watch(ctx, []string{queueKey}, func(tx *redis.Tx) error {
			claimed, found = nil, false
			id, err := tx.LIndex(ctx, queueKey, 0).Result()
			if errors.Is(err, redis.Nil) {
				return nil
			}
			if err != nil {
				return err
			}
			found = true
			jobKey := d.key("job", id)
			if err = tx.Watch(ctx, jobKey).Err(); err != nil {
				return err
			}
			job, err := redisReadJob(ctx, tx, jobKey, id)
			if err != nil && !errors.Is(err, errRedisPayload) {
				return err
			}
			plan := &redisPlan{}
			plan.add("LREM", queueKey, 0, id)
			if errors.Is(err, errRedisPayload) {
				refs, err := d.references(ctx, 500)
				if err != nil {
					return err
				}
				d.removeReferences(plan, id, refs)
				return plan.commit(ctx, tx)
			}
			if job == nil {
				plan.add("ZREM", d.key("delayed"), id)
				d.removeIndexes(plan, id, queue)
				return plan.commit(ctx, tx)
			}
			if job.Queue != queue {
				// Repair a misplaced reference without claiming another queue's job.
				d.ready(plan, job)
			} else if job.Status != Pending {
				plan.add("ZREM", d.key("delayed"), id)
			} else if job.DelayUntil != nil && *job.DelayUntil > Now() {
				plan.add("ZADD", d.key("delayed"), *job.DelayUntil, id)
			} else {
				now := Now()
				job.Status, job.StartedAt, job.UpdatedAt = Processing, &now, now
				job.DelayUntil = nil
				job.Attempts++
				plan.add("ZREM", d.key("delayed"), id)
				claimed = job
			}
			// Keep status persistence ahead of destructive membership changes.
			saved := &redisPlan{}
			if err = d.save(ctx, tx, saved, job); err != nil {
				return err
			}
			saved.commands = append(saved.commands, plan.commands...)
			for key, kind := range plan.types {
				saved.types[key] = kind
			}
			return saved.commit(ctx, tx)
		})
		if err != nil {
			return nil, err
		}
		if claimed != nil || !found {
			return claimed, nil
		}
	}
	return nil, nil
}

func (d *RedisDriver) transition(ctx context.Context, incoming *JobPayload, status, message string, delay float64) error {
	if incoming == nil {
		return errors.New("baqueue: nil payload")
	}
	if delay < 0 || math.IsNaN(delay) || math.IsInf(delay, 0) {
		return errors.New("baqueue: invalid delay")
	}
	key := d.key("job", incoming.ID)
	var result *JobPayload
	err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		result = nil
		job, err := redisReadJob(ctx, tx, key, incoming.ID)
		if err != nil {
			return err
		}
		if err = MergeAttempt(job, incoming); err != nil {
			return err
		}
		if status != Pending && job.Status != Processing {
			return ErrConflict
		}
		now := Now()
		job.Status, job.UpdatedAt = status, now
		switch status {
		case Completed:
			job.CompletedAt, job.Error = &now, nil
		case Failed:
			job.FailedAt, job.Error = &now, &message
		case Pending:
			job.StartedAt, job.CompletedAt, job.FailedAt, job.DelayUntil, job.Error = nil, nil, nil, nil, nil
			if delay > 0 {
				until := now + delay
				job.DelayUntil = &until
			}
		}
		plan := &redisPlan{}
		if err = d.save(ctx, tx, plan, job); err != nil {
			return err
		}
		if job.BatchID != nil && (status == Completed || status == Failed) {
			batchKey := d.key("batch", *job.BatchID)
			if err = tx.Watch(ctx, batchKey).Err(); err != nil {
				return err
			}
			batch, err := redisReadMap(ctx, tx, batchKey)
			if err != nil {
				return err
			}
			if batch != nil && ApplyBatchOutcome(batch, job) {
				raw, err := json.Marshal(batch)
				if err != nil {
					return err
				}
				plan.add("HSET", batchKey, "data", string(raw))
			}
		}
		d.ready(plan, job)
		if err = plan.commit(ctx, tx); err == nil {
			result = job
		}
		return err
	})
	if err == nil && result != nil {
		*incoming = *result
	}
	return err
}

func (d *RedisDriver) Complete(ctx context.Context, job *JobPayload) error {
	return d.transition(ctx, job, Completed, "", 0)
}

func (d *RedisDriver) Fail(ctx context.Context, job *JobPayload, message string) error {
	return d.transition(ctx, job, Failed, message, 0)
}

func (d *RedisDriver) Release(ctx context.Context, job *JobPayload, delay float64) error {
	return d.transition(ctx, job, Pending, "", delay)
}

func (d *RedisDriver) promote(ctx context.Context, id string, force bool, now float64) (*JobPayload, error) {
	var promoted *JobPayload
	key := d.key("job", id)
	err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		promoted = nil
		job, err := redisReadJob(ctx, tx, key, id)
		if err != nil && !errors.Is(err, errRedisPayload) {
			return err
		}
		plan := &redisPlan{}
		if errors.Is(err, errRedisPayload) {
			refs, err := d.references(ctx, 500)
			if err != nil {
				return err
			}
			d.removeReferences(plan, id, refs)
			return plan.commit(ctx, tx)
		}
		if job == nil || job.Status != Pending {
			plan.add("ZREM", d.key("delayed"), id)
			return plan.commit(ctx, tx)
		}
		if !force && job.DelayUntil != nil && *job.DelayUntil > now {
			plan.add("ZADD", d.key("delayed"), *job.DelayUntil, id)
			return plan.commit(ctx, tx)
		}
		if !force && job.DelayUntil == nil {
			plan.add("ZREM", d.key("delayed"), id)
			return plan.commit(ctx, tx)
		}
		if err = d.saveQueueMembership(ctx, tx, plan, job.Queue, true); err != nil {
			return err
		}
		if force && job.DelayUntil == nil {
			queueKey := d.key("queue", job.Queue)
			if err = tx.Watch(ctx, queueKey).Err(); err != nil {
				return err
			}
			positions, err := tx.LPosCount(ctx, queueKey, id, 2, redis.LPosArgs{}).Result()
			if err != nil {
				return err
			}
			if len(positions) == 1 {
				plan.add("ZREM", d.key("delayed"), id)
				if err = plan.commit(ctx, tx); err == nil {
					promoted = job
				}
				return err
			}
		}
		job.DelayUntil, job.UpdatedAt = nil, now
		if err = d.save(ctx, tx, plan, job); err != nil {
			return err
		}
		d.ready(plan, job)
		if err = plan.commit(ctx, tx); err == nil {
			promoted = job
		}
		return err
	})
	return promoted, err
}

func (d *RedisDriver) Promote(ctx context.Context, id string) (bool, error) {
	job, err := d.promote(ctx, id, true, Now())
	return job != nil, err
}

func (d *RedisDriver) PopDelayed(ctx context.Context) ([]*JobPayload, error) {
	now := Now()
	ids, err := d.client().ZRangeByScore(ctx, d.key("delayed"), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatFloat(now, 'f', -1, 64)}).Result()
	if err != nil {
		return nil, err
	}
	jobs := make([]*JobPayload, 0, len(ids))
	for _, id := range ids {
		job, err := d.promote(ctx, id, false, now)
		if err != nil {
			return jobs, err
		}
		if job != nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (d *RedisDriver) RequeueStuckJobs(ctx context.Context, age float64, queue string) (int, error) {
	ids, err := d.client().ZRange(ctx, d.index(queue, Processing), 0, -1).Result()
	if err != nil {
		return 0, err
	}
	cutoff, count := Now()-age, 0
	for _, id := range ids {
		changed := false
		key := d.key("job", id)
		err = d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
			changed = false
			job, err := redisReadJob(ctx, tx, key, id)
			if err != nil || job == nil {
				return err
			}
			started := job.UpdatedAt
			if job.StartedAt != nil {
				started = *job.StartedAt
			}
			if job.Status != Processing || (queue != "" && job.Queue != queue) || started > cutoff {
				return nil
			}
			job.Status, job.StartedAt, job.DelayUntil, job.UpdatedAt = Pending, nil, nil, Now()
			plan := &redisPlan{}
			if err = d.save(ctx, tx, plan, job); err != nil {
				return err
			}
			d.ready(plan, job)
			if err = plan.commit(ctx, tx); err == nil {
				changed = true
			}
			return err
		})
		if err != nil {
			return count, err
		}
		if changed {
			count++
		}
	}
	return count, nil
}

func redisMatches(job *JobPayload, filter JobFilter) bool {
	return (filter.Queue == "" || job.Queue == filter.Queue) &&
		(filter.Status == "" || job.Status == filter.Status) &&
		(filter.Tag == "" || slices.Contains(job.Tags, filter.Tag)) &&
		(filter.BatchID == "" || (job.BatchID != nil && *job.BatchID == filter.BatchID)) &&
		(filter.CreatedFrom == nil || job.CreatedAt >= *filter.CreatedFrom) &&
		(filter.CreatedTo == nil || job.CreatedAt <= *filter.CreatedTo)
}

func (d *RedisDriver) GetJobs(ctx context.Context, filter JobFilter) ([]*JobPayload, error) {
	query := &redis.ZRangeBy{Min: "-inf", Max: "+inf", Count: 256}
	if filter.CreatedFrom != nil {
		query.Min = strconv.FormatFloat(*filter.CreatedFrom, 'f', -1, 64)
	}
	if filter.CreatedTo != nil {
		query.Max = strconv.FormatFloat(*filter.CreatedTo, 'f', -1, 64)
	}
	limit := filter.Limit
	if limit == 0 {
		limit = 50
	}
	offset := max(0, filter.Offset)
	jobs := []*JobPayload{}
	for {
		ids, err := d.client().ZRevRangeByScore(ctx, d.index(filter.Queue, filter.Status), query).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			job, err := d.GetJob(ctx, id)
			if err != nil {
				return nil, err
			}
			if job == nil || !redisMatches(job, filter) {
				continue
			}
			if offset > 0 {
				offset--
				continue
			}
			jobs = append(jobs, job)
			if limit > 0 && len(jobs) >= limit {
				return jobs, nil
			}
		}
		if len(ids) < int(query.Count) {
			return jobs, nil
		}
		query.Offset += int64(len(ids))
	}
}

func (d *RedisDriver) CountJobs(ctx context.Context, filter JobFilter) (int, error) {
	filter.Offset, filter.Limit = 0, -1
	jobs, err := d.GetJobs(ctx, filter)
	return len(jobs), err
}

func (d *RedisDriver) Size(ctx context.Context, queue string) (int, error) {
	n, err := d.client().LLen(ctx, d.key("queue", queue)).Result()
	return int(n), err
}

func (d *RedisDriver) Queues(ctx context.Context) ([]string, error) {
	names, err := d.allQueueNames(ctx)
	if err != nil {
		return nil, err
	}
	queues := make([]string, 0, len(names))
	for _, name := range names {
		info, err := d.readDynamicQueue(ctx, d.client(), name)
		if err != nil {
			return nil, err
		}
		if info == nil || info.Active {
			queues = append(queues, name)
		}
	}
	return queues, nil
}

type redisMetric struct {
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
	Time   float64 `json:"time"`
}

func (d *RedisDriver) RecordMetric(ctx context.Context, queue, metric string, value float64) error {
	raw, err := json.Marshal(redisMetric{Metric: metric, Value: value, Time: Now()})
	if err != nil {
		return err
	}
	key := d.key("metrics", queue)
	return d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		plan := &redisPlan{}
		plan.add("RPUSH", key, string(raw))
		plan.add("LTRIM", key, -10000, -1)
		return plan.commit(ctx, tx)
	})
}

func (d *RedisDriver) GetMetrics(ctx context.Context, queue string) (map[string]any, error) {
	queues := []string{queue}
	if queue == "" {
		var err error
		queues, err = d.allQueueNames(ctx)
		if err != nil {
			return nil, err
		}
	}
	out := make(map[string]any, len(queues))
	for _, name := range queues {
		counts := map[string]int{}
		for _, status := range []string{Pending, Processing, Completed, Failed} {
			n, err := d.client().ZCard(ctx, d.index(name, status)).Result()
			if err != nil {
				return nil, err
			}
			counts[status] = int(n)
		}
		out[name] = counts
	}
	return out, nil
}

func (d *RedisDriver) RecentThroughput(ctx context.Context, seconds int, queue string) (map[string]int, error) {
	keys := []string{d.key("metrics", queue)}
	if queue == "" {
		var err error
		keys, err = d.scanKeys(ctx, "metrics:", 200)
		if err != nil {
			return nil, err
		}
	}
	out := map[string]int{Processing: 0, Completed: 0, Failed: 0}
	cutoff := Now() - float64(seconds)
	for _, key := range keys {
		entries, err := d.client().LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, raw := range entries {
			var entry redisMetric
			if json.Unmarshal([]byte(raw), &entry) != nil || entry.Time < cutoff {
				continue
			}
			if _, ok := out[entry.Metric]; ok {
				out[entry.Metric]++
			}
		}
	}
	return out, nil
}

func (d *RedisDriver) ReportSupervisor(ctx context.Context, stats map[string]any) error {
	name, _ := stats["name"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("baqueue: supervisor name is required")
	}
	raw, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	key := d.key("supervisor", name)
	return d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		plan := &redisPlan{}
		plan.add("HSET", key, "data", string(raw), "heartbeat_at", Now())
		plan.add("SADD", d.key("supervisors"), name)
		return plan.commit(ctx, tx)
	})
}

func (d *RedisDriver) GetSupervisorStats(ctx context.Context, staleAfter float64) ([]map[string]any, error) {
	names, err := d.client().SMembers(ctx, d.key("supervisors")).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	out := []map[string]any{}
	cutoff := Now() - staleAfter
	for _, name := range names {
		row, err := d.client().HGetAll(ctx, d.key("supervisor", name)).Result()
		if err != nil {
			return nil, err
		}
		heartbeat, err := strconv.ParseFloat(row["heartbeat_at"], 64)
		if err != nil || heartbeat < cutoff || math.IsNaN(heartbeat) {
			continue
		}
		var data map[string]any
		if json.Unmarshal([]byte(row["data"]), &data) == nil && data["running"] == true {
			out = append(out, data)
		}
	}
	return out, nil
}

func (d *RedisDriver) StoreBatch(ctx context.Context, id string, data map[string]any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return d.write(ctx, func() error { return d.client().HSet(ctx, d.key("batch", id), "data", string(raw)).Err() })
}

func redisReadMap(ctx context.Context, client redis.Cmdable, key string) (map[string]any, error) {
	raw, err := client.HGet(ctx, key, "data").Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var data map[string]any
	err = json.Unmarshal([]byte(raw), &data)
	return data, err
}

func (d *RedisDriver) GetBatch(ctx context.Context, id string) (map[string]any, error) {
	return redisReadMap(ctx, d.client(), d.key("batch", id))
}

func (d *RedisDriver) GetPendingBatches(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	result := []map[string]any{}
	seen := map[string]bool{}
	var cursor uint64
	for {
		keys, next, err := d.client().Scan(ctx, cursor, redisGlobLiteral(d.key("batch", ""))+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if seen[key] {
				continue
			}
			seen[key] = true
			raw, err := d.client().HGet(ctx, key, "data").Result()
			if errors.Is(err, redis.Nil) {
				continue
			}
			if err != nil {
				return nil, err
			}
			var batch map[string]any
			if json.Unmarshal([]byte(raw), &batch) != nil || !BatchNeedsReconciliation(batch) {
				continue
			}
			result = append(result, batch)
			if len(result) >= limit {
				return result, nil
			}
		}
		cursor = next
		if cursor == 0 {
			return result, nil
		}
	}
}

func (d *RedisDriver) UpdateBatch(ctx context.Context, id string, patch map[string]any) error {
	key := d.key("batch", id)
	return d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		data, err := redisReadMap(ctx, tx, key)
		if err != nil {
			return err
		}
		if data == nil {
			return ErrNotFound
		}
		for k, v := range patch {
			data[k] = v
		}
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		plan := &redisPlan{}
		plan.add("HSET", key, "data", string(raw))
		return plan.commit(ctx, tx)
	})
}

func (d *RedisDriver) IncrementBatchCounter(ctx context.Context, id, field string, delta int) (map[string]any, error) {
	key := d.key("batch", id)
	var result map[string]any
	err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		result = nil
		data, err := redisReadMap(ctx, tx, key)
		if err != nil || data == nil {
			return err
		}
		data[field] = valueNumber(data[field]) + float64(delta)
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		plan := &redisPlan{}
		plan.add("HSET", key, "data", string(raw))
		if err = plan.commit(ctx, tx); err == nil {
			result = data
		}
		return err
	})
	return result, err
}

func redisGlobLiteral(value string) string {
	return strings.NewReplacer("\\", "\\\\", "*", "\\*", "?", "\\?", "[", "\\[", "]", "\\]").Replace(value)
}

func (d *RedisDriver) scanKeys(ctx context.Context, family string, batch int) ([]string, error) {
	pattern := redisGlobLiteral(d.key(family)) + "*"
	seen := map[string]bool{}
	var cursor uint64
	for {
		keys, next, err := d.client().Scan(ctx, cursor, pattern, int64(max(1, batch))).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			seen[key] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

type redisReferences struct {
	queues  []string
	indexes []string
}

func (d *RedisDriver) references(ctx context.Context, batch int) (redisReferences, error) {
	var refs redisReferences
	var err error
	refs.queues, err = d.scanKeys(ctx, "queue:", batch)
	if err == nil {
		refs.indexes, err = d.scanKeys(ctx, "jobs:", batch)
	}
	return refs, err
}

func (d *RedisDriver) removeReferences(plan *redisPlan, id string, refs redisReferences) {
	for _, key := range refs.queues {
		plan.add("LREM", key, 0, id)
	}
	for _, key := range refs.indexes {
		plan.add("ZREM", key, id)
	}
	plan.add("ZREM", d.key("delayed"), id)
}

func (d *RedisDriver) deleteMatching(ctx context.Context, id string, refs redisReferences, deleteMalformed bool, match func(*JobPayload) bool) (bool, error) {
	key := d.key("job", id)
	removed := false
	err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
		removed = false
		job, err := redisReadJob(ctx, tx, key, id)
		if errors.Is(err, errRedisPayload) {
			if !deleteMalformed {
				return nil
			}
		} else if err != nil {
			return err
		}
		if job != nil && !match(job) {
			return nil
		}
		plan := &redisPlan{}
		d.removeReferences(plan, id, refs)
		if job != nil {
			plan.add("LREM", d.key("queue", job.Queue), 0, id)
			d.removeIndexes(plan, id, job.Queue)
		}
		plan.add("DEL", key)
		if err = plan.commit(ctx, tx); err == nil {
			removed = true
		}
		return err
	})
	return removed, err
}

func (d *RedisDriver) Delete(ctx context.Context, id string) error {
	_, err := d.BulkDeleteJobs(ctx, []string{id}, 0)
	return err
}

func (d *RedisDriver) BulkDeleteJobs(ctx context.Context, ids []string, limit int) (int, error) {
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	refs, err := d.references(ctx, 500)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		removed, err := d.deleteMatching(ctx, id, refs, true, func(*JobPayload) bool { return true })
		if err != nil {
			return count, err
		}
		if removed {
			count++
		}
	}
	return count, nil
}

func (d *RedisDriver) prune(ctx context.Context, options PruneOptions, terminal bool) (int, error) {
	if err := options.Validate(); err != nil {
		return 0, err
	}
	refs, err := d.references(ctx, 500)
	if err != nil {
		return 0, err
	}
	ids, err := d.client().ZRange(ctx, d.index(options.Queue, options.Status), 0, -1).Result()
	if err != nil {
		return 0, err
	}
	cutoff := Now()
	if options.OlderThanSeconds != nil {
		cutoff -= *options.OlderThanSeconds
	}
	match := func(job *JobPayload) bool {
		return (!terminal || job.Status == Completed || job.Status == Failed || job.Status == Cancelled) &&
			redisMatches(job, JobFilter{Queue: options.Queue, Status: options.Status, Tag: options.Tag}) &&
			(options.OlderThanSeconds == nil || job.UpdatedAt <= cutoff)
	}
	count := 0
	for _, id := range ids {
		removed, err := d.deleteMatching(ctx, id, refs, false, match)
		if err != nil {
			return count, err
		}
		if removed {
			count++
		}
		if options.Limit > 0 && count >= options.Limit {
			break
		}
	}
	return count, nil
}

func (d *RedisDriver) Prune(ctx context.Context, options PruneOptions) (int, error) {
	return d.prune(ctx, options, false)
}

func (d *RedisDriver) PruneTerminalJobs(ctx context.Context, options PruneOptions) (int, error) {
	return d.prune(ctx, options, true)
}

func (d *RedisDriver) indexMembers(ctx context.Context, key string, batch int) ([]string, error) {
	seen := map[string]bool{}
	var cursor uint64
	for {
		members, next, err := d.client().ZScan(ctx, key, cursor, "*", int64(max(1, batch))).Result()
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(members); i += 2 {
			seen[members[i]] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

// ReconcileIndexes removes stale/wrong memberships and backfills missing ones.
// Its return value counts removed memberships, as in Python (not additions).
func (d *RedisDriver) ReconcileIndexes(ctx context.Context, batch int) (int, error) {
	indexes, err := d.scanKeys(ctx, "jobs:", batch)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, index := range indexes {
		ids, err := d.indexMembers(ctx, index, batch)
		if err != nil {
			return removed, err
		}
		for _, id := range ids {
			key := d.key("job", id)
			changed := false
			err := d.watch(ctx, []string{key, index}, func(tx *redis.Tx) error {
				changed = false
				job, err := redisReadJob(ctx, tx, key, id)
				if err != nil && !errors.Is(err, errRedisPayload) {
					return err
				}
				valid := job != nil && (index == d.index("", "") || index == d.index(job.Queue, "") ||
					index == d.index("", job.Status) || index == d.index(job.Queue, job.Status))
				plan := &redisPlan{}
				if valid {
					plan.add("ZADD", index, job.CreatedAt, id)
				} else {
					_, err = tx.ZScore(ctx, index, id).Result()
					if errors.Is(err, redis.Nil) {
						return nil
					}
					if err != nil {
						return err
					}
					plan.add("ZREM", index, id)
				}
				if err = plan.commit(ctx, tx); err == nil {
					changed = !valid
				}
				return err
			})
			if err != nil {
				return removed, err
			}
			if changed {
				removed++
			}
		}
	}
	keys, err := d.scanKeys(ctx, "job:", batch)
	if err != nil {
		return removed, err
	}
	for _, key := range keys {
		id := strings.TrimPrefix(key, d.key("job", ""))
		err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
			job, err := redisReadJob(ctx, tx, key, id)
			if errors.Is(err, errRedisPayload) {
				return nil
			}
			if err != nil || job == nil {
				return err
			}
			plan := &redisPlan{}
			if err = d.saveQueueMembership(ctx, tx, plan, job.Queue, job.Status == Pending || job.Status == Processing); err != nil {
				return err
			}
			d.addIndexes(plan, job)
			return plan.commit(ctx, tx)
		})
		if err != nil {
			return removed, err
		}
	}
	return removed, nil
}

func (d *RedisDriver) knownIDs(ctx context.Context, refs redisReferences, batch int) ([]string, error) {
	seen := map[string]bool{}
	keys, err := d.scanKeys(ctx, "job:", batch)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		seen[strings.TrimPrefix(key, d.key("job", ""))] = true
	}
	for _, queue := range refs.queues {
		ids, err := d.client().LRange(ctx, queue, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			seen[id] = true
		}
	}
	for _, index := range append(append([]string{}, refs.indexes...), d.key("delayed")) {
		ids, err := d.indexMembers(ctx, index, batch)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			seen[id] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// ReconcilePendingJobs also discovers hashes missing from every index. Malformed
// hashes are retained for inspection, but cannot remain runnable.
func (d *RedisDriver) ReconcilePendingJobs(ctx context.Context, batch int) (int, error) {
	refs, err := d.references(ctx, batch)
	if err != nil {
		return 0, err
	}
	ids, err := d.knownIDs(ctx, refs, batch)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		key := d.key("job", id)
		keys := append([]string{key, d.key("delayed")}, refs.queues...)
		err := d.watch(ctx, keys, func(tx *redis.Tx) error {
			job, err := redisReadJob(ctx, tx, key, id)
			if err != nil && !errors.Is(err, errRedisPayload) {
				return err
			}
			plan := &redisPlan{}
			if job == nil {
				d.removeReferences(plan, id, refs)
				return plan.commit(ctx, tx)
			}
			queueKey := d.key("queue", job.Queue)
			if err = tx.Watch(ctx, queueKey).Err(); err != nil {
				return err
			}
			positions, err := tx.LPosCount(ctx, queueKey, id, 2, redis.LPosArgs{}).Result()
			if err != nil {
				return err
			}
			_, delayedErr := tx.ZScore(ctx, d.key("delayed"), id).Result()
			if delayedErr != nil && !errors.Is(delayedErr, redis.Nil) {
				return delayedErr
			}
			for _, other := range refs.queues {
				if other != queueKey {
					plan.add("LREM", other, 0, id)
				}
			}
			for _, index := range refs.indexes {
				plan.add("ZREM", index, id)
			}
			now := Now()
			if job.Status == Pending && (job.DelayUntil == nil || *job.DelayUntil <= now) {
				if job.DelayUntil != nil {
					job.DelayUntil, job.UpdatedAt = nil, now
				}
				if len(positions) != 1 || delayedErr == nil {
					plan.add("LREM", queueKey, 0, id)
					plan.add("RPUSH", queueKey, id)
				}
				plan.add("ZREM", d.key("delayed"), id)
			} else {
				d.ready(plan, job)
			}
			if err = d.save(ctx, tx, plan, job); err != nil {
				return err
			}
			return plan.commit(ctx, tx)
		})
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (d *RedisDriver) PruneMetrics(ctx context.Context, age float64) (int, error) {
	keys, err := d.scanKeys(ctx, "metrics:", 200)
	if err != nil {
		return 0, err
	}
	cutoff, total := Now()-age, 0
	for _, key := range keys {
		removed := 0
		err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
			removed = 0
			entries, err := tx.LRange(ctx, key, 0, -1).Result()
			if err != nil {
				return err
			}
			// LREM removes only inspected records, never a concurrent append.
			plan := &redisPlan{}
			for _, raw := range entries {
				var entry redisMetric
				if json.Unmarshal([]byte(raw), &entry) != nil || entry.Time < cutoff {
					plan.add("LREM", key, 1, raw)
					removed++
				}
			}
			return plan.commit(ctx, tx)
		})
		if err != nil {
			return total, err
		}
		total += removed
	}
	return total, nil
}

func (d *RedisDriver) Flush(ctx context.Context, queue string) error {
	refs, err := d.references(ctx, 500)
	if err != nil {
		return err
	}
	ids, err := d.knownIDs(ctx, refs, 500)
	if err != nil {
		return err
	}
	for _, id := range ids {
		_, err = d.deleteMatching(ctx, id, refs, queue == "", func(job *JobPayload) bool { return queue == "" || job.Queue == queue })
		if err != nil {
			return err
		}
	}
	queues := []string{queue}
	if queue == "" {
		queues, err = d.allQueueNames(ctx)
		if err != nil {
			return err
		}
	}
	for _, name := range queues {
		index, list := d.index(name, ""), d.key("queue", name)
		err = d.watch(ctx, d.dynamicKeys(name), func(tx *redis.Tx) error {
			n, err := tx.ZCard(ctx, index).Result()
			if err != nil || n != 0 {
				return err
			}
			n, err = tx.LLen(ctx, list).Result()
			if err != nil || n != 0 {
				return err
			}
			plan := &redisPlan{}
			plan.add("SREM", d.key("queues"), name)
			plan.add("DEL", d.dynamicKey(name))
			plan.add("SREM", d.key("dynamic_queues"), name)
			return plan.commit(ctx, tx)
		})
		if err != nil {
			return err
		}
	}
	if queue == "" {
		for _, family := range []string{"metrics:", "batch:", "supervisor:"} {
			keys, err := d.scanKeys(ctx, family, 500)
			if err != nil {
				return err
			}
			for _, key := range keys {
				if err := d.write(ctx, func() error { return d.client().Del(ctx, key).Err() }); err != nil {
					return err
				}
			}
		}
		// Stale names are harmless and filtered by GetSupervisorStats. Deleting
		// the set wholesale could hide a concurrent supervisor heartbeat.
		names, err := d.client().SMembers(ctx, d.key("supervisors")).Result()
		if err != nil {
			return err
		}
		for _, name := range names {
			key := d.key("supervisor", name)
			if err := d.watch(ctx, []string{key}, func(tx *redis.Tx) error {
				n, err := tx.Exists(ctx, key).Result()
				if err != nil || n != 0 {
					return err
				}
				plan := &redisPlan{}
				plan.add("SREM", d.key("supervisors"), name)
				return plan.commit(ctx, tx)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
