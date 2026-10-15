package baqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
)

type memoryMetric struct {
	queue, name string
	value, at   float64
}
type memorySupervisor struct {
	data map[string]any
	at   float64
}

// MemoryDriver is safe for concurrent goroutines, but is not shared by processes.
type MemoryDriver struct {
	mu          sync.Mutex
	jobs        map[string]*JobPayload
	ready       map[string][]string
	batches     map[string]map[string]any
	metrics     []memoryMetric
	supervisors map[string]memorySupervisor
	dynamic     map[string]DynamicQueueInfo
}

func NewMemoryDriver() *MemoryDriver {
	return &MemoryDriver{jobs: map[string]*JobPayload{}, ready: map[string][]string{},
		batches: map[string]map[string]any{}, supervisors: map[string]memorySupervisor{}, dynamic: map[string]DynamicQueueInfo{}}
}

var _ Driver = (*MemoryDriver)(nil)

func (d *MemoryDriver) Connect(ctx context.Context) error { return ctx.Err() }
func (d *MemoryDriver) Disconnect(context.Context) error  { return nil }
func (d *MemoryDriver) Push(ctx context.Context, p *JobPayload) (string, error) {
	ids, err := d.PushMany(ctx, []*JobPayload{p})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}
func (d *MemoryDriver) PushMany(ctx context.Context, ps []*JobPayload) ([]string, error) {
	return d.pushMany(ctx, ps, nil)
}

func (d *MemoryDriver) pushMany(ctx context.Context, ps []*JobPayload, definition *DynamicQueueInfo) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, p := range ps {
		if err := p.Validate(); err != nil {
			return nil, err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := map[string]bool{}
	for _, p := range ps {
		if d.jobs[p.ID] != nil || seen[p.ID] {
			return nil, ErrDuplicate
		}
		seen[p.ID] = true
	}
	if definition != nil {
		if _, err := d.registerDynamic(definition.Name, definition.IdleTimeoutSeconds); err != nil {
			return nil, err
		}
	}
	ids := make([]string, 0, len(ps))
	for _, p := range ps {
		p = p.Clone()
		p.Status = Pending
		p.UpdatedAt = Now()
		d.jobs[p.ID] = p
		d.enqueue(p)
		ids = append(ids, p.ID)
	}
	return ids, nil
}
func (d *MemoryDriver) enqueue(p *JobPayload) {
	if p.Status == Pending {
		if info, ok := d.dynamic[p.Queue]; ok {
			info.Active, info.IdleSince = true, nil
			d.dynamic[p.Queue] = info
		}
	}
	d.removeReady(p.Queue, p.ID)
	if p.Status == Pending && (p.DelayUntil == nil || *p.DelayUntil <= Now()) {
		d.ready[p.Queue] = append(d.ready[p.Queue], p.ID)
	}
}
func (d *MemoryDriver) removeReady(q, id string) {
	d.ready[q] = slices.DeleteFunc(d.ready[q], func(s string) bool { return s == id })
}
func (d *MemoryDriver) Pop(ctx context.Context, q string) (*JobPayload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.ready[q]) > 0 {
		id := d.ready[q][0]
		d.ready[q] = d.ready[q][1:]
		p := d.jobs[id]
		if p == nil || p.Status != Pending || p.Queue != q {
			continue
		}
		now := Now()
		if p.DelayUntil != nil && *p.DelayUntil > now {
			continue
		}
		p.Status = Processing
		p.Attempts++
		p.StartedAt = &now
		p.UpdatedAt = now
		p.DelayUntil = nil
		return p.Clone(), nil
	}
	return nil, nil
}
func (d *MemoryDriver) PopDelayed(ctx context.Context) ([]*JobPayload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []*JobPayload{}
	for _, p := range d.jobs {
		if p.Status == Pending && p.DelayUntil != nil && *p.DelayUntil <= Now() {
			p.DelayUntil = nil
			p.UpdatedAt = Now()
			d.enqueue(p)
			out = append(out, p.Clone())
		}
	}
	return out, nil
}
func (d *MemoryDriver) transition(ctx context.Context, p *JobPayload, status, msg string, delay float64) error {
	if delay < 0 || math.IsNaN(delay) || math.IsInf(delay, 0) {
		return fmt.Errorf("baqueue: delay must be finite and nonnegative")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil {
		return ErrNotFound
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.jobs[p.ID]
	if err := MergeAttempt(s, p); err != nil {
		return err
	}
	if status != Pending && s.Status != Processing {
		return ErrConflict
	}
	now := Now()
	s.Status = status
	s.UpdatedAt = now
	d.removeReady(s.Queue, s.ID)
	switch status {
	case Completed:
		s.CompletedAt = &now
		s.Error = nil
		s.FailedAt = nil
		s.DelayUntil = nil
	case Failed:
		s.FailedAt = &now
		s.Error = &msg
		s.CompletedAt = nil
		s.DelayUntil = nil
	case Pending:
		s.StartedAt = nil
		s.CompletedAt = nil
		s.FailedAt = nil
		s.Error = nil
		s.DelayUntil = nil
		if delay > 0 {
			at := now + delay
			s.DelayUntil = &at
		}
		d.enqueue(s)
	}
	if s.BatchID != nil && (status == Completed || status == Failed) {
		ApplyBatchOutcome(d.batches[*s.BatchID], s)
	}
	*p = *s.Clone()
	return nil
}
func (d *MemoryDriver) Complete(ctx context.Context, p *JobPayload) error {
	return d.transition(ctx, p, Completed, "", 0)
}
func (d *MemoryDriver) Fail(ctx context.Context, p *JobPayload, msg string) error {
	return d.transition(ctx, p, Failed, msg, 0)
}
func (d *MemoryDriver) Release(ctx context.Context, p *JobPayload, delay float64) error {
	return d.transition(ctx, p, Pending, "", delay)
}
func (d *MemoryDriver) RequeueStuckJobs(ctx context.Context, age float64, q string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, p := range d.jobs {
		started := p.UpdatedAt
		if p.StartedAt != nil {
			started = *p.StartedAt
		}
		if p.Status == Processing && (q == "" || p.Queue == q) && started <= Now()-age {
			p.Status = Pending
			p.StartedAt = nil
			p.DelayUntil = nil
			p.UpdatedAt = Now()
			d.enqueue(p)
			n++
		}
	}
	return n, nil
}
func (d *MemoryDriver) delete(id string) {
	if p := d.jobs[id]; p != nil {
		d.removeReady(p.Queue, id)
		delete(d.jobs, id)
	}
}
func (d *MemoryDriver) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.delete(id)
	return nil
}
func (d *MemoryDriver) Promote(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p := d.jobs[id]
	if p == nil || p.Status != Pending {
		return false, nil
	}
	p.DelayUntil = nil
	p.UpdatedAt = Now()
	d.enqueue(p)
	return true, nil
}
func (d *MemoryDriver) GetJob(ctx context.Context, id string) (*JobPayload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.jobs[id].Clone(), nil
}
func memoryMatches(p *JobPayload, f JobFilter) bool {
	return (f.Queue == "" || p.Queue == f.Queue) && (f.Status == "" || p.Status == f.Status) && (f.Tag == "" || slices.Contains(p.Tags, f.Tag)) && (f.BatchID == "" || (p.BatchID != nil && *p.BatchID == f.BatchID)) && (f.CreatedFrom == nil || p.CreatedAt >= *f.CreatedFrom) && (f.CreatedTo == nil || p.CreatedAt <= *f.CreatedTo)
}
func (d *MemoryDriver) GetJobs(ctx context.Context, f JobFilter) ([]*JobPayload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []*JobPayload{}
	for _, p := range d.jobs {
		if memoryMatches(p, f) {
			out = append(out, p.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt == out[j].CreatedAt {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt > out[j].CreatedAt
	})
	offset := max(0, f.Offset)
	if offset >= len(out) {
		return []*JobPayload{}, nil
	}
	out = out[offset:]
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (d *MemoryDriver) CountJobs(ctx context.Context, f JobFilter) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, p := range d.jobs {
		if memoryMatches(p, f) {
			n++
		}
	}
	return n, nil
}
func (d *MemoryDriver) Size(ctx context.Context, q string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, id := range d.ready[q] {
		p := d.jobs[id]
		if p != nil && p.Status == Pending && (p.DelayUntil == nil || *p.DelayUntil <= Now()) {
			n++
		}
	}
	return n, nil
}
func (d *MemoryDriver) queues(q string) []string {
	if q != "" {
		return []string{q}
	}
	set := map[string]bool{}
	for _, p := range d.jobs {
		set[p.Queue] = true
	}
	for name := range d.ready {
		set[name] = true
	}
	out := []string{}
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
func (d *MemoryDriver) Queues(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	names := map[string]bool{}
	for _, name := range d.queues("") {
		names[name] = true
	}
	for name, info := range d.dynamic {
		if info.Active {
			names[name] = true
		} else {
			delete(names, name)
		}
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}
func (d *MemoryDriver) RecordMetric(ctx context.Context, q, m string, v float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metrics = append(d.metrics, memoryMetric{q, m, v, Now()})
	return nil
}
func (d *MemoryDriver) GetMetrics(ctx context.Context, q string) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]any{}
	for _, name := range d.queues(q) {
		counts := map[string]int{Pending: 0, Processing: 0, Completed: 0, Failed: 0}
		for _, p := range d.jobs {
			if p.Queue == name {
				if _, ok := counts[p.Status]; ok {
					counts[p.Status]++
				}
			}
		}
		out[name] = counts
	}
	return out, nil
}
func (d *MemoryDriver) RecentThroughput(ctx context.Context, seconds int, q string) (map[string]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]int{Processing: 0, Completed: 0, Failed: 0}
	for _, m := range d.metrics {
		if (q == "" || m.queue == q) && m.at >= Now()-float64(seconds) {
			if _, ok := out[m.name]; ok {
				out[m.name]++
			}
		}
	}
	return out, nil
}
func cloneJSONMap(m map[string]any) (map[string]any, error) {
	if m == nil {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	err = json.Unmarshal(b, &out)
	return out, err
}
func valueNumber(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}
func (d *MemoryDriver) ReportSupervisor(ctx context.Context, m map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := cloneJSONMap(m)
	if err != nil {
		return err
	}
	name, _ := m["name"].(string)
	if name == "" {
		return fmt.Errorf("baqueue: supervisor name is required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.supervisors[name] = memorySupervisor{c, Now()}
	return nil
}
func (d *MemoryDriver) GetSupervisorStats(ctx context.Context, age float64) ([]map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []map[string]any{}
	for _, s := range d.supervisors {
		if s.at >= Now()-age && s.data["running"] == true {
			c, _ := cloneJSONMap(s.data)
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]["name"]) < fmt.Sprint(out[j]["name"]) })
	return out, nil
}
func (d *MemoryDriver) StoreBatch(ctx context.Context, id string, m map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := cloneJSONMap(m)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.batches[id] = c
	return nil
}
func (d *MemoryDriver) GetBatch(ctx context.Context, id string) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return cloneJSONMap(d.batches[id])
}

func (d *MemoryDriver) GetPendingBatches(ctx context.Context, limit int) ([]map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []map[string]any{}
	for _, batch := range d.batches {
		if !BatchNeedsReconciliation(batch) {
			continue
		}
		copy, err := cloneJSONMap(batch)
		if err != nil {
			return nil, err
		}
		out = append(out, copy)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
func (d *MemoryDriver) UpdateBatch(ctx context.Context, id string, m map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := cloneJSONMap(m)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.batches[id]
	if b == nil {
		return ErrNotFound
	}
	for k, v := range c {
		b[k] = v
	}
	return nil
}
func (d *MemoryDriver) IncrementBatchCounter(ctx context.Context, id, field string, delta int) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.batches[id]
	if b == nil {
		return nil, nil
	}
	b[field] = valueNumber(b[field]) + float64(delta)
	return cloneJSONMap(b)
}
func (d *MemoryDriver) prune(ctx context.Context, o PruneOptions, terminal bool) (int, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for id, p := range d.jobs {
		if terminal && p.Status != Completed && p.Status != Failed && p.Status != Cancelled {
			continue
		}
		if !memoryMatches(p, JobFilter{Queue: o.Queue, Status: o.Status, Tag: o.Tag}) {
			continue
		}
		if o.OlderThanSeconds != nil && p.UpdatedAt > Now()-*o.OlderThanSeconds {
			continue
		}
		d.delete(id)
		n++
		if o.Limit > 0 && n >= o.Limit {
			break
		}
	}
	return n, nil
}
func (d *MemoryDriver) Prune(ctx context.Context, o PruneOptions) (int, error) {
	return d.prune(ctx, o, false)
}
func (d *MemoryDriver) PruneTerminalJobs(ctx context.Context, o PruneOptions) (int, error) {
	return d.prune(ctx, o, true)
}
func (d *MemoryDriver) BulkDeleteJobs(ctx context.Context, ids []string, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	for _, id := range ids {
		d.delete(id)
	}
	return len(ids), nil
}
func (d *MemoryDriver) ReconcileIndexes(ctx context.Context, _ int) (int, error) { return 0, ctx.Err() }
func (d *MemoryDriver) ReconcilePendingJobs(ctx context.Context, _ int) (int, error) {
	return 0, ctx.Err()
}
func (d *MemoryDriver) Flush(ctx context.Context, q string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, p := range d.jobs {
		if q == "" || q == p.Queue {
			d.delete(id)
		}
	}
	if q != "" {
		delete(d.ready, q)
		delete(d.dynamic, q)
	} else {
		clear(d.dynamic)
		clear(d.ready)
		clear(d.batches)
		clear(d.supervisors)
		d.metrics = nil
	}
	return nil
}

var _ DynamicQueueDriver = (*MemoryDriver)(nil)

func (d *MemoryDriver) registerDynamic(name string, timeout float64) (DynamicQueueInfo, error) {
	info, exists := d.dynamic[name]
	busy := false
	for _, p := range d.jobs {
		if p.Queue == name {
			if !exists {
				return DynamicQueueInfo{}, fmt.Errorf("baqueue: ordinary queue %q already contains jobs", name)
			}
			busy = busy || p.Status == Pending || p.Status == Processing
		}
	}
	now := Now()
	if !exists {
		info = newDynamicQueue(name, timeout, now)
	} else if info.IdleTimeoutSeconds != timeout {
		return DynamicQueueInfo{}, fmt.Errorf("baqueue: dynamic queue %q already has a different idle timeout", name)
	}
	info.Active, info.IdleSince = true, &now
	if busy {
		info.IdleSince = nil
	}
	d.dynamic[name] = info
	return cloneDynamicQueue(info), nil
}

func (d *MemoryDriver) RegisterDynamicQueue(ctx context.Context, name string, timeout float64) (DynamicQueueInfo, error) {
	if err := validateDynamicQueue(name, timeout); err != nil {
		return DynamicQueueInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return DynamicQueueInfo{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.registerDynamic(name, timeout)
}

func (d *MemoryDriver) PushDynamic(ctx context.Context, ps []*JobPayload, name string, timeout float64) ([]string, error) {
	if err := validateDynamicQueue(name, timeout); err != nil {
		return nil, err
	}
	for _, p := range ps {
		if p == nil || p.Queue != name {
			return nil, fmt.Errorf("baqueue: dynamic payload queue must match its definition")
		}
	}
	return d.pushMany(ctx, ps, &DynamicQueueInfo{Name: name, IdleTimeoutSeconds: timeout})
}

func (d *MemoryDriver) GetDynamicQueues(ctx context.Context) ([]DynamicQueueInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []DynamicQueueInfo{}
	for _, info := range d.dynamic {
		if info.Active {
			out = append(out, cloneDynamicQueue(info))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (d *MemoryDriver) ExpireDynamicQueues(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := Now()
	expired := []string{}
	for name, info := range d.dynamic {
		busy, retained := false, false
		for _, p := range d.jobs {
			if p.Queue == name {
				retained = true
				busy = busy || p.Status == Pending || p.Status == Processing
			}
		}
		if busy {
			info.Active, info.IdleSince = true, nil
		} else if info.Active {
			if info.IdleSince == nil {
				at := now
				info.IdleSince = &at
			} else if now >= *info.IdleSince+info.IdleTimeoutSeconds {
				info.Active = false
				delete(d.ready, name)
				expired = append(expired, name)
			}
		}
		if !info.Active && !retained {
			delete(d.dynamic, name)
		} else {
			d.dynamic[name] = info
		}
	}
	sort.Strings(expired)
	return expired, nil
}
func (d *MemoryDriver) PruneMetrics(ctx context.Context, age float64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	before := len(d.metrics)
	cutoff := Now() - age
	d.metrics = slices.DeleteFunc(d.metrics, func(m memoryMetric) bool { return m.at < cutoff })
	return before - len(d.metrics), nil
}
