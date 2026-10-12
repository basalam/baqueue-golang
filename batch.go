package baqueue

import (
	"context"
	"errors"
	"sync/atomic"
)

type Batch struct {
	queue                          *Queue
	jobs                           []JobRequest
	id, name, then, catch, finally string
	allowFailures                  bool
	tags                           []string
	dispatched                     atomic.Bool
}
type BatchResult struct {
	BatchID string   `json:"batch_id"`
	Name    string   `json:"name"`
	JobIDs  []string `json:"job_ids"`
	Total   int      `json:"total"`
}

func (q *Queue) Batch(jobs []JobRequest) *Batch {
	return &Batch{queue: q, jobs: append([]JobRequest{}, jobs...), id: NewID()}
}
func (b *Batch) Name(name string) *Batch    { b.name = name; return b }
func (b *Batch) Then(name string) *Batch    { b.then = name; return b }
func (b *Batch) Catch(name string) *Batch   { b.catch = name; return b }
func (b *Batch) Finally(name string) *Batch { b.finally = name; return b }
func (b *Batch) AllowFailures() *Batch      { b.allowFailures = true; return b }
func (b *Batch) Tag(tags ...string) *Batch  { b.tags = append(b.tags, tags...); return b }
func (b *Batch) Dispatch(ctx context.Context) (*BatchResult, error) {
	if !b.dispatched.CompareAndSwap(false, true) {
		return nil, errors.New("baqueue: a batch builder can only be dispatched once")
	}
	ps := make([]*JobPayload, 0, len(b.jobs))
	for _, j := range b.jobs {
		p, err := b.queue.BuildPayload(j.Name, j.Data, j.Options...)
		if err != nil {
			return nil, err
		}
		p.BatchID = &b.id
		p.Tags = append(p.Tags, "batch:"+b.id)
		if b.name != "" {
			p.Tags = append(p.Tags, "batch:"+b.name)
		}
		p.Tags = append(p.Tags, b.tags...)
		ps = append(ps, p)
	}
	m := map[string]any{"id": b.id, "name": b.name, "total": len(ps), "completed_count": 0, "failed_count": 0, "allow_failures": b.allowFailures, "then_job": b.then, "catch_job": b.catch, "finally_job": b.finally, "created_at": Now(), "status": Processing}
	if err := b.queue.Driver.StoreBatch(ctx, b.id, m); err != nil {
		return nil, err
	}
	ids, err := b.queue.Driver.PushMany(ctx, ps)
	if err != nil {
		return nil, err
	}
	b.queue.Events.Emit(ctx, "batch.dispatched", map[string]any{"batch_id": b.id, "count": len(ids)})
	if len(ids) == 0 {
		if err = b.queue.ReconcileBatch(ctx, b.id); err != nil {
			return nil, err
		}
	}
	return &BatchResult{b.id, b.name, ids, len(ids)}, nil
}
func (q *Queue) batchCallback(ctx context.Context, id, kind string, b map[string]any) error {
	name, _ := b[kind+"_job"].(string)
	if name == "" {
		return nil
	}
	p, err := q.BuildPayload(name, map[string]any{"batch_id": id, "batch": b})
	if err != nil {
		return err
	}
	p.ID = id + ":" + kind
	_, err = q.Driver.Push(ctx, p)
	if errors.Is(err, ErrDuplicate) {
		return nil
	}
	return err
}

// ApplyBatchOutcome is called by drivers inside the terminal job transaction.
// Manual retries do not reopen the original batch's first terminal outcome.
func ApplyBatchOutcome(data map[string]any, p *JobPayload) bool {
	if data == nil || p == nil || (p.Status != Completed && p.Status != Failed) {
		return false
	}
	outcomes, _ := data["member_outcomes"].(map[string]any)
	if outcomes == nil {
		outcomes = map[string]any{}
	}
	if _, exists := outcomes[p.ID]; exists {
		return false
	}
	outcomes[p.ID] = p.Status
	data["member_outcomes"] = outcomes
	field := "completed_count"
	if p.Status == Failed {
		field = "failed_count"
	}
	data[field] = valueNumber(data[field]) + 1
	return true
}

func BatchNeedsReconciliation(data map[string]any) bool {
	if data == nil || data["status"] == Completed {
		return false
	}
	failed := valueNumber(data["failed_count"])
	return valueNumber(data["completed_count"])+failed >= valueNumber(data["total"]) ||
		(failed > 0 && data["allow_failures"] != true && data["catch_dispatched"] != true)
}

// ReconcileBatch retries pending callback dispatch after worker/storage failures.
func (q *Queue) ReconcileBatch(ctx context.Context, id string) error {
	b, err := q.Driver.GetBatch(ctx, id)
	if err != nil || !BatchNeedsReconciliation(b) {
		return err
	}
	dispatch := func(kind string) error {
		if b[kind+"_dispatched"] == true {
			return nil
		}
		if err := q.batchCallback(ctx, id, kind, b); err != nil {
			return err
		}
		if err := q.Driver.UpdateBatch(ctx, id, map[string]any{kind + "_dispatched": true}); err != nil {
			return err
		}
		b[kind+"_dispatched"] = true
		return nil
	}
	if valueNumber(b["failed_count"]) > 0 && b["allow_failures"] != true && b["catch_dispatched"] != true {
		if err = dispatch("catch"); err != nil {
			return err
		}
		announced, err := q.Driver.IncrementBatchCounter(ctx, id, "catch_announced", 1)
		if err != nil {
			return err
		}
		if valueNumber(announced["catch_announced"]) == 1 {
			q.Events.Emit(ctx, "batch.failed", map[string]any{"batch_id": id, "batch": b})
		}
	}
	if valueNumber(b["completed_count"])+valueNumber(b["failed_count"]) < valueNumber(b["total"]) {
		return nil
	}
	if valueNumber(b["failed_count"]) == 0 {
		if err = dispatch("then"); err != nil {
			return err
		}
	}
	if err = dispatch("finally"); err != nil {
		return err
	}
	if err = q.Driver.UpdateBatch(ctx, id, map[string]any{"status": Completed}); err != nil {
		return err
	}
	announced, err := q.Driver.IncrementBatchCounter(ctx, id, "completed_announced", 1)
	if err != nil {
		return err
	}
	if valueNumber(announced["completed_announced"]) == 1 {
		b["status"] = Completed
		q.Events.Emit(ctx, "batch.completed", map[string]any{"batch_id": id, "batch": b})
	}
	return nil
}

func (q *Queue) ReconcileBatches(ctx context.Context, limit int) (int, error) {
	batches, err := q.Driver.GetPendingBatches(ctx, limit)
	if err != nil {
		return 0, err
	}
	count := 0
	var errs []error
	for _, b := range batches {
		id, _ := b["id"].(string)
		if id == "" {
			continue
		}
		if err = q.ReconcileBatch(ctx, id); err != nil {
			errs = append(errs, err)
		} else {
			count++
		}
	}
	return count, errors.Join(errs...)
}

func (q *Queue) finishBatch(ctx context.Context, p *JobPayload) error {
	if p.BatchID == nil {
		return nil
	}
	return q.ReconcileBatch(ctx, *p.BatchID)
}
