package baqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Handler interface {
	Handle(context.Context, map[string]any) (any, error)
}
type HandlerFunc func(context.Context, map[string]any) (any, error)

func (f HandlerFunc) Handle(ctx context.Context, data map[string]any) (any, error) {
	return f(ctx, data)
}

type SuccessHandler interface {
	OnSuccess(context.Context, any, *JobPayload) error
}
type FailureHandler interface {
	OnFailure(context.Context, error, *JobPayload) error
}
type JobOption func(*JobPayload)

func OnQueue(name string) JobOption         { return func(p *JobPayload) { p.Queue = name } }
func WithMaxAttempts(n int) JobOption       { return func(p *JobPayload) { p.MaxAttempts = n } }
func WithBackoff(b Backoff) JobOption       { return func(p *JobPayload) { p.Backoff = b } }
func WithTimeout(t time.Duration) JobOption { return func(p *JobPayload) { p.Timeout = t.Seconds() } }
func WithTags(tags ...string) JobOption {
	return func(p *JobPayload) { p.Tags = append(p.Tags, tags...) }
}

type registeredJob struct {
	handler Handler
	options []JobOption
}
type Queue struct {
	Driver   Driver
	Events   *EventBus
	Config   Config
	mu       sync.RWMutex
	handlers map[string]registeredJob
}

func NewQueue(d Driver) *Queue {
	return &Queue{Driver: d, Events: NewEventBus(), Config: DefaultConfig(), handlers: map[string]registeredJob{}}
}
func (q *Queue) Close(ctx context.Context) error { return q.Driver.Disconnect(ctx) }
func (q *Queue) Register(name string, h Handler, options ...JobOption) error {
	if name == "" || h == nil {
		return errors.New("baqueue: handler name and implementation required")
	}
	p := NewPayload(name, nil)
	for _, opt := range options {
		opt(p)
	}
	if err := p.Validate(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.handlers[name]; ok {
		return fmt.Errorf("baqueue: handler %q already registered", name)
	}
	q.handlers[name] = registeredJob{h, append([]JobOption{}, options...)}
	return nil
}
func (q *Queue) RegisterFunc(name string, h HandlerFunc, options ...JobOption) error {
	return q.Register(name, h, options...)
}
func (q *Queue) lookup(name string) (registeredJob, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	v, ok := q.handlers[name]
	return v, ok
}
func (q *Queue) BuildPayload(name string, data map[string]any, options ...JobOption) (*JobPayload, error) {
	p := NewPayload(name, data)
	if def, ok := q.lookup(name); ok {
		for _, opt := range def.options {
			opt(p)
		}
	}
	for _, opt := range options {
		opt(p)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p.Clone(), nil
}
func (q *Queue) Push(ctx context.Context, name string, data map[string]any, options ...JobOption) (string, error) {
	p, err := q.BuildPayload(name, data, options...)
	if err != nil {
		return "", err
	}
	return q.pushPayload(ctx, p)
}
func (q *Queue) pushPayload(ctx context.Context, p *JobPayload) (string, error) {
	id, err := q.Driver.Push(ctx, p)
	if err == nil {
		q.Events.Emit(ctx, "job.pushed", map[string]any{"payload": p.Clone()})
	}
	return id, err
}
func (q *Queue) Later(ctx context.Context, name string, delay time.Duration, data map[string]any, options ...JobOption) (string, error) {
	if delay < 0 {
		return "", errors.New("baqueue: delay must be nonnegative")
	}
	p, err := q.BuildPayload(name, data, options...)
	if err != nil {
		return "", err
	}
	at := Now() + delay.Seconds()
	p.DelayUntil = &at
	return q.pushPayload(ctx, p)
}

type JobRequest struct {
	Name    string
	Data    map[string]any
	Options []JobOption
}

func (q *Queue) Bulk(ctx context.Context, jobs []JobRequest) ([]string, error) {
	ps := make([]*JobPayload, 0, len(jobs))
	for _, j := range jobs {
		p, err := q.BuildPayload(j.Name, j.Data, j.Options...)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	ids, err := q.Driver.PushMany(ctx, ps)
	if err != nil {
		return nil, err
	}
	for _, p := range ps {
		q.Events.Emit(ctx, "job.pushed", map[string]any{"payload": p.Clone()})
	}
	return ids, nil
}
func (q *Queue) RetryFailed(ctx context.Context, f JobFilter) (int, error) {
	f.Status = Failed
	f.Offset = 0
	f.Limit = 100
	total := 0
	for i := 0; i < 1000; i++ {
		jobs, err := q.Driver.GetJobs(ctx, f)
		if err != nil {
			return total, err
		}
		if len(jobs) == 0 {
			return total, nil
		}
		progress := 0
		for _, p := range jobs {
			err = q.Driver.Release(ctx, p, 0)
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return total, err
			}
			total++
			progress++
		}
		if progress == 0 {
			return total, nil
		}
	}
	return total, nil
}
func (q *Queue) Prune(ctx context.Context, o PruneOptions) (int, error) {
	n, err := q.Driver.Prune(ctx, o)
	if err == nil {
		q.Events.Emit(ctx, "queue.pruned", map[string]any{"count": n})
	}
	return n, err
}
func (q *Queue) Metrics(ctx context.Context, queue string) (map[string]any, error) {
	return q.Driver.GetMetrics(ctx, queue)
}

func (q *Queue) Flush(ctx context.Context, queue string) error {
	return q.Driver.Flush(ctx, queue)
}

func (q *Queue) Size(ctx context.Context, queue string) (int, error) {
	return q.Driver.Size(ctx, queue)
}

func (q *Queue) Queues(ctx context.Context) ([]string, error) {
	return q.Driver.Queues(ctx)
}

func (q *Queue) GetJob(ctx context.Context, id string) (*JobPayload, error) {
	return q.Driver.GetJob(ctx, id)
}

func (q *Queue) GetJobs(ctx context.Context, filter JobFilter) ([]*JobPayload, error) {
	return q.Driver.GetJobs(ctx, filter)
}
