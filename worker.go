package baqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerOptions struct {
	Name    string
	Queues  []string
	Sleep   time.Duration
	Timeout time.Duration
	MaxJobs int
	MaxTime time.Duration
}

type Worker struct {
	queue             *Queue
	options           WorkerOptions
	stop              chan struct{}
	stopOnce          sync.Once
	running           atomic.Bool
	mu                sync.RWMutex
	current           string
	processed, failed int
	cursor            int
}

func NewWorker(q *Queue, o WorkerOptions) *Worker {
	if o.Name == "" {
		o.Name = "worker-" + NewID()[:8]
	}
	if len(o.Queues) == 0 {
		o.Queues = []string{"default"}
	}
	o.Queues = append([]string{}, o.Queues...)
	if o.Sleep <= 0 {
		o.Sleep = time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = time.Minute
	}
	return &Worker{queue: q, options: o, stop: make(chan struct{})}
}

func (w *Worker) Stop() { w.stopOnce.Do(func() { close(w.stop) }) }
func (w *Worker) Stats() map[string]any {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var current any
	if w.current != "" {
		current = w.current
	}
	return map[string]any{"name": w.options.Name, "queues": append([]string{}, w.options.Queues...), "running": w.running.Load(), "current_job": current, "jobs_processed": w.processed, "jobs_failed": w.failed}
}

// Run stops fetching on cancellation or Stop. Stop allows the active job to finish.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("baqueue: worker already running")
	}
	defer w.running.Store(false)
	w.queue.Events.Emit(ctx, "worker.started", map[string]any{"worker": w.options.Name})
	defer w.queue.Events.Emit(context.WithoutCancel(ctx), "worker.stopped", map[string]any{"worker": w.options.Name})
	start := time.Now()
	count := 0
	consecutiveErrors := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.stop:
			return nil
		default:
		}
		if (w.options.MaxJobs > 0 && count >= w.options.MaxJobs) || (w.options.MaxTime > 0 && time.Since(start) >= w.options.MaxTime) {
			return nil
		}
		worked, err := w.RunOnce(ctx)
		if err != nil {
			consecutiveErrors++
			if consecutiveErrors == 1 || consecutiveErrors%10 == 0 {
				slog.Error("baqueue worker operation failed", "worker", w.options.Name, "error", err, "consecutive_errors", consecutiveErrors)
			}
		} else {
			consecutiveErrors = 0
		}
		if worked {
			count++
		}
		if !worked || err != nil {
			delay := w.options.Sleep
			if err != nil {
				delay = max(delay, min(5*time.Second, 100*time.Millisecond*time.Duration(1<<min(consecutiveErrors-1, 6))))
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-w.stop:
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
	}
}

// RunOnce claims and executes at most one job. Do not call concurrently on one worker.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	for i := 0; i < len(w.options.Queues); i++ {
		index := (w.cursor + i) % len(w.options.Queues)
		p, err := w.queue.Driver.Pop(ctx, w.options.Queues[index])
		if err != nil {
			return false, err
		}
		if p == nil {
			continue
		}
		w.cursor = (index + 1) % len(w.options.Queues)
		return true, w.process(ctx, p)
	}
	w.cursor = (w.cursor + 1) % len(w.options.Queues)
	return false, nil
}

func invokeHandler(ctx context.Context, h Handler, data map[string]any) (result any, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("handler panic: %v\n%s", v, debug.Stack())
		}
	}()
	return h.Handle(ctx, data)
}
func invokeHook(fn func() error) {
	defer func() {
		if v := recover(); v != nil {
			slog.Error("baqueue hook panic", "panic", v)
		}
	}()
	if err := fn(); err != nil {
		slog.Error("baqueue hook failed", "error", err)
	}
}
func (w *Worker) metric(ctx context.Context, p *JobPayload, name string) {
	if err := w.queue.Driver.RecordMetric(ctx, p.Queue, name, 1); err != nil {
		slog.Warn("baqueue metric write failed", "error", err)
	}
}

func (w *Worker) process(ctx context.Context, p *JobPayload) error {
	w.mu.Lock()
	w.current = p.ID
	w.mu.Unlock()
	defer func() { w.mu.Lock(); w.current = ""; w.mu.Unlock() }()
	w.queue.Events.Emit(ctx, "job.started", map[string]any{"payload": p.Clone(), "worker": w.options.Name})
	w.metric(ctx, p, Processing)
	timeout := w.options.Timeout
	if p.Timeout > 0 {
		timeout = time.Duration(p.Timeout * float64(time.Second))
	}
	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	def, ok := w.queue.lookup(p.JobClass)
	var result any
	var jobErr error
	if !ok {
		jobErr = fmt.Errorf("baqueue: handler %q is not registered", p.JobClass)
	} else {
		result, jobErr = invokeHandler(jobCtx, def.handler, p.Data)
	}
	if jobErr == nil {
		jobErr = jobCtx.Err()
	}
	// Persistence has a separate deadline: shutdown must not discard a finished attempt.
	writeCtx, writeCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer writeCancel()
	finished := Now()
	started := finished
	if p.StartedAt != nil {
		started = *p.StartedAt
	}
	record := Attempt{Attempt: p.Attempts, StartedAt: started, FinishedAt: finished, Duration: max(0, finished-started), Status: Completed}
	if jobErr == nil {
		p.History = append(p.History, record)
		if err := w.queue.Driver.Complete(writeCtx, p); err != nil {
			return err
		}
		batchErr := w.queue.finishBatch(writeCtx, p)
		w.metric(writeCtx, p, Completed)
		w.mu.Lock()
		w.processed++
		w.mu.Unlock()
		w.queue.Events.Emit(writeCtx, "job.completed", map[string]any{"payload": p.Clone(), "result": result, "worker": w.options.Name})
		if h, ok := def.handler.(SuccessHandler); ok {
			invokeHook(func() error { return h.OnSuccess(writeCtx, result, p.Clone()) })
		}
		return batchErr
	}
	message := jobErr.Error()
	short := message
	if len(short) > 1000 {
		short = short[:1000]
	}
	record.Status = Failed
	record.Error = &short
	if ShouldRetry(p.Attempts, p.MaxAttempts) {
		delay := ComputeDelay(p.Backoff, p.Attempts)
		at := finished + delay
		record.WillRetry = true
		record.NextRetryAt = &at
		p.History = append(p.History, record)
		if err := w.queue.Driver.Release(writeCtx, p, delay); err != nil {
			return err
		}
		w.queue.Events.Emit(writeCtx, "job.retrying", map[string]any{"payload": p.Clone(), "error": message, "delay": delay})
		return nil
	}
	p.History = append(p.History, record)
	if err := w.queue.Driver.Fail(writeCtx, p, message); err != nil {
		return err
	}
	batchErr := w.queue.finishBatch(writeCtx, p)
	w.metric(writeCtx, p, Failed)
	w.mu.Lock()
	w.failed++
	w.mu.Unlock()
	w.queue.Events.Emit(writeCtx, "job.failed", map[string]any{"payload": p.Clone(), "error": message, "worker": w.options.Name})
	if ok {
		if h, yes := def.handler.(FailureHandler); yes {
			invokeHook(func() error { return h.OnFailure(writeCtx, jobErr, p.Clone()) })
		}
	}
	return batchErr
}
