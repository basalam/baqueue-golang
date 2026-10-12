package baqueue

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

type Balancer interface {
	Recommend(context.Context, Driver, []string, int) (int, error)
}
type AutoBalancer struct{ MinWorkers, MaxWorkers int }
type SimpleBalancer struct{ MinWorkers, MaxWorkers int }
type NullBalancer struct{}

func backlog(ctx context.Context, d Driver, queues []string) (int, error) {
	n := 0
	for _, q := range queues {
		size, err := d.Size(ctx, q)
		if err != nil {
			return 0, err
		}
		n += size
	}
	return n, nil
}
func (b AutoBalancer) Recommend(ctx context.Context, d Driver, queues []string, current int) (int, error) {
	n, err := backlog(ctx, d, queues)
	if err != nil {
		return current, err
	}
	desired := current
	switch {
	case n == 0:
		desired = b.MinWorkers
	case n > current*10:
		desired += 2
	case n > current*5:
		desired++
	case n < current:
		desired--
	}
	return max(b.MinWorkers, min(b.MaxWorkers, desired)), nil
}
func (b SimpleBalancer) Recommend(ctx context.Context, d Driver, queues []string, current int) (int, error) {
	n, err := backlog(ctx, d, queues)
	if err != nil {
		return current, err
	}
	if n == 0 {
		return b.MinWorkers, nil
	}
	return max(b.MinWorkers, min(b.MaxWorkers, len(queues))), nil
}
func (NullBalancer) Recommend(_ context.Context, _ Driver, _ []string, current int) (int, error) {
	return current, nil
}
func CreateBalancer(strategy string, minWorkers, maxWorkers int) Balancer {
	switch strategy {
	case "auto":
		return AutoBalancer{minWorkers, maxWorkers}
	case "simple":
		return SimpleBalancer{minWorkers, maxWorkers}
	default:
		return NullBalancer{}
	}
}

type supervisorWorker struct {
	worker *Worker
	done   chan struct{}
}
type Supervisor struct {
	Queue    *Queue
	Config   SupervisorConfig
	Balancer Balancer
	Pruner   *Pruner
	mu       sync.Mutex
	workers  []*supervisorWorker
	retiring []*supervisorWorker
	ctx      context.Context
	running  bool
	serial   int
	desired  int
}

func NewSupervisor(q *Queue, c SupervisorConfig) *Supervisor {
	return &Supervisor{Queue: q, Config: c, Balancer: CreateBalancer(c.Balance, c.MinWorkers, c.MaxWorkers)}
}
func (s *Supervisor) Stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := make([]map[string]any, 0, len(s.workers))
	for _, w := range s.workers {
		ws = append(ws, w.worker.Stats())
	}
	return map[string]any{"name": s.Config.Name, "queues": append([]string{}, s.Config.Queues...), "balance": s.Config.Balance, "workers": len(ws), "running": s.running, "worker_stats": ws}
}
func (s *Supervisor) spawn() {
	s.serial++
	w := NewWorker(s.Queue, WorkerOptions{Name: fmt.Sprintf("%s-worker-%d", s.Config.Name, s.serial), Queues: s.Config.Queues, Sleep: time.Duration(s.Config.Sleep * float64(time.Second)), Timeout: time.Duration(s.Config.Timeout * float64(time.Second)), MaxJobs: s.Config.MaxJobsPerWorker, MaxTime: time.Duration(s.Config.MaxTime * float64(time.Second))})
	entry := &supervisorWorker{worker: w, done: make(chan struct{})}
	s.workers = append(s.workers, entry)
	go func() {
		defer close(entry.done)
		if err := w.Run(s.ctx); err != nil {
			slog.Error("baqueue worker stopped", "error", err)
		}
	}()
}

// Scale retires surplus workers after their current job finishes.
func (s *Supervisor) Scale(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	count = max(s.Config.MinWorkers, min(s.Config.MaxWorkers, count))
	s.desired = count
	s.resize()
}

// Call with s.mu held; retiring workers still occupy execution capacity.
func (s *Supervisor) resize() {
	for len(s.workers) < s.desired && len(s.workers)+len(s.retiring) < s.Config.MaxWorkers {
		s.spawn()
	}
	for len(s.workers) > s.desired {
		i := len(s.workers) - 1
		w := s.workers[i]
		s.workers = s.workers[:i]
		w.worker.Stop()
		s.retiring = append(s.retiring, w)
	}
}
func (s *Supervisor) reap() {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := s.workers[:0]
	for _, w := range s.workers {
		select {
		case <-w.done:
		default:
			active = append(active, w)
		}
	}
	s.workers = active
	retiring := s.retiring[:0]
	for _, w := range s.retiring {
		select {
		case <-w.done:
		default:
			retiring = append(retiring, w)
		}
	}
	s.retiring = retiring
	s.resize()
}
func (s *Supervisor) report(ctx context.Context) {
	if err := s.Queue.Driver.ReportSupervisor(ctx, s.Stats()); err != nil && ctx.Err() == nil {
		slog.Warn("baqueue supervisor heartbeat failed", "error", err)
	}
}
func (s *Supervisor) recover(ctx context.Context) {
	if !s.Config.RecoverStuckJobs {
		return
	}
	seen := map[string]bool{}
	for _, q := range s.Config.Queues {
		if seen[q] {
			continue
		}
		seen[q] = true
		if _, err := s.Queue.Driver.RequeueStuckJobs(ctx, s.Config.StuckProcessingSeconds, q); err != nil && ctx.Err() == nil {
			slog.Error("baqueue stuck recovery failed", "error", err)
		}
	}
}
func (s *Supervisor) Run(ctx context.Context) error {
	if err := s.Config.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("baqueue: supervisor already running")
	}
	s.running = true
	s.ctx = ctx
	s.mu.Unlock()
	s.Queue.Events.Emit(ctx, "supervisor.started", map[string]any{"supervisor": s.Config.Name})
	s.Scale(s.Config.MinWorkers)
	auxCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if s.Pruner != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Pruner.Run(auxCtx); err != nil {
				slog.Error("baqueue pruner stopped", "error", err)
			}
		}()
	}
	defer func() {
		cancel()
		s.mu.Lock()
		s.running = false
		all := append(append([]*supervisorWorker{}, s.workers...), s.retiring...)
		s.mu.Unlock()
		for _, w := range all {
			w.worker.Stop()
		}
		for _, w := range all {
			<-w.done
		}
		wg.Wait()
		s.mu.Lock()
		s.workers = nil
		s.retiring = nil
		s.mu.Unlock()
		reportCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		s.report(reportCtx)
		s.Queue.Events.Emit(reportCtx, "supervisor.stopped", map[string]any{"supervisor": s.Config.Name})
	}()
	heartbeat := time.NewTicker(time.Second)
	defer heartbeat.Stop()
	balance := time.NewTicker(5 * time.Second)
	defer balance.Stop()
	stuck := time.NewTicker(time.Duration(max(1, s.Config.StuckCheckIntervalSeconds) * float64(time.Second)))
	defer stuck.Stop()
	s.recover(ctx)
	s.report(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeat.C:
			s.reap()
			if _, err := s.Queue.ReconcileBatches(ctx, 100); err != nil && ctx.Err() == nil {
				slog.Warn("baqueue batch reconciliation failed", "error", err)
			}
			if _, err := s.Queue.Driver.PopDelayed(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("baqueue delayed promotion failed", "error", err)
			}
			s.report(ctx)
		case <-stuck.C:
			s.recover(ctx)
		case <-balance.C:
			s.mu.Lock()
			count := len(s.workers)
			s.mu.Unlock()
			if s.Balancer != nil {
				desired, err := s.Balancer.Recommend(ctx, s.Queue.Driver, s.Config.Queues, count)
				if err == nil {
					s.Scale(desired)
				} else if ctx.Err() == nil {
					slog.Warn("baqueue balancing failed", "error", err)
				}
			}
			// Goroutines share a heap: memory pressure triggers GC, not per-worker termination.
			if s.Config.MemoryLimit > 0 {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > uint64(s.Config.MemoryLimit)*1024*1024 {
					runtime.GC()
				}
			}
		}
	}
}
