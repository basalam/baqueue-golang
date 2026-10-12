package baqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type scheduledEntry struct {
	entry    ScheduleEntry
	schedule cron.Schedule
	next     time.Time
}
type Scheduler struct {
	Queue   *Queue
	mu      sync.Mutex
	entries []*scheduledEntry
}

func NewScheduler(q *Queue, entries []ScheduleEntry) (*Scheduler, error) {
	s := &Scheduler{Queue: q}
	for _, e := range entries {
		if err := s.Add(e); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func (s *Scheduler) Add(e ScheduleEntry) error {
	if e.JobClass == "" {
		return fmt.Errorf("baqueue: scheduled job name required")
	}
	if e.Queue == "" {
		e.Queue = "default"
	}
	var schedule cron.Schedule
	var err error
	if e.Cron != "" {
		schedule, err = cron.ParseStandard(e.Cron)
		if err != nil {
			return err
		}
	} else if !validSeconds(e.Every, false) {
		return fmt.Errorf("baqueue: schedule requires positive every or cron")
	}
	payload, err := s.Queue.BuildPayload(e.JobClass, e.Payload, OnQueue(e.Queue))
	if err != nil {
		return err
	}
	e.Payload = payload.Data
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, &scheduledEntry{entry: e, schedule: schedule})
	return nil
}

// Tick reserves due entries under the lock, then dispatches without holding it.
// Event listeners may register schedules without deadlocking the scheduler.
func (s *Scheduler) Tick(ctx context.Context, now time.Time) error {
	type dueEntry struct {
		item           *scheduledEntry
		previous, next time.Time
	}
	var due []dueEntry
	s.mu.Lock()
	for _, item := range s.entries {
		if !item.next.IsZero() && now.Before(item.next) {
			continue
		}
		previous := item.next
		if item.schedule != nil {
			item.next = item.schedule.Next(now.UTC())
		} else {
			item.next = now.Add(time.Duration(item.entry.Every * float64(time.Second)))
		}
		due = append(due, dueEntry{item, previous, item.next})
	}
	s.mu.Unlock()
	var errs []error
	for _, reserved := range due {
		item := reserved.item
		_, err := s.Queue.Push(ctx, item.entry.JobClass, item.entry.Payload, OnQueue(item.entry.Queue))
		if err != nil {
			s.mu.Lock()
			if item.next.Equal(reserved.next) {
				item.next = reserved.previous
			}
			s.mu.Unlock()
			errs = append(errs, err)
			continue
		}
		s.Queue.Events.Emit(ctx, "schedule.dispatched", map[string]any{"entry": item.entry})
	}
	return errors.Join(errs...)
}
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.Tick(ctx, time.Now().UTC()); err != nil && ctx.Err() == nil {
			slog.Error("baqueue schedule dispatch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
