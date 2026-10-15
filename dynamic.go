package baqueue

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// DynamicQueueInfo describes a queue whose active registration expires when idle.
type DynamicQueueInfo struct {
	Name               string   `json:"name"`
	IdleTimeoutSeconds float64  `json:"idle_timeout_seconds"`
	CreatedAt          float64  `json:"created_at"`
	IdleSince          *float64 `json:"idle_since"`
	Active             bool     `json:"active"`
}

// DynamicQueueDriver is an optional extension implemented by all built-in drivers.
// An expired policy is retained with historical jobs so manual retry can revive it.
type DynamicQueueDriver interface {
	RegisterDynamicQueue(context.Context, string, float64) (DynamicQueueInfo, error)
	GetDynamicQueues(context.Context) ([]DynamicQueueInfo, error)
	ExpireDynamicQueues(context.Context) ([]string, error)
	PushDynamic(context.Context, []*JobPayload, string, float64) ([]string, error)
}

func validateDynamicQueue(name string, timeout float64) error {
	if strings.TrimSpace(name) == "" || strings.Contains(name, ":status:") {
		return fmt.Errorf("baqueue: dynamic queue name must be nonempty and must not contain ':status:'")
	}
	if timeout <= 0 || math.IsNaN(timeout) || math.IsInf(timeout, 0) {
		return fmt.Errorf("baqueue: dynamic queue idle timeout must be finite and positive")
	}
	return nil
}

func newDynamicQueue(name string, timeout, now float64) DynamicQueueInfo {
	return DynamicQueueInfo{Name: name, IdleTimeoutSeconds: timeout, CreatedAt: now, IdleSince: &now, Active: true}
}

func cloneDynamicQueue(info DynamicQueueInfo) DynamicQueueInfo {
	if info.IdleSince != nil {
		value := *info.IdleSince
		info.IdleSince = &value
	}
	return info
}

// DynamicQueue can be reused after expiration; dispatch atomically recreates it.
type DynamicQueue struct {
	Name               string
	IdleTimeoutSeconds float64
	queue              *Queue
	driver             DynamicQueueDriver
}

// CreateDynamicQueue defaults to a generated name and a 30-second idle timeout.
// Pass an empty name to generate one, and optionally supply a positive timeout.
func (q *Queue) CreateDynamicQueue(ctx context.Context, name string, idleTimeout ...time.Duration) (*DynamicQueue, error) {
	if len(idleTimeout) > 1 {
		return nil, fmt.Errorf("baqueue: at most one idle timeout is allowed")
	}
	timeout := 30.0
	if len(idleTimeout) == 1 {
		timeout = idleTimeout[0].Seconds()
	}
	if name == "" {
		name = "dynamic-" + NewID()
	}
	if err := validateDynamicQueue(name, timeout); err != nil {
		return nil, err
	}
	driver, ok := q.Driver.(DynamicQueueDriver)
	if !ok {
		return nil, fmt.Errorf("baqueue: driver does not support dynamic queues")
	}
	if _, err := driver.RegisterDynamicQueue(ctx, name, timeout); err != nil {
		return nil, err
	}
	return &DynamicQueue{Name: name, IdleTimeoutSeconds: timeout, queue: q, driver: driver}, nil
}

func (q *Queue) DynamicQueues(ctx context.Context) ([]DynamicQueueInfo, error) {
	if driver, ok := q.Driver.(DynamicQueueDriver); ok {
		return driver.GetDynamicQueues(ctx)
	}
	return []DynamicQueueInfo{}, nil
}

func (d *DynamicQueue) Push(ctx context.Context, name string, data map[string]any, options ...JobOption) (string, error) {
	ids, err := d.Bulk(ctx, []JobRequest{{Name: name, Data: data, Options: options}})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func (d *DynamicQueue) Later(ctx context.Context, name string, delay time.Duration, data map[string]any, options ...JobOption) (string, error) {
	if delay < 0 {
		return "", fmt.Errorf("baqueue: delay must be nonnegative")
	}
	opts := append(append([]JobOption{}, options...), OnQueue(d.Name))
	p, err := d.queue.BuildPayload(name, data, opts...)
	if err != nil {
		return "", err
	}
	at := Now() + delay.Seconds()
	p.DelayUntil = &at
	ids, err := d.dispatch(ctx, []*JobPayload{p})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func (d *DynamicQueue) Bulk(ctx context.Context, jobs []JobRequest) ([]string, error) {
	ps := make([]*JobPayload, 0, len(jobs))
	for _, job := range jobs {
		opts := append(append([]JobOption{}, job.Options...), OnQueue(d.Name))
		p, err := d.queue.BuildPayload(job.Name, job.Data, opts...)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	return d.dispatch(ctx, ps)
}

func (d *DynamicQueue) dispatch(ctx context.Context, ps []*JobPayload) ([]string, error) {
	ids, err := d.driver.PushDynamic(ctx, ps, d.Name, d.IdleTimeoutSeconds)
	if err == nil {
		for _, p := range ps {
			d.queue.Events.Emit(ctx, "job.pushed", map[string]any{"payload": p.Clone()})
		}
	}
	return ids, err
}
