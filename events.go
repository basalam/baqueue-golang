package baqueue

import (
	"context"
	"log/slog"
	"sync"
)

type Event struct {
	Name string
	Data map[string]any
}
type EventHandler func(context.Context, Event) error
type eventListener struct {
	id uint64
	fn EventHandler
}

// EventBus is instance scoped; listeners run in registration order outside the lock.
type EventBus struct {
	mu        sync.RWMutex
	next      uint64
	listeners map[string][]eventListener
}

func NewEventBus() *EventBus { return &EventBus{listeners: map[string][]eventListener{}} }
func (e *EventBus) On(name string, fn EventHandler) func() {
	e.mu.Lock()
	e.next++
	id := e.next
	e.listeners[name] = append(e.listeners[name], eventListener{id, fn})
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		ls := e.listeners[name]
		for i, l := range ls {
			if l.id == id {
				e.listeners[name] = append(ls[:i], ls[i+1:]...)
				break
			}
		}
	}
}
func (e *EventBus) Emit(ctx context.Context, name string, data map[string]any) {
	e.mu.RLock()
	ls := append([]eventListener{}, e.listeners[name]...)
	e.mu.RUnlock()
	for _, l := range ls {
		func() {
			defer func() {
				if v := recover(); v != nil {
					slog.Error("baqueue event handler panic", "event", name, "panic", v)
				}
			}()
			if err := l.fn(ctx, Event{name, data}); err != nil {
				slog.Error("baqueue event handler failed", "event", name, "error", err)
			}
		}()
	}
}
