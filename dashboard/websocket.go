package dashboard

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type snapshot struct {
	data any
	err  error
}

type overviewHub struct {
	mu          sync.Mutex
	subscribers map[chan snapshot]struct{}
	cancel      context.CancelFunc
	done        chan struct{}
}

func (s *server) subscribeOverview() (<-chan snapshot, func()) {
	h := &s.hub
	ch := make(chan snapshot, 1)
	h.mu.Lock()
	if len(h.subscribers) == 0 {
		h.subscribers = make(map[chan snapshot]struct{})
		ctx, cancel := context.WithCancel(s.lifecycleContext())
		h.cancel = cancel
		h.done = make(chan struct{})
		done := h.done
		go func() { defer close(done); s.broadcastOverview(ctx) }()
	}
	h.subscribers[ch] = struct{}{}
	done := h.done
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subscribers, ch)
		last := len(h.subscribers) == 0
		if last {
			h.cancel()
		}
		h.mu.Unlock()
		if last {
			<-done
		}
	}
}

func (s *server) broadcastOverview(ctx context.Context) {
	ticker := time.NewTicker(s.config.overviewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		data, err := s.overview(ctx)
		s.hub.mu.Lock()
		if ctx.Err() != nil {
			s.hub.mu.Unlock()
			return
		}
		for ch := range s.hub.subscribers {
			// A slow connection needs only the latest snapshot, never a backlog.
			select {
			case <-ch:
			default:
			}
			ch <- snapshot{data, err}
		}
		s.hub.mu.Unlock()
	}
}

type wsMessage struct {
	kind websocket.MessageType
	data []byte
}

func (s *server) accept(w http.ResponseWriter, r *http.Request) (*websocket.Conn, context.Context, <-chan wsMessage, func()) {
	if s.lifecycleContext().Err() != nil {
		http.Error(w, "Dashboard shutting down", http.StatusServiceUnavailable)
		return nil, nil, nil, nil
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.config.origins})
	if err != nil {
		return nil, nil, nil, nil
	}
	c.SetReadLimit(16 * 1024)
	ctx, cancel := context.WithCancel(r.Context())
	readCtx, cancelRead := context.WithCancel(r.Context())
	shutdownDone := make(chan struct{})
	stopShutdown := context.AfterFunc(s.lifecycleContext(), func() {
		defer close(shutdownDone)
		cancel()
		_ = c.Close(websocket.StatusGoingAway, "Dashboard shutting down")
		cancelRead()
	})
	messages := make(chan wsMessage, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		for {
			kind, data, err := c.Read(readCtx)
			if err != nil {
				return
			}
			select {
			case messages <- wsMessage{kind, data}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return c, ctx, messages, func() {
		stopped := stopShutdown()
		cancel()
		if !stopped {
			<-shutdownDone
		}
		cancelRead()
		_ = c.CloseNow()
		<-done
	}
}

func writeSnapshot(ctx context.Context, c *websocket.Conn, data any) bool {
	if ctx.Err() != nil {
		return false
	}
	// Query cancellation must not abort a frame before the shutdown close frame.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return wsjson.Write(ctx, c, data) == nil
}

func (s *server) overviewWS(w http.ResponseWriter, r *http.Request) {
	c, ctx, messages, cleanup := s.accept(w, r)
	if c == nil {
		return
	}
	defer cleanup()
	updates, unsubscribe := s.subscribeOverview()
	defer unsubscribe()
	data, err := s.overview(ctx)
	if err != nil {
		if ctx.Err() == nil {
			_ = c.Close(websocket.StatusInternalError, "Overview unavailable")
		}
		return
	}
	if !writeSnapshot(ctx, c, data) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			if msg.kind != websocket.MessageText {
				_ = c.Close(websocket.StatusUnsupportedData, "Text messages required")
				return
			}
		case update := <-updates:
			if update.err != nil {
				if ctx.Err() == nil {
					_ = c.Close(websocket.StatusInternalError, "Overview unavailable")
				}
				return
			}
			if !writeSnapshot(ctx, c, update.data) {
				return
			}
		}
	}
}

func (s *server) jobsWS(w http.ResponseWriter, r *http.Request) {
	c, ctx, messages, cleanup := s.accept(w, r)
	if c == nil {
		return
	}
	defer cleanup()
	filters := make(url.Values)
	push := func() bool {
		query, err := parseJobs(filters)
		if err != nil {
			_ = c.Close(websocket.StatusPolicyViolation, "Invalid job filters")
			return false
		}
		data, err := s.jobs(ctx, query)
		if err != nil {
			if ctx.Err() == nil {
				_ = c.Close(websocket.StatusInternalError, "Jobs unavailable")
			}
			return false
		}
		return writeSnapshot(ctx, c, data)
	}
	if !push() {
		return
	}
	ticker := time.NewTicker(s.config.jobsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !push() {
				return
			}
		case msg := <-messages:
			if msg.kind != websocket.MessageText {
				_ = c.Close(websocket.StatusUnsupportedData, "Text messages required")
				return
			}
			next, changed, err := subscription(msg.data, filters)
			if err != nil {
				_ = c.Close(websocket.StatusPolicyViolation, "Invalid job filters")
				return
			}
			if changed {
				filters = next
				if !push() {
					return
				}
			}
		}
	}
}
