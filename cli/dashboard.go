package cli

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
	"github.com/basalam/baqueue-golang/dashboard"
)

// Server.Shutdown does not wait for hijacked WebSocket handlers. Track every
// admitted handler, and stop admissions before waiting to avoid Add/Wait races.
type dashboardRequests struct {
	next    http.Handler
	mu      sync.Mutex
	closing bool
	active  sync.WaitGroup
}

func (h *dashboardRequests) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		http.Error(w, "Dashboard shutting down", http.StatusServiceUnavailable)
		return
	}
	h.active.Add(1)
	h.mu.Unlock()
	defer h.active.Done()
	h.next.ServeHTTP(w, r)
}

func serveDashboard(ctx context.Context, listener net.Listener, driver baqueue.Driver, grace time.Duration) error {
	feedCtx, cancelFeeds := context.WithCancel(context.Background())
	defer cancelFeeds()
	handler := &dashboardRequests{next: dashboard.New(driver, dashboard.WithContext(feedCtx))}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var serveErr error
	finished := false
	select {
	case <-ctx.Done():
	case serveErr = <-served:
		finished = true
	}
	handler.mu.Lock()
	handler.closing = true
	handler.mu.Unlock()
	cancelFeeds()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), grace)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	if !finished {
		serveErr = <-served
	}
	// Close cancels remaining HTTP request contexts after the grace period.
	// Drivers must honor cancellation; never disconnect storage under a handler.
	handler.active.Wait()
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr)
}
