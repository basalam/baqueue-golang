package cli

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type blockingDashboardDriver struct {
	baqueue.Driver
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
	active   atomic.Int64
}

func (d *blockingDashboardDriver) GetJobs(ctx context.Context, f baqueue.JobFilter) ([]*baqueue.JobPayload, error) {
	d.active.Add(1)
	defer func() { d.active.Add(-1); close(d.finished) }()
	close(d.started)
	select {
	case <-d.release:
		return d.Driver.GetJobs(ctx, f)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func startDashboard(t *testing.T, d baqueue.Driver, grace time.Duration) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveDashboard(ctx, listener, d, grace); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = listener.Close()
			t.Error("dashboard did not stop")
		}
	})
	return "http://" + listener.Addr().String(), cancel, done
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handler")
	}
}

func TestDashboardShutdownDrainsHTTPBeforeReturning(t *testing.T) {
	d := &blockingDashboardDriver{Driver: baqueue.NewMemoryDriver(), started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	url, cancel, stopped := startDashboard(t, d, time.Second)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(d.release) }) })
	response := make(chan error, 1)
	go func() {
		client := http.Client{Timeout: 3 * time.Second}
		r, err := client.Get(url + "/api/jobs")
		if err == nil {
			defer r.Body.Close()
			_, err = io.Copy(io.Discard, r.Body)
			if r.StatusCode != http.StatusOK {
				err = errors.New("request was not drained gracefully")
			}
		}
		response <- err
	}()
	waitSignal(t, d.started)
	cancel()
	select {
	case err := <-stopped:
		t.Fatalf("returned while request was active: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	release.Do(func() { close(d.release) })
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	if d.active.Load() != 0 {
		t.Fatal("driver is still serving a request")
	}
}

func TestDashboardShutdownCancelsHTTPAfterGracePeriod(t *testing.T) {
	d := &blockingDashboardDriver{Driver: baqueue.NewMemoryDriver(), started: make(chan struct{}), finished: make(chan struct{})}
	url, cancel, stopped := startDashboard(t, d, 25*time.Millisecond)
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		client := http.Client{Timeout: 3 * time.Second}
		if response, err := client.Get(url + "/api/jobs"); err == nil {
			response.Body.Close()
		}
	}()
	waitSignal(t, d.started)
	cancel()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected grace timeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not cancel request")
	}
	waitSignal(t, d.finished)
	waitSignal(t, requestDone)
	if d.active.Load() != 0 {
		t.Fatal("shutdown returned before request cleanup")
	}
}

func TestDashboardShutdownClosesWebSocketsAndWaitsForQueries(t *testing.T) {
	d := &blockingDashboardDriver{Driver: baqueue.NewMemoryDriver(), started: make(chan struct{}), finished: make(chan struct{})}
	url, cancel, stopped := startDashboard(t, d, time.Second)
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	overview, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(url, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer overview.CloseNow()
	var initial map[string]any
	if err := wsjson.Read(ctx, overview, &initial); err != nil {
		t.Fatal(err)
	}
	jobs, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(url, "http")+"/ws/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer jobs.CloseNow()
	waitSignal(t, d.started)
	cancel()
	for _, conn := range []*websocket.Conn{overview, jobs} {
		_, _, err := conn.Read(ctx)
		if websocket.CloseStatus(err) != websocket.StatusGoingAway {
			t.Fatalf("shutdown close: %v", err)
		}
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("websocket handlers did not stop")
	}
	waitSignal(t, d.finished)
	if d.active.Load() != 0 {
		t.Fatal("driver query survived shutdown")
	}
}

func TestDashboardServeErrorCleansUp(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener.Close()
	if err := serveDashboard(context.Background(), listener, baqueue.NewMemoryDriver(), time.Second); err == nil {
		t.Fatal("listener error was discarded")
	}
}
