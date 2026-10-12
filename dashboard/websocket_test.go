package dashboard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func dial(t *testing.T, serverURL, path string, headers http.Header) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverURL, "http")+path, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatalf("dial: %v (%v)", err, response)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func TestLifecycleContextClosesFeedsAndRejectsNewHandshakes(t *testing.T) {
	for _, path := range []string{"/ws", "/ws/jobs"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan struct{})
			h := New(baqueue.NewMemoryDriver(), WithContext(ctx), WithAuth(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == path && ctx.Err() == nil {
						defer close(finished)
					}
					next.ServeHTTP(w, r)
				})
			}))
			srv := httptest.NewServer(h)
			defer srv.Close()
			c := dial(t, srv.URL, path, nil)
			readSnapshot(t, c)
			cancel()
			closeStatus(t, c, websocket.StatusGoingAway)
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not clean up")
			}
			r := httptest.NewRecorder()
			h.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
			if r.Code != http.StatusServiceUnavailable {
				t.Fatalf("handshake after shutdown: %d", r.Code)
			}
			request(t, h, "GET", "/api/jobs", 200)
		})
	}
}

type drainingOverviewDriver struct {
	baqueue.Driver
	armed                       atomic.Bool
	started, cancelled, release chan struct{}
}

func (d *drainingOverviewDriver) Queues(ctx context.Context) ([]string, error) {
	if !d.armed.Load() {
		return d.Driver.Queues(ctx)
	}
	close(d.started)
	<-ctx.Done()
	close(d.cancelled)
	<-d.release
	return nil, ctx.Err()
}

func TestLifecycleWaitsForOverviewBroadcasterCleanup(t *testing.T) {
	d := &drainingOverviewDriver{Driver: baqueue.NewMemoryDriver(), started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	srv := httptest.NewServer(New(d, WithContext(ctx), WithRefreshIntervals(10*time.Millisecond, time.Hour), WithAuth(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(finished); next.ServeHTTP(w, r) })
	})))
	defer srv.Close()
	var release sync.Once
	defer release.Do(func() { close(d.release) })
	c := dial(t, srv.URL, "/ws", nil)
	readSnapshot(t, c)
	d.armed.Store(true)
	select {
	case <-d.started:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcaster did not query")
	}
	cancel()
	closeStatus(t, c, websocket.StatusGoingAway)
	select {
	case <-d.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcaster query was not cancelled")
	}
	select {
	case <-finished:
		t.Fatal("handler exited while broadcaster was still using driver")
	default:
	}
	release.Do(func() { close(d.release) })
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not await broadcaster")
	}
}

func TestLifecycleCancelsQueryBeforeWebSocketCloseReply(t *testing.T) {
	d := &observedDriver{Driver: baqueue.NewMemoryDriver(), block: true, cancelled: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := httptest.NewServer(New(d, WithContext(ctx)))
	defer srv.Close()
	c := dial(t, srv.URL, "/ws/jobs", nil)
	eventually(t, func() bool { return d.calls.Load() == 1 })
	cancel()
	select {
	case <-d.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("query cancellation waited for the peer's close reply")
	}
	closeStatus(t, c, websocket.StatusGoingAway)
}

func readSnapshot(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result map[string]any
	if err := wsjson.Read(ctx, c, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func send(t *testing.T, c *websocket.Conn, value any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, c, value); err != nil {
		t.Fatal(err)
	}
}

func closeStatus(t *testing.T, c *websocket.Conn, want websocket.StatusCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := c.Read(ctx)
	if websocket.CloseStatus(err) != want {
		t.Fatalf("close status %d, want %d: %v", websocket.CloseStatus(err), want, err)
	}
}

func TestWebSocketOverviewAndPeriodicJobs(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	srv := httptest.NewServer(New(d, WithRefreshIntervals(20*time.Millisecond, 20*time.Millisecond)))
	defer srv.Close()
	overview := dial(t, srv.URL, "/ws", nil)
	if readSnapshot(t, overview)["totals"].(map[string]any)["total"] != float64(0) {
		t.Fatal("nonempty initial overview")
	}
	jobs := dial(t, srv.URL, "/ws/jobs", nil)
	if readSnapshot(t, jobs)["count"] != float64(0) {
		t.Fatal("nonempty initial jobs")
	}
	seed(t, d, "emails", baqueue.Pending, baqueue.Now(), "urgent")
	for {
		if readSnapshot(t, overview)["totals"].(map[string]any)["total"] == float64(1) {
			break
		}
	}
	for {
		if readSnapshot(t, jobs)["total"] == float64(1) {
			break
		}
	}
	_ = overview.Close(websocket.StatusNormalClosure, "")
	_ = jobs.Close(websocket.StatusNormalClosure, "")
}

func TestWebSocketSubscriptionsArePartialAndIsolated(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	p := seed(t, d, "emails", baqueue.Failed, 200, "urgent")
	seed(t, d, "emails", baqueue.Failed, 300, "routine")
	seed(t, d, "other", baqueue.Failed, 400, "urgent")
	srv := httptest.NewServer(New(d, WithRefreshIntervals(time.Hour, time.Hour)))
	defer srv.Close()
	a, b := dial(t, srv.URL, "/ws/jobs", nil), dial(t, srv.URL, "/ws/jobs", nil)
	readSnapshot(t, a)
	readSnapshot(t, b)
	send(t, a, map[string]any{"type": "subscribe", "filters": map[string]any{"queue": "emails", "tag": "urgent", "status": "failed", "batch_id": "batch-one", "created_from": "100", "created_to": 250, "per_page": "1", "page": 1}})
	snapshot := readSnapshot(t, a)
	if snapshot["total"] != float64(1) || snapshot["per_page"] != float64(1) {
		t.Fatalf("filter: %#v", snapshot)
	}
	job := snapshot["jobs"].([]any)[0].(map[string]any)
	if job["id"] != p.ID {
		t.Fatalf("job: %#v", job)
	}
	if _, exists := job["history"]; exists {
		t.Fatal("websocket jobs include history")
	}
	send(t, b, map[string]any{"type": "subscribe", "filters": map[string]any{"queue": "other"}})
	if readSnapshot(t, b)["total"] != float64(1) {
		t.Fatal("subscription leaked across connections")
	}
	send(t, a, map[string]any{"type": "subscribe", "filters": map[string]any{"tag": nil, "created_to": "", "page": nil}})
	if readSnapshot(t, a)["total"] != float64(2) {
		t.Fatal("partial subscription did not retain queue or clear optional filters")
	}
	send(t, a, map[string]any{"type": "ignored"})
	send(t, a, map[string]any{"type": "subscribe", "filters": map[string]any{"queue": nil, "created_from": nil}})
	if readSnapshot(t, a)["total"] != float64(3) {
		t.Fatal("null filter was not cleared")
	}
}

func TestWebSocketPrefixAuthAndOrigin(t *testing.T) {
	srv := httptest.NewServer(New(baqueue.NewMemoryDriver(), WithPrefix("/private/queues"), WithAuth(auth)))
	defer srv.Close()
	for _, path := range []string{"/private/queues/ws", "/private/queues/ws/jobs"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+path, nil)
		cancel()
		if err == nil || response == nil || response.StatusCode != 401 {
			t.Fatalf("unauthenticated handshake: %v %v", response, err)
		}
		headers := http.Header{"Authorization": []string{"Bearer test"}, "Origin": []string{srv.URL}}
		c := dial(t, srv.URL, path, headers)
		readSnapshot(t, c)
		_ = c.CloseNow()
		headers.Set("Origin", "https://attacker.example")
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		_, response, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+path, &websocket.DialOptions{HTTPHeader: headers})
		cancel()
		if err == nil || response == nil || response.StatusCode != 403 {
			t.Fatalf("cross-origin handshake: %v %v", response, err)
		}
	}
	trusted := httptest.NewServer(New(baqueue.NewMemoryDriver(), WithOriginPatterns("trusted.example")))
	defer trusted.Close()
	c := dial(t, trusted.URL, "/ws", http.Header{"Origin": []string{"https://trusted.example"}})
	readSnapshot(t, c)
}

func TestWebSocketInvalidMessagesAndDriverErrors(t *testing.T) {
	srv := httptest.NewServer(New(baqueue.NewMemoryDriver(), WithRefreshIntervals(time.Hour, time.Hour)))
	defer srv.Close()
	for _, message := range []string{
		`{`,
		`{"type":"subscribe","filters":[]}`,
		`{"type":"subscribe","filters":{"page":0}}`,
		`{"type":"subscribe","filters":{"per_page":1001}}`,
		`{"type":"subscribe","filters":{"page":true}}`,
		`{"type":"subscribe","filters":{"queue":123}}`,
		`{"type":"subscribe","filters":{"created_from":"NaN"}}`,
		`{"type":"subscribe","filters":{"created_from":3,"created_to":1}}`,
	} {
		t.Run(message, func(t *testing.T) {
			c := dial(t, srv.URL, "/ws/jobs", nil)
			readSnapshot(t, c)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.Write(ctx, websocket.MessageText, []byte(message)); err != nil {
				t.Fatal(err)
			}
			closeStatus(t, c, websocket.StatusPolicyViolation)
		})
	}
	for _, path := range []string{"/ws", "/ws/jobs"} {
		c := dial(t, srv.URL, path, nil)
		readSnapshot(t, c)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := c.Write(ctx, websocket.MessageBinary, []byte{0})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		closeStatus(t, c, websocket.StatusUnsupportedData)
	}
	failing := httptest.NewServer(New(errorDriver{baqueue.NewMemoryDriver(), errors.New("private backend detail")}))
	defer failing.Close()
	c := dial(t, failing.URL, "/ws/jobs", nil)
	closeStatus(t, c, websocket.StatusInternalError)
}

func TestWebSocketCancelsPendingQueryOnDisconnect(t *testing.T) {
	d := &observedDriver{Driver: baqueue.NewMemoryDriver(), block: true, cancelled: make(chan struct{})}
	srv := httptest.NewServer(New(d))
	defer srv.Close()
	c := dial(t, srv.URL, "/ws/jobs", nil)
	eventually(t, func() bool { return d.calls.Load() == 1 })
	_ = c.CloseNow()
	select {
	case <-d.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("driver query survived websocket disconnect")
	}
}

func TestWebSocketStopsPollingOnDisconnect(t *testing.T) {
	d := &observedDriver{Driver: baqueue.NewMemoryDriver()}
	var active atomic.Int64
	h := New(d, WithRefreshIntervals(10*time.Millisecond, 10*time.Millisecond), WithAuth(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			active.Add(1)
			defer active.Add(-1)
			next.ServeHTTP(w, r)
		})
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := dial(t, srv.URL, "/ws/jobs", nil)
	readSnapshot(t, c)
	eventually(t, func() bool { return d.calls.Load() >= 2 })
	_ = c.CloseNow()
	eventually(t, func() bool { return active.Load() == 0 })
	before := d.calls.Load()
	time.Sleep(40 * time.Millisecond)
	if d.calls.Load() != before {
		t.Fatal("jobs continued polling after cleanup")
	}
}

func TestOverviewHubLifecycle(t *testing.T) {
	s := &server{driver: baqueue.NewMemoryDriver(), config: config{overviewInterval: 10 * time.Millisecond}}
	a, stopA := s.subscribeOverview()
	b, stopB := s.subscribeOverview()
	for _, ch := range []<-chan snapshot{a, b} {
		select {
		case update := <-ch:
			if update.err != nil {
				t.Fatal(update.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("broadcast not received")
		}
	}
	stopA()
	s.hub.mu.Lock()
	remaining := len(s.hub.subscribers)
	s.hub.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("remaining subscribers: %d", remaining)
	}
	stopB()
	s.hub.mu.Lock()
	remaining = len(s.hub.subscribers)
	s.hub.mu.Unlock()
	if remaining != 0 {
		t.Fatal("subscriber leaked")
	}
	c, stopC := s.subscribeOverview()
	defer stopC()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatal("hub did not restart")
	}
}
