package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

func request(t *testing.T, h http.Handler, method, path string, code int) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	if w.Code != code {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, w.Code, code, w.Body.String())
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("not JSON: %s", w.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func seed(t *testing.T, d baqueue.Driver, queue, status string, created float64, tags ...string) *baqueue.JobPayload {
	t.Helper()
	p := baqueue.NewPayload("example.Job", map[string]any{"message": "hello"})
	p.Queue, p.CreatedAt, p.Tags, p.MaxAttempts = queue, created, tags, 7
	batch := "batch-one"
	p.BatchID = &batch
	p.Backoff = baqueue.Backoff{Delays: []float64{1, 5, 10}}
	if _, err := d.Push(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if status != baqueue.Pending {
		claimed, err := d.Pop(context.Background(), queue)
		if err != nil || claimed == nil || claimed.ID != p.ID {
			t.Fatalf("claim: %v, %v", claimed, err)
		}
		p = claimed
		if status != baqueue.Processing {
			errText := "original failure"
			p.History = []baqueue.Attempt{{Attempt: p.Attempts, StartedAt: *p.StartedAt, FinishedAt: baqueue.Now(), Duration: 0.12, Status: status, Error: &errText}}
			if status == baqueue.Failed {
				err = d.Fail(context.Background(), p, errText)
			} else {
				err = d.Complete(context.Background(), p)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	p, err := d.GetJob(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadEndpoints(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	ctx := context.Background()
	now := baqueue.Now()
	failed := seed(t, d, "emails", baqueue.Failed, now-10, "urgent")
	seed(t, d, "emails", baqueue.Completed, now-5, "routine")
	seed(t, d, "other", baqueue.Processing, now-120)
	seed(t, d, "emails", baqueue.Pending, now-2, "urgent")
	for _, status := range []string{baqueue.Processing, baqueue.Completed, baqueue.Failed} {
		if err := d.RecordMetric(ctx, "emails", status, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.ReportSupervisor(ctx, map[string]any{"name": "worker-one", "running": true, "workers": 2}); err != nil {
		t.Fatal(err)
	}
	if err := d.StoreBatch(ctx, "batch-one", map[string]any{"id": "batch-one", "total_jobs": 4, "pending_jobs": 2}); err != nil {
		t.Fatal(err)
	}
	h := New(d)
	overview := request(t, h, "GET", "/api/overview", 200)
	wantTotals := map[string]any{"pending": float64(1), "processing": float64(1), "completed": float64(1), "failed": float64(1), "queues": float64(2), "total": float64(4)}
	if !reflect.DeepEqual(overview["totals"], wantTotals) {
		t.Fatalf("totals: %#v", overview["totals"])
	}
	rates := overview["rates"].(map[string]any)
	if rates["pending_per_min"] != float64(3) || rates["completed_per_min"] != float64(1) {
		t.Fatalf("rates: %#v", rates)
	}
	if len(overview["queues"].([]any)) != 2 || len(overview["supervisors"].([]any)) != 1 {
		t.Fatalf("overview: %#v", overview)
	}
	if overview["timestamp"].(float64) < now {
		t.Fatal("invalid timestamp")
	}
	stats := request(t, h, "GET", fmt.Sprintf("/api/stats?created_from=%f&created_to=%f", now-60, now), 200)
	if stats["total"] != float64(3) || stats["statuses"].(map[string]any)["processing"] != float64(0) {
		t.Fatalf("stats: %#v", stats)
	}
	queues := request(t, h, "GET", "/api/queues", 200)
	if len(queues["queues"].([]any)) != 2 {
		t.Fatalf("queues: %#v", queues)
	}
	detail := request(t, h, "GET", "/api/queues/emails", 200)
	if detail["name"] != "emails" || detail["size"] != float64(1) || detail["metrics"].(map[string]any)["failed"] != float64(1) {
		t.Fatalf("queue detail: %#v", detail)
	}
	if len(request(t, h, "GET", "/api/metrics", 200)) != 2 {
		t.Fatal("metrics missing queues")
	}
	if len(request(t, h, "GET", "/api/supervisors", 200)["supervisors"].([]any)) != 1 {
		t.Fatal("supervisor missing")
	}
	batch := request(t, h, "GET", "/api/batches/batch-one", 200)
	if batch["id"] != "batch-one" || batch["total_jobs"] != float64(4) {
		t.Fatalf("batch: %#v", batch)
	}
	job := request(t, h, "GET", "/api/jobs/"+failed.ID, 200)
	if len(job["history"].([]any)) != 1 || job["attempts"] != float64(1) || job["max_attempts"] != float64(7) {
		t.Fatalf("job: %#v", job)
	}
	if request(t, h, "GET", "/api/jobs/missing", 404)["error"] != "Job not found" {
		t.Fatal("wrong job error")
	}
	if request(t, h, "GET", "/api/batches/missing", 404)["error"] != "Batch not found" {
		t.Fatal("wrong batch error")
	}
}

func TestJobsFiltersPaginationAndHistory(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	seed(t, d, "emails", baqueue.Failed, 100, "urgent")
	matching := seed(t, d, "emails", baqueue.Failed, 200, "urgent")
	seed(t, d, "emails", baqueue.Failed, 300, "routine")
	seed(t, d, "other", baqueue.Failed, 400, "urgent")
	h := New(d)
	paths := []struct {
		path         string
		count, total float64
	}{
		{"/api/jobs", 4, 4},
		{"/api/jobs?queue=emails&status=failed&tag=urgent&batch_id=batch-one&created_from=100&created_to=200&page=1&per_page=1", 1, 2},
		{"/api/jobs?tag=urgent&page=2&per_page=2", 1, 3},
		{"/api/jobs?batch_id=missing", 0, 0},
		{"/api/jobs?status=pending", 0, 0},
		{"/api/jobs?page=99", 0, 4},
	}
	for _, tc := range paths {
		t.Run(tc.path, func(t *testing.T) {
			result := request(t, h, "GET", tc.path, 200)
			if result["count"] != tc.count || result["total"] != tc.total {
				t.Fatalf("pagination: %#v", result)
			}
			for _, job := range result["jobs"].([]any) {
				if _, ok := job.(map[string]any)["history"]; ok {
					t.Fatal("list includes history")
				}
			}
		})
	}
	result := request(t, h, "GET", "/api/jobs?queue=emails&tag=urgent&per_page=1", 200)
	if result["jobs"].([]any)[0].(map[string]any)["id"] != matching.ID {
		t.Fatal("ordering changed")
	}
}

func TestMutationsPreserveRetryPolicy(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	p := seed(t, d, "emails", baqueue.Failed, 100, "urgent")
	h := New(d)
	if request(t, h, "POST", "/api/jobs/"+p.ID+"/retry", 200)["success"] != true {
		t.Fatal("retry failed")
	}
	after, err := d.GetJob(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != baqueue.Pending || after.Attempts != p.Attempts || after.MaxAttempts != p.MaxAttempts || !reflect.DeepEqual(after.Backoff, p.Backoff) || !reflect.DeepEqual(after.History, p.History) {
		t.Fatalf("retry lost policy/history: %#v", after)
	}
	if request(t, h, "POST", "/api/jobs/"+p.ID+"/retry", 200)["success"] != false {
		t.Fatal("retried pending job")
	}
	if request(t, h, "POST", "/api/jobs/missing/retry", 200)["success"] != false {
		t.Fatal("retried nonexistent job")
	}
	delayed := baqueue.NewPayload("example.Job", nil)
	delayed.Queue = "scheduled"
	later := baqueue.Now() + 3600
	delayed.DelayUntil = &later
	if _, err := d.Push(context.Background(), delayed); err != nil {
		t.Fatal(err)
	}
	if request(t, h, "POST", "/api/jobs/"+delayed.ID+"/execute", 200)["success"] != true {
		t.Fatal("execute failed")
	}
	claimed, err := d.Pop(context.Background(), "scheduled")
	if err != nil || claimed == nil || claimed.ID != delayed.ID {
		t.Fatalf("not promoted: %v %v", claimed, err)
	}
	if request(t, h, "POST", "/api/jobs/missing/execute", 200)["success"] != false {
		t.Fatal("promoted missing job")
	}
	request(t, h, "DELETE", "/api/jobs/"+p.ID, 200)
	request(t, h, "GET", "/api/jobs/"+p.ID, 404)
	request(t, h, "DELETE", "/api/jobs/missing", 200)
}

func TestRetryFailedFiltersAndBatches(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	var original *baqueue.JobPayload
	for i := 0; i < 205; i++ {
		original = seed(t, d, "emails", baqueue.Failed, 200, "urgent")
	}
	seed(t, d, "emails", baqueue.Failed, 200, "routine")
	seed(t, d, "other", baqueue.Failed, 200, "urgent")
	seed(t, d, "emails", baqueue.Failed, 50, "urgent")
	h := New(d)
	result := request(t, h, "POST", "/api/jobs/retry-failed?queue=emails&tag=urgent&created_from=100&created_to=300", 200)
	if result["retried"] != float64(205) {
		t.Fatalf("retry batches: %#v", result)
	}
	after, err := d.GetJob(context.Background(), original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Attempts != original.Attempts || after.MaxAttempts != original.MaxAttempts || !reflect.DeepEqual(after.History, original.History) {
		t.Fatal("bulk retry lost attempts/history")
	}
	remaining, err := d.CountJobs(context.Background(), baqueue.JobFilter{Status: baqueue.Failed})
	if err != nil || remaining != 3 {
		t.Fatalf("remaining: %d %v", remaining, err)
	}
	if request(t, h, "POST", "/api/jobs/retry-failed?queue=missing", 200)["retried"] != float64(0) {
		t.Fatal("unexpected retries")
	}
}

type pruneRecorder struct {
	baqueue.Driver
	options baqueue.PruneOptions
}

type conflictingReleaseDriver struct {
	baqueue.Driver
	conflicts map[string]bool
}

func (d conflictingReleaseDriver) Release(ctx context.Context, p *baqueue.JobPayload, delay float64) error {
	if d.conflicts[p.ID] {
		return baqueue.ErrConflict
	}
	return d.Driver.Release(ctx, p, delay)
}

func TestBulkRetryDoesNotStopAtConflictingPage(t *testing.T) {
	d := conflictingReleaseDriver{Driver: baqueue.NewMemoryDriver(), conflicts: make(map[string]bool)}
	for i := 0; i < 205; i++ {
		p := seed(t, d.Driver, "emails", baqueue.Failed, float64(i))
		if i >= 5 {
			d.conflicts[p.ID] = true
		}
	}
	result := request(t, New(d), "POST", "/api/jobs/retry-failed", 200)
	if result["retried"] != float64(5) {
		t.Fatalf("retry stopped at conflicting page: %#v", result)
	}
}

func (d *pruneRecorder) Prune(_ context.Context, options baqueue.PruneOptions) (int, error) {
	d.options = options
	return 3, nil
}

func TestPrune(t *testing.T) {
	d := &pruneRecorder{Driver: baqueue.NewMemoryDriver()}
	h := New(d)
	result := request(t, h, "POST", "/api/prune?status=completed&tag=expired&hours=2.5", 200)
	if result["pruned"] != float64(3) || d.options.Status != "completed" || d.options.Tag != "expired" || d.options.OlderThanSeconds == nil || *d.options.OlderThanSeconds != 9000 {
		t.Fatalf("prune: %#v %#v", result, d.options)
	}
	request(t, h, "POST", "/api/prune?hours=0", 200)
	if d.options.OlderThanSeconds != nil {
		t.Fatal("zero hours should not apply age filter")
	}
}

func TestValidation(t *testing.T) {
	h := New(baqueue.NewMemoryDriver())
	for _, query := range []string{"page=0", "page=-1", "page=no", "per_page=0", "per_page=1001", "per_page=1.2", "page=9223372036854775807&per_page=1000", "status=bogus", "created_from=NaN", "created_to=Inf", "created_from=3&created_to=2"} {
		t.Run(query, func(t *testing.T) { request(t, h, "GET", "/api/jobs?"+query, 422) })
	}
	request(t, h, "GET", "/api/stats?created_from=bad", 422)
	request(t, h, "POST", "/api/jobs/retry-failed?created_to=NaN", 422)
	for _, query := range []string{"hours=-1", "hours=NaN", "hours=Inf", "hours=1e300", "status=bogus"} {
		request(t, h, "POST", "/api/prune?"+query, 422)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/jobs/id/retry", nil))
	if w.Code != 405 {
		t.Fatalf("GET mutated route: %d", w.Code)
	}
}

type errorDriver struct {
	baqueue.Driver
	err error
}

func (d errorDriver) GetJobs(context.Context, baqueue.JobFilter) ([]*baqueue.JobPayload, error) {
	return nil, d.err
}
func (d errorDriver) Release(context.Context, *baqueue.JobPayload, float64) error { return d.err }

func TestErrorsAreSanitized(t *testing.T) {
	d := baqueue.NewMemoryDriver()
	p := seed(t, d, "default", baqueue.Failed, 100)
	for _, tc := range []struct {
		err     error
		code    int
		message string
	}{
		{errors.New("secret database credentials"), 500, "Internal server error"},
		{baqueue.ErrConflict, 409, "Job state changed"},
		{baqueue.ErrNotFound, 404, "Job not found"},
	} {
		h := New(errorDriver{d, tc.err})
		result := request(t, h, "POST", "/api/jobs/"+p.ID+"/retry", tc.code)
		if result["error"] != tc.message {
			t.Fatalf("error: %#v", result)
		}
	}
	request(t, New(errorDriver{d, errors.New("secret")}), "GET", "/api/jobs", 500)
}

func auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func TestPrefixAssetsAndAuth(t *testing.T) {
	h := New(baqueue.NewMemoryDriver(), WithPrefix("/admin/queue/"), WithAuth(auth))
	for _, path := range []string{"/admin/queue", "/admin/queue/", "/admin/queue/app.js", "/admin/queue/style.css", "/admin/queue/api/jobs", "/admin/queue/ws", "/admin/queue/ws/jobs"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 401 {
			t.Fatalf("unprotected %s: %d", path, w.Code)
		}
	}
	for _, tc := range []struct {
		path     string
		code     int
		contains string
	}{
		{"/admin/queue?keep=1", 307, "/admin/queue/?keep=1"},
		{"/admin/queue/", 200, `src="./app.js"`},
		{"/admin/queue/app.js", 200, `new URL("./ws/jobs", document.baseURI)`},
		{"/admin/queue/style.css", 200, "tabular-nums"},
		{"/admin/queue/api/jobs", 200, `"jobs":[]`},
		{"/api/jobs", 404, "404"},
		{"/admin/queue-other/api/jobs", 404, "404"},
		{"/admin/queue/not-an-asset", 404, "404"},
	} {
		r := httptest.NewRequest("GET", tc.path, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.contains) {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	stripped := http.StripPrefix("/mount", New(baqueue.NewMemoryDriver()))
	w := httptest.NewRecorder()
	stripped.ServeHTTP(w, httptest.NewRequest("GET", "/mount/", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `href="./style.css"`) {
		t.Fatal("StripPrefix embedding failed")
	}
}

func TestEmptyShapesAndStaticHEAD(t *testing.T) {
	h := New(baqueue.NewMemoryDriver())
	result := request(t, h, "GET", "/api/overview", 200)
	if !reflect.DeepEqual(result["queues"], []any{}) || !reflect.DeepEqual(result["supervisors"], []any{}) {
		t.Fatalf("empty overview: %#v", result)
	}
	for _, path := range []string{"/", "/app.js", "/style.css"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("HEAD", path, nil))
		if w.Code != 200 || w.Body.Len() != 0 {
			t.Fatalf("HEAD %s: %d", path, w.Code)
		}
	}
}

func TestEmbeddedFrontendPreservesCentiseconds(t *testing.T) {
	data, err := assets.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"durationTimer", "Math.floor(now / 10)", "formatDuration(diff)", `./api/jobs/`, `new URL("./ws", document.baseURI)`} {
		if !strings.Contains(string(data), part) {
			t.Fatalf("missing frontend behavior: %s", part)
		}
	}
}

// A live HTTP server exercises headers and serving outside the checkout.
func TestHTTPServer(t *testing.T) {
	srv := httptest.NewServer(New(baqueue.NewMemoryDriver()))
	defer srv.Close()
	response, err := http.Get(srv.URL + "/api/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || response.Header.Get("Cache-Control") != "no-store" || !strings.Contains(string(body), `"jobs":[]`) {
		t.Fatalf("response: %s", body)
	}
}

type observedDriver struct {
	baqueue.Driver
	calls     atomic.Int64
	cancelled chan struct{}
	block     bool
}

func (d *observedDriver) GetJobs(ctx context.Context, f baqueue.JobFilter) ([]*baqueue.JobPayload, error) {
	d.calls.Add(1)
	if d.block {
		<-ctx.Done()
		close(d.cancelled)
		return nil, ctx.Err()
	}
	return d.Driver.GetJobs(ctx, f)
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
