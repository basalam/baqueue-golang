package dashboard

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

func forDynamicDrivers(t *testing.T, test func(*testing.T, baqueue.Driver, baqueue.DynamicQueueDriver)) {
	t.Helper()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var driver baqueue.Driver = baqueue.NewMemoryDriver()
			if backend == "sqlite" {
				driver = baqueue.NewSQLiteDriver(filepath.Join(t.TempDir(), "queue.db"))
			}
			if err := driver.Connect(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := driver.Disconnect(context.Background()); err != nil {
					t.Error(err)
				}
			})
			dynamic, ok := driver.(baqueue.DynamicQueueDriver)
			if !ok {
				t.Fatal("missing dynamic queue extension")
			}
			test(t, driver, dynamic)
		})
	}
}

func expireDynamicDefinition(t *testing.T, driver baqueue.DynamicQueueDriver, name string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		expired, err := driver.ExpireDynamicQueues(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(expired, name) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("idle definition did not expire")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDynamicQueueMetadata(t *testing.T) {
	forDynamicDrivers(t, func(t *testing.T, driver baqueue.Driver, dynamic baqueue.DynamicQueueDriver) {
		info, err := dynamic.RegisterDynamicQueue(context.Background(), "tenant-empty", 30)
		if err != nil {
			t.Fatal(err)
		}
		seed(t, driver, "dynamic-static", baqueue.Pending, baqueue.Now())
		h := New(driver)
		wantMetadata := map[string]any{
			"name": info.Name, "idle_timeout_seconds": info.IdleTimeoutSeconds,
			"created_at": info.CreatedAt, "idle_since": *info.IdleSince, "active": true,
		}
		detail := request(t, h, "GET", "/api/queues/tenant-empty", 200)
		if !reflect.DeepEqual(detail["dynamic"], wantMetadata) || detail["size"] != float64(0) {
			t.Fatalf("empty queue detail: %#v", detail)
		}
		listing := request(t, h, "GET", "/api/queues", 200)
		if !reflect.DeepEqual(listing["queues"], []any{"dynamic-static", "tenant-empty"}) {
			t.Fatalf("queue list: %#v", listing)
		}
		overview := request(t, h, "GET", "/api/overview", 200)
		found := false
		for _, item := range overview["queues"].([]any) {
			queue := item.(map[string]any)
			if queue["name"] == "tenant-empty" {
				found = true
				if !reflect.DeepEqual(queue["dynamic"], wantMetadata) {
					t.Fatalf("overview metadata: %#v", queue)
				}
				for _, status := range statuses {
					if queue[status] != float64(0) {
						t.Fatalf("empty queue count: %#v", queue)
					}
				}
			}
		}
		if !found || overview["totals"].(map[string]any)["total"] != float64(1) {
			t.Fatalf("overview: %#v", overview)
		}
		for _, name := range []string{"dynamic-static", "missing"} {
			detail := request(t, h, "GET", "/api/queues/"+name, 200)
			if value, exists := detail["dynamic"]; !exists || value != nil {
				t.Fatalf("non-dynamic metadata: %#v", detail)
			}
		}
	})
}

func TestDynamicExpiredHistoryTotalsAndRetry(t *testing.T) {
	for _, status := range []string{baqueue.Completed, baqueue.Failed} {
		t.Run(status, func(t *testing.T) {
			forDynamicDrivers(t, func(t *testing.T, driver baqueue.Driver, dynamic baqueue.DynamicQueueDriver) {
				if _, err := dynamic.RegisterDynamicQueue(context.Background(), "tenant-history", 0.001); err != nil {
					t.Fatal(err)
				}
				job := seed(t, driver, "tenant-history", status, baqueue.Now())
				seed(t, driver, "static", baqueue.Pending, baqueue.Now())
				h := New(driver)
				before := request(t, h, "GET", "/api/jobs/"+job.ID, 200)
				expireDynamicDefinition(t, dynamic, job.Queue)
				overview := request(t, h, "GET", "/api/overview", 200)
				wantTotals := map[string]any{"pending": float64(1), "processing": float64(0), "completed": float64(0), "failed": float64(0), "queues": float64(1), "total": float64(2)}
				wantTotals[status] = float64(1)
				if !reflect.DeepEqual(overview["totals"], wantTotals) {
					t.Fatalf("historical totals: %#v", overview["totals"])
				}
				queues := overview["queues"].([]any)
				if len(queues) != 1 || queues[0].(map[string]any)["name"] != "static" {
					t.Fatalf("active queue list: %#v", queues)
				}
				detail := request(t, h, "GET", "/api/queues/"+job.Queue, 200)
				if detail["dynamic"] != nil || detail["metrics"].(map[string]any)[status] != float64(1) {
					t.Fatalf("archived queue detail: %#v", detail)
				}
				if after := request(t, h, "GET", "/api/jobs/"+job.ID, 200); !reflect.DeepEqual(before, after) {
					t.Fatal("expiry changed retained job details")
				}
				if status == baqueue.Failed {
					if request(t, h, "POST", "/api/jobs/"+job.ID+"/retry", 200)["success"] != true {
						t.Fatal("retry failed")
					}
					detail = request(t, h, "GET", "/api/queues/"+job.Queue, 200)
					metadata := detail["dynamic"].(map[string]any)
					if metadata["active"] != true || metadata["idle_since"] != nil || metadata["idle_timeout_seconds"] != 0.001 {
						t.Fatalf("reactivated metadata: %#v", metadata)
					}
				}
			})
		})
	}
}

type dynamicMetadataErrorDriver struct{ *baqueue.MemoryDriver }

func (d dynamicMetadataErrorDriver) GetDynamicQueues(context.Context) ([]baqueue.DynamicQueueInfo, error) {
	return nil, errors.New("metadata unavailable")
}

func TestDynamicMetadataErrors(t *testing.T) {
	h := New(dynamicMetadataErrorDriver{baqueue.NewMemoryDriver()})
	for _, path := range []string{"/api/overview", "/api/queues/example"} {
		request(t, h, "GET", path, 500)
	}
}

type staticOnlyDriver struct{ baqueue.Driver }

func TestDynamicMetadataOptionalDriverExtension(t *testing.T) {
	driver := baqueue.NewMemoryDriver()
	seed(t, driver, "static", baqueue.Pending, baqueue.Now())
	h := New(staticOnlyDriver{driver})
	if detail := request(t, h, "GET", "/api/queues/static", 200); detail["dynamic"] != nil {
		t.Fatalf("unexpected metadata: %#v", detail)
	}
	if overview := request(t, h, "GET", "/api/overview", 200); overview["totals"].(map[string]any)["total"] != float64(1) {
		t.Fatalf("static totals: %#v", overview)
	}
}
