package baqueue

import (
	"context"
	"fmt"
	"math"
)

type JobFilter struct {
	Queue       string
	Status      string
	Tag         string
	BatchID     string
	CreatedFrom *float64
	CreatedTo   *float64
	Offset      int
	Limit       int // 0 means 50; negative means unbounded for internal maintenance.
}

type PruneOptions struct {
	Queue            string
	Status           string
	Tag              string
	OlderThanSeconds *float64
	Limit            int // 0 means no cap.
}

func (o PruneOptions) Validate() error {
	if o.Limit < 0 {
		return fmt.Errorf("baqueue: prune limit must be nonnegative")
	}
	if o.OlderThanSeconds != nil {
		age := *o.OlderThanSeconds
		if age < 0 || math.IsNaN(age) || math.IsInf(age, 0) {
			return fmt.Errorf("baqueue: prune age must be finite and nonnegative")
		}
	}
	return nil
}

// Driver implementations must serialize claims and lifecycle transitions.
// Complete/Fail/Release reject stale processing attempts with ErrConflict.
// Release preserves attempts, max_attempts, backoff and history from storage,
// appending the caller's history for the current processing attempt only.
// Successful lifecycle writes refresh the supplied payload. Complete and Fail
// commit the first batch-member outcome in the same transaction as job state.
type Driver interface {
	Connect(context.Context) error
	Disconnect(context.Context) error
	Push(context.Context, *JobPayload) (string, error)
	PushMany(context.Context, []*JobPayload) ([]string, error)
	Pop(context.Context, string) (*JobPayload, error)
	PopDelayed(context.Context) ([]*JobPayload, error)
	Complete(context.Context, *JobPayload) error
	Fail(context.Context, *JobPayload, string) error
	Release(context.Context, *JobPayload, float64) error
	RequeueStuckJobs(context.Context, float64, string) (int, error)
	Delete(context.Context, string) error
	Promote(context.Context, string) (bool, error)
	GetJob(context.Context, string) (*JobPayload, error)
	GetJobs(context.Context, JobFilter) ([]*JobPayload, error)
	CountJobs(context.Context, JobFilter) (int, error)
	Size(context.Context, string) (int, error)
	Queues(context.Context) ([]string, error)
	RecordMetric(context.Context, string, string, float64) error
	GetMetrics(context.Context, string) (map[string]any, error)
	RecentThroughput(context.Context, int, string) (map[string]int, error)
	ReportSupervisor(context.Context, map[string]any) error
	GetSupervisorStats(context.Context, float64) ([]map[string]any, error)
	StoreBatch(context.Context, string, map[string]any) error
	GetBatch(context.Context, string) (map[string]any, error)
	GetPendingBatches(context.Context, int) ([]map[string]any, error)
	UpdateBatch(context.Context, string, map[string]any) error
	IncrementBatchCounter(context.Context, string, string, int) (map[string]any, error)
	Prune(context.Context, PruneOptions) (int, error)
	BulkDeleteJobs(context.Context, []string, int) (int, error)
	PruneTerminalJobs(context.Context, PruneOptions) (int, error)
	ReconcileIndexes(context.Context, int) (int, error)
	ReconcilePendingJobs(context.Context, int) (int, error)
	Flush(context.Context, string) error
	PruneMetrics(context.Context, float64) (int, error)
}

// MergeAttempt preserves stored policy; only an owned attempt may update history.
func MergeAttempt(stored, incoming *JobPayload) error {
	if stored == nil {
		return ErrNotFound
	}
	if stored.Status == Processing {
		if incoming.Status != Processing || stored.Attempts != incoming.Attempts ||
			stored.StartedAt == nil || incoming.StartedAt == nil || *stored.StartedAt != *incoming.StartedAt {
			return ErrConflict
		}
		for _, attempt := range incoming.History {
			if attempt.Attempt != stored.Attempts || attempt.StartedAt != *stored.StartedAt {
				continue
			}
			present := false
			for _, previous := range stored.History {
				if previous.Attempt == attempt.Attempt && previous.StartedAt == attempt.StartedAt {
					present = true
					break
				}
			}
			if !present {
				stored.History = append(stored.History, attempt)
			}
		}
	} else if incoming.Status != stored.Status || incoming.Attempts != stored.Attempts {
		return ErrConflict
	}
	return nil
}
