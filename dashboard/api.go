package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	baqueue "github.com/basalam/baqueue-golang"
)

var statuses = [...]string{baqueue.Pending, baqueue.Processing, baqueue.Completed, baqueue.Failed}

func rates(pending int, throughput map[string]int) map[string]int {
	return map[string]int{"pending_per_min": pending, "processing_per_min": throughput[baqueue.Processing], "completed_per_min": throughput[baqueue.Completed], "failed_per_min": throughput[baqueue.Failed]}
}

func (s *server) overview(ctx context.Context) (any, error) {
	queues, err := s.driver.Queues(ctx)
	if err != nil {
		return nil, err
	}
	dynamic, err := s.dynamicQueues(ctx)
	if err != nil {
		return nil, err
	}
	metrics, err := s.driver.GetMetrics(ctx, "")
	if err != nil {
		return nil, err
	}
	// Drivers may use either typed or interface-valued maps for queue metrics.
	encoded, err := json.Marshal(metrics)
	if err != nil {
		return nil, err
	}
	var counts map[string]struct {
		Pending    int
		Processing int
		Completed  int
		Failed     int
	}
	if err = json.Unmarshal(encoded, &counts); err != nil {
		return nil, err
	}
	now := baqueue.Now()
	from := now - 60
	totals := map[string]int{"pending": 0, "processing": 0, "completed": 0, "failed": 0, "queues": len(queues), "total": 0}
	for _, status := range statuses {
		count, err := s.driver.CountJobs(ctx, baqueue.JobFilter{Status: status})
		if err != nil {
			return nil, err
		}
		totals[status] = count
		totals["total"] += count
	}
	details := make([]map[string]any, 0, len(queues))
	for _, queue := range queues {
		m := counts[queue]
		pending, err := s.driver.CountJobs(ctx, baqueue.JobFilter{Queue: queue, CreatedFrom: &from})
		if err != nil {
			return nil, err
		}
		throughput, err := s.driver.RecentThroughput(ctx, 60, queue)
		if err != nil {
			return nil, err
		}
		details = append(details, map[string]any{"name": queue, "dynamic": dynamic[queue], "pending": m.Pending, "processing": m.Processing, "completed": m.Completed, "failed": m.Failed, "rates": rates(pending, throughput)})
	}
	pending, err := s.driver.CountJobs(ctx, baqueue.JobFilter{CreatedFrom: &from})
	if err != nil {
		return nil, err
	}
	throughput, err := s.driver.RecentThroughput(ctx, 60, "")
	if err != nil {
		return nil, err
	}
	supervisors, err := s.supervisors(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"totals": totals, "rates": rates(pending, throughput), "queues": details, "supervisors": supervisors, "timestamp": now}, nil
}

func (s *server) overviewHTTP(r *http.Request) (any, error) { return s.overview(r.Context()) }

func (s *server) dynamicQueues(ctx context.Context) (map[string]*baqueue.DynamicQueueInfo, error) {
	result := make(map[string]*baqueue.DynamicQueueInfo)
	driver, ok := s.driver.(baqueue.DynamicQueueDriver)
	if !ok {
		return result, nil
	}
	definitions, err := driver.GetDynamicQueues(ctx)
	if err != nil {
		return nil, err
	}
	for i := range definitions {
		result[definitions[i].Name] = &definitions[i]
	}
	return result, nil
}

func (s *server) statsHTTP(r *http.Request) (any, error) {
	from, to, err := parseDates(r.URL.Query())
	if err != nil {
		return nil, err
	}
	queues, err := s.driver.Queues(r.Context())
	if err != nil {
		return nil, err
	}
	byStatus := make(map[string]int)
	byQueue := make(map[string]map[string]int)
	total := 0
	for _, status := range statuses {
		count, err := s.driver.CountJobs(r.Context(), baqueue.JobFilter{Status: status, CreatedFrom: from, CreatedTo: to})
		if err != nil {
			return nil, err
		}
		byStatus[status] = count
		total += count
	}
	for _, queue := range queues {
		counts := map[string]int{"total": 0}
		for _, status := range statuses {
			count, err := s.driver.CountJobs(r.Context(), baqueue.JobFilter{Queue: queue, Status: status, CreatedFrom: from, CreatedTo: to})
			if err != nil {
				return nil, err
			}
			counts[status] = count
			counts["total"] += count
		}
		byQueue[queue] = counts
	}
	return map[string]any{"queues": byQueue, "statuses": byStatus, "total": total}, nil
}

func (s *server) queuesHTTP(r *http.Request) (any, error) {
	queues, err := s.driver.Queues(r.Context())
	if queues == nil {
		queues = []string{}
	}
	return map[string]any{"queues": queues}, err
}

func (s *server) queueHTTP(r *http.Request) (any, error) {
	queue := r.PathValue("queue")
	dynamic, err := s.dynamicQueues(r.Context())
	if err != nil {
		return nil, err
	}
	metrics, err := s.driver.GetMetrics(r.Context(), queue)
	if err != nil {
		return nil, err
	}
	size, err := s.driver.Size(r.Context(), queue)
	if err != nil {
		return nil, err
	}
	m := metrics[queue]
	if m == nil {
		m = map[string]any{}
	}
	return map[string]any{"name": queue, "dynamic": dynamic[queue], "size": size, "metrics": m}, nil
}

type listJob struct {
	*baqueue.JobPayload
	History []baqueue.Attempt `json:"history,omitempty"`
}

func normalizedJob(job *baqueue.JobPayload) *baqueue.JobPayload {
	copy := *job
	if copy.Data == nil {
		copy.Data = map[string]any{}
	}
	if copy.Tags == nil {
		copy.Tags = []string{}
	}
	if copy.History == nil {
		copy.History = []baqueue.Attempt{}
	}
	return &copy
}

func (s *server) jobs(ctx context.Context, query jobQuery) (any, error) {
	jobs, err := s.driver.GetJobs(ctx, query.filter)
	if err != nil {
		return nil, err
	}
	countFilter := query.filter
	countFilter.Offset, countFilter.Limit = 0, 0
	total, err := s.driver.CountJobs(ctx, countFilter)
	if err != nil {
		return nil, err
	}
	result := make([]listJob, 0, len(jobs))
	for _, job := range jobs {
		if job != nil {
			result = append(result, listJob{JobPayload: normalizedJob(job)})
		}
	}
	return map[string]any{"jobs": result, "page": query.page, "per_page": query.perPage, "count": len(result), "total": total}, nil
}

func (s *server) jobsHTTP(r *http.Request) (any, error) {
	query, err := parseJobs(r.URL.Query())
	if err != nil {
		return nil, err
	}
	return s.jobs(r.Context(), query)
}

func (s *server) jobHTTP(r *http.Request) (any, error) {
	job, err := s.driver.GetJob(r.Context(), r.PathValue("job_id"))
	if errors.Is(err, baqueue.ErrNotFound) || (err == nil && job == nil) {
		return nil, &httpError{404, "Job not found"}
	}
	if err != nil {
		return nil, err
	}
	return normalizedJob(job), nil
}

func (s *server) retryHTTP(r *http.Request) (any, error) {
	job, err := s.driver.GetJob(r.Context(), r.PathValue("job_id"))
	if errors.Is(err, baqueue.ErrNotFound) {
		return map[string]bool{"success": false}, nil
	}
	if err != nil {
		return nil, err
	}
	if job == nil || job.Status != baqueue.Failed {
		return map[string]bool{"success": false}, nil
	}
	err = s.driver.Release(r.Context(), job, 0)
	return map[string]bool{"success": err == nil}, err
}

func (s *server) retryFailedHTTP(r *http.Request) (any, error) {
	filter, err := parseFilter(r.URL.Query())
	if err != nil {
		return nil, err
	}
	filter.Status, filter.BatchID, filter.Limit = baqueue.Failed, "", 200
	count := 0
	seen := make(map[string]struct{})
	// Restart at zero after releases shrink the selection. Skip previously seen
	// pages so conflicts or immediately re-failed jobs cannot block later jobs.
	for iteration := 0; iteration < 1000; iteration++ {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		jobs, err := s.driver.GetJobs(r.Context(), filter)
		if err != nil {
			return nil, err
		}
		if len(jobs) == 0 {
			break
		}
		progress := false
		for _, job := range jobs {
			if job == nil {
				continue
			}
			if _, ok := seen[job.ID]; ok {
				continue
			}
			seen[job.ID] = struct{}{}
			progress = true
			if job.Status != baqueue.Failed {
				continue
			}
			err := s.driver.Release(r.Context(), job, 0)
			if errors.Is(err, baqueue.ErrConflict) || errors.Is(err, baqueue.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			count++
		}
		if !progress {
			filter.Offset += len(jobs)
		} else {
			filter.Offset = 0
		}
	}
	return map[string]int{"retried": count}, nil
}

func (s *server) executeHTTP(r *http.Request) (any, error) {
	ok, err := s.driver.Promote(r.Context(), r.PathValue("job_id"))
	if errors.Is(err, baqueue.ErrNotFound) {
		err = nil
		ok = false
	}
	return map[string]bool{"success": ok}, err
}

func (s *server) deleteHTTP(r *http.Request) (any, error) {
	err := s.driver.Delete(r.Context(), r.PathValue("job_id"))
	if errors.Is(err, baqueue.ErrNotFound) {
		err = nil
	}
	return map[string]bool{"success": err == nil}, err
}

func (s *server) batchHTTP(r *http.Request) (any, error) {
	batch, err := s.driver.GetBatch(r.Context(), r.PathValue("batch_id"))
	if errors.Is(err, baqueue.ErrNotFound) || (err == nil && batch == nil) {
		return nil, &httpError{404, "Batch not found"}
	}
	return batch, err
}

func (s *server) pruneHTTP(r *http.Request) (any, error) {
	values := r.URL.Query()
	status := values.Get("status")
	if err := validateStatus(status); err != nil {
		return nil, err
	}
	hours, err := optionalFloat(values, "hours")
	if err != nil {
		return nil, err
	}
	var olderThan *float64
	if hours != nil {
		if *hours < 0 || *hours > 1e12 {
			return nil, invalid("hours must be between 0 and 1000000000000")
		}
		if *hours != 0 {
			seconds := *hours * 3600
			olderThan = &seconds
		}
	}
	count, err := s.driver.Prune(r.Context(), baqueue.PruneOptions{Status: status, Tag: values.Get("tag"), OlderThanSeconds: olderThan})
	return map[string]int{"pruned": count}, err
}

func (s *server) metricsHTTP(r *http.Request) (any, error) {
	metrics, err := s.driver.GetMetrics(r.Context(), "")
	if metrics == nil {
		metrics = map[string]any{}
	}
	return metrics, err
}

func (s *server) supervisors(ctx context.Context) ([]map[string]any, error) {
	stats, err := s.driver.GetSupervisorStats(ctx, 10)
	if stats == nil {
		stats = []map[string]any{}
	}
	return stats, err
}

func (s *server) supervisorsHTTP(r *http.Request) (any, error) {
	stats, err := s.supervisors(r.Context())
	return map[string]any{"supervisors": stats}, err
}
