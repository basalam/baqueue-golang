// Stress exercises Redis recovery. Restart the isolated Redis server while it runs.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	url := flag.String("url", "redis://127.0.0.1:16389/0", "isolated Redis URL")
	jobs := flag.Int("jobs", 1000, "job count")
	workers := flag.Int("workers", 8, "worker count")
	flag.Parse()
	if *jobs < 1 || *workers < 1 {
		return fmt.Errorf("jobs and workers must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prefix := "baqueue:go-stress:" + baqueue.NewID()
	d, err := baqueue.NewRedisDriver(*url, prefix)
	if err != nil {
		return err
	}
	if err = d.Connect(ctx); err != nil {
		return err
	}
	defer d.Disconnect(context.Background())
	defer d.Flush(context.Background(), "")
	q := baqueue.NewQueue(d)
	var executions atomic.Int64
	if err = q.RegisterFunc("stress", func(ctx context.Context, _ map[string]any) (any, error) {
		executions.Add(1)
		timer := time.NewTimer(40 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, nil
		}
	}, baqueue.WithMaxAttempts(20)); err != nil {
		return err
	}
	requests := make([]baqueue.JobRequest, *jobs)
	for i := range requests {
		requests[i] = baqueue.JobRequest{Name: "stress", Data: map[string]any{"number": i}}
	}
	if _, err = q.Bulk(ctx, requests); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Seeded %d jobs; starting %d workers\n", *jobs, *workers)
	c := baqueue.DefaultSupervisorConfig()
	c.MinWorkers = *workers
	c.MaxWorkers = *workers
	c.Balance = "false"
	c.Sleep = .02
	c.StuckProcessingSeconds = 1
	c.StuckCheckIntervalSeconds = 1
	s := baqueue.NewSupervisor(q, c)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = s.Run(ctx) }()
	defer wg.Wait()
	defer cancel()
	start := time.Now()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	recoveries := 0
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("stress timed out: %w", ctx.Err())
		case <-ticker.C:
			n, err := d.CountJobs(ctx, baqueue.JobFilter{Status: baqueue.Completed})
			if err != nil {
				recoveries++
				continue
			}
			if n != *jobs {
				continue
			}
			for _, status := range []string{baqueue.Pending, baqueue.Processing, baqueue.Failed} {
				count, err := d.CountJobs(ctx, baqueue.JobFilter{Status: status})
				if err != nil {
					return err
				}
				if count != 0 {
					return fmt.Errorf("unexpected %s count: %d", status, count)
				}
			}
			payloads, err := d.GetJobs(ctx, baqueue.JobFilter{Limit: -1})
			if err != nil {
				return err
			}
			for _, p := range payloads {
				if p.MaxAttempts != 20 || p.Attempts < 1 || len(p.History) < 1 {
					return fmt.Errorf("invalid attempt policy: %s", p.ID)
				}
			}
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"jobs": *jobs, "workers": *workers, "completed": n, "executions": executions.Load(), "observed_connection_errors": recoveries, "elapsed_seconds": time.Since(start).Seconds(), "prefix": prefix})
		}
	}
}
