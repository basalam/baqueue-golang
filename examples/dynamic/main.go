package main

import (
	"context"
	"fmt"
	"log"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	config := baqueue.DefaultConfig()
	config.Driver.Name = "memory"
	q, err := baqueue.Open(ctx, config)
	if err != nil {
		return err
	}
	defer q.Close(context.Background())
	if err := q.RegisterFunc("process_item", func(ctx context.Context, data map[string]any) (any, error) {
		select {
		case <-time.After(10 * time.Millisecond):
			return data["item"], nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}); err != nil {
		return err
	}
	supervisorConfig := baqueue.DefaultSupervisorConfig()
	supervisorConfig.Queues = []string{}
	supervisorConfig.MinWorkers, supervisorConfig.MaxWorkers = 0, 3
	supervisorConfig.Sleep, supervisorConfig.DynamicQueuePollSeconds = 0.01, 0.02
	supervisorConfig.DynamicQueuePrefixes = []string{"session:"}
	supervisor := baqueue.NewSupervisor(q, supervisorConfig)
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	defer func() { cancel(); <-done }()
	// A short timeout keeps this example quick; the default is 30 seconds.
	dynamic, err := q.CreateDynamicQueue(ctx, "session:example", 200*time.Millisecond)
	if err != nil {
		return err
	}
	jobs := make([]baqueue.JobRequest, 50)
	for i := range jobs {
		jobs[i] = baqueue.JobRequest{Name: "process_item", Data: map[string]any{"item": i}}
	}
	if _, err = dynamic.Bulk(ctx, jobs); err != nil {
		return err
	}
	for {
		queues, err := q.DynamicQueues(ctx)
		if err != nil {
			return err
		}
		if len(queues) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	completed, err := q.Driver.CountJobs(ctx, baqueue.JobFilter{Queue: dynamic.Name, Status: baqueue.Completed})
	if err != nil {
		return err
	}
	if completed != len(jobs) {
		return fmt.Errorf("completed %d of %d jobs", completed, len(jobs))
	}
	fmt.Printf("Completed %d jobs; dynamic queue expired; job history remains available.\n", completed)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
