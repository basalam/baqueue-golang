// Package cli embeds BaQueue commands in an application that registers its jobs.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

type RegisterFunc func(*baqueue.Queue) error
type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }

// Run executes a command, using register to make application handlers available.
func Run(ctx context.Context, args []string, out io.Writer, register RegisterFunc) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		_, err := fmt.Fprintln(out, "baqueue [--config file.json] <work|schedule|dashboard|status|prune|retry-failed|reconcile-indexes|reconcile-pending|test|version>\nEach command accepts --help. Compile application handlers with cli.Run's register callback.")
		return err
	}
	config := baqueue.DefaultConfig()
	if args[0] == "--config" || args[0] == "-c" {
		if len(args) < 3 {
			return errors.New("config path and command required")
		}
		f, err := os.Open(args[1])
		if err != nil {
			return err
		}
		config, err = baqueue.LoadConfig(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		args = args[2:]
	}
	command := args[0]
	if command == "version" {
		_, err := fmt.Fprintln(out, baqueue.Version)
		return err
	}
	if command == "test" {
		cmd := exec.CommandContext(ctx, "go", append([]string{"test", "./..."}, args[1:]...)...)
		cmd.Stdout = out
		cmd.Stderr = out
		return cmd.Run()
	}
	valid := map[string]bool{"work": true, "schedule": true, "dashboard": true, "status": true, "prune": true, "retry-failed": true, "reconcile-indexes": true, "reconcile-pending": true}
	if !valid[command] {
		return fmt.Errorf("unknown command %q", command)
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(out)
	flags.StringVar(&config.Driver.Name, "driver", config.Driver.Name, "memory, sqlite, redis or postgres")
	flags.StringVar(&config.Driver.URL, "driver-url", config.Driver.URL, "database path or connection URL")
	flags.StringVar(&config.Prefix, "prefix", config.Prefix, "Redis namespace")
	var queues stringsFlag
	flags.Var(&queues, "q", "queue name; repeat for multiple queues")
	var dynamicPrefixes stringsFlag
	flags.Var(&dynamicPrefixes, "dynamic-queue-prefix", "discover registered dynamic queues by prefix; repeatable, defaults to all unless configured")
	dynamicPoll := flags.Float64("dynamic-queue-poll-seconds", 1, "dynamic queue discovery/cleanup interval in positive finite seconds; overrides configuration when supplied")
	queue := flags.String("queue", "", "queue filter")
	status := flags.String("status", "", "job status")
	tag := flags.String("tag", "", "job tag")
	hours := flags.Float64("hours", 0, "minimum age in hours")
	from := flags.Float64("created-from", 0, "minimum creation Unix timestamp")
	to := flags.Float64("created-to", 0, "maximum creation Unix timestamp")
	workers := flags.Int("workers", 1, "minimum workers")
	maxWorkers := flags.Int("max-workers", 10, "maximum workers")
	balance := flags.String("balance", "auto", "auto, simple or false")
	stuck := flags.Float64("stuck-job-timeout-seconds", 3600, "stale processing timeout")
	noStuck := flags.Bool("no-stuck-job-recovery", false, "disable stuck recovery")
	noPrune := flags.Bool("no-auto-prune", false, "disable automatic pruning")
	noCleanup := flags.Bool("no-disk-full-cleanup", false, "disable emergency cleanup")
	flags.Float64Var(&config.PruneCompletedSeconds, "prune-completed-seconds", config.PruneCompletedSeconds, "completed retention")
	flags.Float64Var(&config.PruneOtherSeconds, "prune-other-seconds", config.PruneOtherSeconds, "failed/cancelled retention")
	flags.StringVar(&config.DashboardHost, "host", config.DashboardHost, "dashboard bind host")
	flags.IntVar(&config.DashboardPort, "port", config.DashboardPort, "dashboard port")
	batch := flags.Int("batch", 500, "reconciliation scan batch size")
	yes := flags.Bool("yes", false, "confirm destructive prune/retry operation")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *dynamicPoll <= 0 || math.IsNaN(*dynamicPoll) || math.IsInf(*dynamicPoll, 0) {
		return errors.New("--dynamic-queue-poll-seconds must be positive and finite")
	}
	for _, numeric := range []struct {
		name  string
		value float64
	}{
		{"hours", *hours}, {"created-from", *from}, {"created-to", *to},
		{"stuck-job-timeout-seconds", *stuck},
		{"prune-completed-seconds", config.PruneCompletedSeconds},
		{"prune-other-seconds", config.PruneOtherSeconds},
	} {
		if math.IsNaN(numeric.value) || math.IsInf(numeric.value, 0) {
			return fmt.Errorf("--%s must be finite", numeric.name)
		}
	}
	if *hours > math.MaxFloat64/3600 {
		return errors.New("--hours is too large")
	}
	if *hours < 0 || *from < 0 || *to < 0 || (*from > 0 && *to > 0 && *from > *to) || *batch < 1 {
		return errors.New("invalid age, date range or batch size")
	}
	if *noCleanup {
		config.AutoCleanupOnDiskFull = false
	}
	if *noPrune {
		config.AutoPrune = false
	}
	if (command == "prune" || command == "retry-failed") && !*yes {
		return errors.New("use --yes to confirm this operation")
	}
	if len(config.Supervisors) == 0 {
		config.Supervisors = []baqueue.SupervisorConfig{baqueue.DefaultSupervisorConfig()}
	}
	visited := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	for i := range config.Supervisors {
		s := &config.Supervisors[i]
		if len(queues) > 0 {
			s.Queues = queues
		}
		if visited["dynamic-queue-prefix"] {
			s.DynamicQueuePrefixes = append([]string{}, dynamicPrefixes...)
		}
		if visited["dynamic-queue-poll-seconds"] {
			s.DynamicQueuePollSeconds = *dynamicPoll
		}
		if visited["workers"] {
			s.MinWorkers = *workers
		}
		if visited["max-workers"] {
			s.MaxWorkers = *maxWorkers
		}
		if visited["balance"] {
			s.Balance = *balance
		}
		if visited["stuck-job-timeout-seconds"] {
			s.StuckProcessingSeconds = *stuck
		}
		if *noStuck {
			s.RecoverStuckJobs = false
		}
	}
	q, err := baqueue.Open(ctx, config)
	if err != nil {
		return err
	}
	defer q.Close(context.Background())
	if register != nil {
		if err = register(q); err != nil {
			return err
		}
	}
	encode := func(v any) error { return json.NewEncoder(out).Encode(v) }
	f := baqueue.JobFilter{Queue: *queue, Tag: *tag}
	if *from > 0 {
		f.CreatedFrom = from
	}
	if *to > 0 {
		f.CreatedTo = to
	}
	switch command {
	case "status":
		m, err := q.Metrics(ctx, *queue)
		if err != nil {
			return err
		}
		return encode(m)
	case "prune":
		o := baqueue.PruneOptions{Queue: *queue, Status: *status, Tag: *tag}
		if *hours > 0 {
			seconds := *hours * 3600
			o.OlderThanSeconds = &seconds
		}
		n, err := q.Prune(ctx, o)
		if err != nil {
			return err
		}
		return encode(map[string]int{"pruned": n})
	case "retry-failed":
		n, err := q.RetryFailed(ctx, f)
		if err != nil {
			return err
		}
		return encode(map[string]int{"retried": n})
	case "reconcile-indexes":
		n, err := q.Driver.ReconcileIndexes(ctx, *batch)
		if err != nil {
			return err
		}
		return encode(map[string]int{"removed": n})
	case "reconcile-pending":
		n, err := q.Driver.ReconcilePendingJobs(ctx, *batch)
		if err != nil {
			return err
		}
		return encode(map[string]int{"requeued": n})
	case "schedule":
		s, err := baqueue.NewScheduler(q, config.Schedules)
		if err != nil {
			return err
		}
		return s.Run(ctx)
	case "work":
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		errs := make(chan error, len(config.Supervisors))
		var wg sync.WaitGroup
		for i, c := range config.Supervisors {
			s := baqueue.NewSupervisor(q, c)
			if i == 0 && config.AutoPrune {
				s.Pruner = &baqueue.Pruner{Queue: q, Config: config}
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := s.Run(runCtx)
				if err != nil {
					errs <- err
					cancel()
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			return err
		}
		return nil
	case "dashboard":
		addr := net.JoinHostPort(config.DashboardHost, fmt.Sprint(config.DashboardPort))
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Dashboard: http://%s\n", listener.Addr())
		return serveDashboard(ctx, listener, q.Driver, 10*time.Second)
	}
	return nil
}
