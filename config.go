package baqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

type DriverConfig struct {
	Name    string         `json:"name"`
	URL     string         `json:"url"`
	Options map[string]any `json:"options"`
}

type SupervisorConfig struct {
	Name                      string   `json:"name"`
	Queues                    []string `json:"queues"`
	Balance                   string   `json:"balance"`
	MinWorkers                int      `json:"min_workers"`
	MaxWorkers                int      `json:"max_workers"`
	MaxJobsPerWorker          int      `json:"max_jobs_per_worker"`
	MaxTime                   float64  `json:"max_time"`
	Sleep                     float64  `json:"sleep"`
	Timeout                   float64  `json:"timeout"`
	MemoryLimit               int      `json:"memory_limit"`
	RecoverStuckJobs          bool     `json:"recover_stuck_jobs"`
	StuckProcessingSeconds    float64  `json:"stuck_processing_seconds"`
	StuckCheckIntervalSeconds float64  `json:"stuck_check_interval_seconds"`
}

func DefaultSupervisorConfig() SupervisorConfig {
	return SupervisorConfig{Name: "default", Queues: []string{"default"}, Balance: "auto", MinWorkers: 1, MaxWorkers: 10,
		Sleep: 1, Timeout: 60, MemoryLimit: 128, RecoverStuckJobs: true, StuckProcessingSeconds: 3600, StuckCheckIntervalSeconds: 60}
}
func (c *SupervisorConfig) UnmarshalJSON(b []byte) error {
	type plain SupervisorConfig
	v := plain(DefaultSupervisorConfig())
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*c = SupervisorConfig(v)
	return nil
}

type ScheduleEntry struct {
	JobClass string         `json:"job_class"`
	Cron     string         `json:"cron"`
	Every    float64        `json:"every"`
	Queue    string         `json:"queue"`
	Payload  map[string]any `json:"payload"`
}

type Config struct {
	Prefix                string             `json:"prefix"`
	Driver                DriverConfig       `json:"driver"`
	Supervisors           []SupervisorConfig `json:"supervisors"`
	Schedules             []ScheduleEntry    `json:"schedules"`
	DashboardHost         string             `json:"dashboard_host"`
	DashboardPort         int                `json:"dashboard_port"`
	MetricsTrimMinutes    float64            `json:"metrics_trim_minutes"`
	AutoPrune             bool               `json:"auto_prune"`
	PruneIntervalSeconds  float64            `json:"prune_interval_seconds"`
	PruneCompletedSeconds float64            `json:"prune_completed_seconds"`
	PruneOtherSeconds     float64            `json:"prune_other_seconds"`
	PruneMetricsSeconds   float64            `json:"prune_metrics_seconds"`
	PruneBatchSize        int                `json:"prune_batch_size"`
	PruneCompletedHours   float64            `json:"prune_completed_hours"`
	PruneFailedHours      float64            `json:"prune_failed_hours"`
	PruneCancelledHours   float64            `json:"prune_cancelled_hours"`
	PruneMetricsHours     float64            `json:"prune_metrics_hours"`
	ReconcileOnConnect    bool               `json:"reconcile_on_connect"`
	AutoCleanupOnDiskFull bool               `json:"auto_cleanup_on_disk_full"`
}

func DefaultConfig() Config {
	return Config{Prefix: "baqueue", Driver: DriverConfig{Name: "sqlite"}, Supervisors: []SupervisorConfig{DefaultSupervisorConfig()}, DashboardHost: "127.0.0.1", DashboardPort: 9100, MetricsTrimMinutes: 1440, AutoPrune: true, PruneIntervalSeconds: 5, PruneCompletedSeconds: 5, PruneOtherSeconds: 86400, PruneMetricsSeconds: 604800, PruneBatchSize: 1000, AutoCleanupOnDiskFull: true}
}
func LoadConfig(r io.Reader) (Config, error) {
	c := DefaultConfig()
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("baqueue: config must contain one JSON object")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	for _, value := range []float64{c.PruneCompletedSeconds, c.PruneOtherSeconds, c.PruneMetricsSeconds, c.PruneCompletedHours, c.PruneFailedHours, c.PruneCancelledHours, c.PruneMetricsHours, c.MetricsTrimMinutes} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("baqueue: retention values must be finite")
		}
	}
	if c.Prefix == "" {
		return fmt.Errorf("baqueue: prefix required")
	}
	if c.DashboardPort < 1 || c.DashboardPort > 65535 {
		return fmt.Errorf("baqueue: invalid dashboard port")
	}
	if !validSeconds(c.PruneIntervalSeconds, false) || c.PruneBatchSize < 1 {
		return fmt.Errorf("baqueue: invalid prune interval/batch size")
	}
	for _, s := range c.Supervisors {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (c SupervisorConfig) Validate() error {
	if c.Name == "" || len(c.Queues) == 0 || c.MinWorkers < 1 || c.MaxWorkers < c.MinWorkers || !validSeconds(c.Sleep, false) || !validSeconds(c.Timeout, false) || c.MaxJobsPerWorker < 0 || !validSeconds(c.MaxTime, true) {
		return fmt.Errorf("baqueue: invalid supervisor configuration")
	}
	for _, q := range c.Queues {
		if q == "" || strings.Contains(q, ":status:") {
			return fmt.Errorf("baqueue: queue name required")
		}
	}
	if c.Balance != "auto" && c.Balance != "simple" && c.Balance != "false" {
		return fmt.Errorf("baqueue: invalid balance strategy")
	}
	if c.RecoverStuckJobs && (!validSeconds(c.StuckProcessingSeconds, false) || !validSeconds(c.StuckCheckIntervalSeconds, false)) {
		return fmt.Errorf("baqueue: stuck recovery intervals must be positive")
	}
	return nil
}

func validSeconds(value float64, zero bool) bool {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value >= float64(math.MaxInt64)/float64(time.Second) {
		return false
	}
	if value == 0 {
		return zero
	}
	return time.Duration(value*float64(time.Second)) > 0
}

func Open(ctx context.Context, c Config) (*Queue, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var d Driver
	switch c.Driver.Name {
	case "memory":
		d = NewMemoryDriver()
	case "sqlite":
		s := NewSQLiteDriver(c.Driver.URL)
		s.AutoCleanupOnDiskFull = c.AutoCleanupOnDiskFull
		s.ReconcileOnConnect = c.ReconcileOnConnect
		d = s
	case "postgres", "postgresql":
		s := NewPostgresDriver(c.Driver.URL)
		s.AutoCleanupOnDiskFull = c.AutoCleanupOnDiskFull
		s.ReconcileOnConnect = c.ReconcileOnConnect
		d = s
	case "redis":
		s, err := NewRedisDriver(c.Driver.URL, c.Prefix)
		if err != nil {
			return nil, err
		}
		s.AutoCleanupOnDiskFull = c.AutoCleanupOnDiskFull
		s.ReconcileOnConnect = c.ReconcileOnConnect
		d = s
	default:
		return nil, fmt.Errorf("baqueue: unknown driver %q", c.Driver.Name)
	}
	if err := configureDriverOptions(d, c.Driver.Options); err != nil {
		_ = d.Disconnect(context.Background())
		return nil, err
	}
	if err := d.Connect(ctx); err != nil {
		_ = d.Disconnect(context.Background())
		return nil, err
	}
	q := NewQueue(d)
	q.Config = c
	return q, nil
}
