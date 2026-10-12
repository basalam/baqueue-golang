package baqueue

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type Pruner struct {
	Queue  *Queue
	Config Config
}

func (p *Pruner) RunOnce(ctx context.Context) (int, error) {
	ages := map[string]float64{Completed: p.Config.PruneCompletedSeconds, Failed: p.Config.PruneOtherSeconds, Cancelled: p.Config.PruneOtherSeconds}
	for status, hours := range map[string]float64{Completed: p.Config.PruneCompletedHours, Failed: p.Config.PruneFailedHours, Cancelled: p.Config.PruneCancelledHours} {
		if hours > 0 {
			ages[status] = hours * 3600
		}
	}
	total := 0
	limit := max(1, p.Config.PruneBatchSize)
	for _, status := range []string{Completed, Failed, Cancelled} {
		age := ages[status]
		if age <= 0 {
			continue
		}
		for {
			n, err := p.Queue.Driver.PruneTerminalJobs(ctx, PruneOptions{Status: status, OlderThanSeconds: &age, Limit: limit})
			total += n
			if err != nil {
				return total, err
			}
			if n < limit {
				break
			}
		}
	}
	metricAge := p.Config.PruneMetricsSeconds
	if p.Config.PruneMetricsHours > 0 {
		metricAge = p.Config.PruneMetricsHours * 3600
	}
	if metricAge > 0 {
		_, err := p.Queue.Driver.PruneMetrics(ctx, metricAge)
		if err != nil {
			return total, err
		}
	}
	p.Queue.Events.Emit(ctx, "queue.pruned", map[string]any{"count": total})
	return total, nil
}
func (p *Pruner) Run(ctx context.Context) error {
	if p.Config.PruneIntervalSeconds <= 0 {
		return errors.New("baqueue: pruning interval must be positive")
	}
	ticker := time.NewTicker(time.Duration(p.Config.PruneIntervalSeconds * float64(time.Second)))
	defer ticker.Stop()
	for {
		if _, err := p.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("baqueue pruning failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
