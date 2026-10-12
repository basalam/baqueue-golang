// Package baqueue provides durable jobs, workers, scheduling and monitoring.
package baqueue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const Version = "0.1.0"

const (
	Pending    = "pending"
	Processing = "processing"
	Completed  = "completed"
	Failed     = "failed"
	Cancelled  = "cancelled"
)

var (
	ErrNotFound  = errors.New("baqueue: job not found")
	ErrConflict  = errors.New("baqueue: job state changed")
	ErrDuplicate = errors.New("baqueue: duplicate job ID")
)

// Backoff uses the same JSON representation as Python: a strategy or seconds array.
type Backoff struct {
	Strategy string
	Delays   []float64
}

func (b Backoff) MarshalJSON() ([]byte, error) {
	if b.Delays != nil {
		return json.Marshal(b.Delays)
	}
	if b.Strategy == "" {
		b.Strategy = "exponential"
	}
	return json.Marshal(b.Strategy)
}

func (b *Backoff) UnmarshalJSON(data []byte) error {
	*b = Backoff{}
	if len(data) > 0 && data[0] == '[' {
		return json.Unmarshal(data, &b.Delays)
	}
	return json.Unmarshal(data, &b.Strategy)
}

type Attempt struct {
	Attempt     int      `json:"attempt"`
	StartedAt   float64  `json:"started_at"`
	FinishedAt  float64  `json:"finished_at"`
	Duration    float64  `json:"duration"`
	Status      string   `json:"status"`
	Error       *string  `json:"error"`
	WillRetry   bool     `json:"will_retry"`
	NextRetryAt *float64 `json:"next_retry_at"`
}

// JobPayload preserves Python's field names and Unix timestamps in seconds.
type JobPayload struct {
	ID          string         `json:"id"`
	JobClass    string         `json:"job_class"`
	Data        map[string]any `json:"data"`
	Queue       string         `json:"queue"`
	Attempts    int            `json:"attempts"`
	MaxAttempts int            `json:"max_attempts"`
	Backoff     Backoff        `json:"backoff"`
	Timeout     float64        `json:"timeout"`
	Tags        []string       `json:"tags"`
	BatchID     *string        `json:"batch_id"`
	DelayUntil  *float64       `json:"delay_until"`
	CreatedAt   float64        `json:"created_at"`
	UpdatedAt   float64        `json:"updated_at"`
	StartedAt   *float64       `json:"started_at"`
	CompletedAt *float64       `json:"completed_at"`
	FailedAt    *float64       `json:"failed_at"`
	Status      string         `json:"status"`
	Error       *string        `json:"error"`
	History     []Attempt      `json:"history"`
}

// Decode job data losslessly: JSON integers may exceed float64's exact range.
func (p *JobPayload) UnmarshalJSON(raw []byte) error {
	type plain JobPayload
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*p = JobPayload(decoded)
	return nil
}

func Now() float64 { return float64(time.Now().UnixMicro()) / 1e6 }

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func NewPayload(name string, data map[string]any) *JobPayload {
	now := Now()
	if data == nil {
		data = map[string]any{}
	}
	return &JobPayload{ID: NewID(), JobClass: name, Data: data, Queue: "default", MaxAttempts: 3,
		Backoff: Backoff{Strategy: "exponential"}, Timeout: 60, Tags: []string{}, History: []Attempt{},
		CreatedAt: now, UpdatedAt: now, Status: Pending}
}

func (p *JobPayload) Clone() *JobPayload {
	if p == nil {
		return nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		panic(fmt.Sprintf("baqueue: non-JSON payload: %v", err))
	}
	var out JobPayload
	if err = json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return &out
}

func (p *JobPayload) Validate() error {
	if p == nil {
		return errors.New("baqueue: nil payload")
	}
	if p.ID == "" || p.Queue == "" || p.JobClass == "" {
		return errors.New("baqueue: id, queue and job_class are required")
	}
	if strings.Contains(p.Queue, ":status:") {
		return errors.New("baqueue: queue names cannot contain :status:")
	}
	if p.MaxAttempts < 1 || p.Attempts < 0 || !validSeconds(p.Timeout, true) {
		return errors.New("baqueue: invalid attempts or timeout")
	}
	if _, err := json.Marshal(p); err != nil {
		return fmt.Errorf("baqueue: invalid JSON payload: %w", err)
	}
	return p.Backoff.Validate()
}

func (b Backoff) Validate() error {
	if b.Delays != nil {
		for _, d := range b.Delays {
			if d < 0 {
				return errors.New("baqueue: backoff delays must be nonnegative")
			}
		}
		return nil
	}
	switch b.Strategy {
	case "", "fixed", "linear", "exponential":
		return nil
	}
	return fmt.Errorf("baqueue: unknown backoff %q", b.Strategy)
}
