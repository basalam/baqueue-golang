package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	baqueue "github.com/basalam/baqueue-golang"
)

func TestDynamicCLIConfiguration(t *testing.T) {
	const configured = `{"supervisors":[{"name":"one","dynamic_queue_prefixes":[],"dynamic_queue_poll_seconds":2.5},{"name":"two","dynamic_queue_prefixes":["configured-"],"dynamic_queue_poll_seconds":3}]}`
	tests := []struct {
		name     string
		config   string
		flags    []string
		prefixes [][]string
		poll     []float64
	}{
		{name: "defaults", prefixes: [][]string{{""}}, poll: []float64{1}},
		{name: "preserve configured supervisors", config: configured, prefixes: [][]string{{}, {"configured-"}}, poll: []float64{2.5, 3}},
		{name: "replace prefixes on all supervisors", config: configured, flags: []string{"--dynamic-queue-prefix", "tenant-", "--dynamic-queue-prefix", "request-"}, prefixes: [][]string{{"tenant-", "request-"}, {"tenant-", "request-"}}, poll: []float64{2.5, 3}},
		{name: "poll override preserves disabled discovery", config: configured, flags: []string{"--dynamic-queue-poll-seconds", "0.25"}, prefixes: [][]string{{}, {"configured-"}}, poll: []float64{0.25, 0.25}},
		{name: "empty prefix enables all", config: configured, flags: []string{"--dynamic-queue-prefix="}, prefixes: [][]string{{""}, {""}}, poll: []float64{2.5, 3}},
		{name: "nil prefix retains default semantics", config: `{"supervisors":[{"dynamic_queue_prefixes":null}]}`, prefixes: [][]string{nil}, poll: []float64{1}},
		{name: "zero config poll retains fallback", config: `{"supervisors":[{"dynamic_queue_poll_seconds":0}]}`, prefixes: [][]string{{""}}, poll: []float64{0}},
		{name: "empty supervisor list uses defaults", config: `{"supervisors":[]}`, prefixes: [][]string{{""}}, poll: []float64{1}},
		{name: "both flags", flags: []string{"--dynamic-queue-prefix", "tenant-", "--dynamic-queue-poll-seconds", "2e1"}, prefixes: [][]string{{"tenant-"}}, poll: []float64{20}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{}
			if tc.config != "" {
				path := filepath.Join(t.TempDir(), "config.json")
				if err := os.WriteFile(path, []byte(tc.config), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--config", path)
			}
			args = append(args, "work", "--driver", "memory")
			args = append(args, tc.flags...)
			stop := errors.New("configuration captured before workers start")
			var captured *baqueue.Config
			var out bytes.Buffer
			err := Run(context.Background(), args, &out, func(q *baqueue.Queue) error {
				config := q.Config
				captured = &config
				return stop
			})
			if !errors.Is(err, stop) || captured == nil {
				t.Fatalf("configuration capture: %v, output: %s", err, out.String())
			}
			if len(captured.Supervisors) != len(tc.prefixes) {
				t.Fatalf("supervisors: %#v", captured.Supervisors)
			}
			for i, supervisor := range captured.Supervisors {
				if !reflect.DeepEqual(supervisor.DynamicQueuePrefixes, tc.prefixes[i]) || supervisor.DynamicQueuePollSeconds != tc.poll[i] {
					t.Fatalf("supervisor %d: prefixes=%#v poll=%g", i, supervisor.DynamicQueuePrefixes, supervisor.DynamicQueuePollSeconds)
				}
			}
		})
	}
}

func TestDynamicCLIInvalidPollBeforeStorageOpens(t *testing.T) {
	for _, value := range []string{"0", "-1", "NaN", "+Inf", "-Inf", "1e309", "invalid"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "must-not-exist.db")
			var out bytes.Buffer
			err := Run(context.Background(), []string{"work", "--driver", "sqlite", "--driver-url", path, "--dynamic-queue-poll-seconds", value}, &out, nil)
			if err == nil || !strings.Contains(err.Error(), "dynamic-queue-poll-seconds") {
				t.Fatalf("validation: %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("storage was opened: %v", err)
			}
		})
	}
}

func TestDynamicCLIHelp(t *testing.T) {
	var out bytes.Buffer
	if err := Run(context.Background(), []string{"work", "--help"}, &out, nil); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dynamic-queue-prefix", "dynamic-queue-poll-seconds"} {
		if !strings.Contains(out.String(), name) {
			t.Fatalf("help missing %s", name)
		}
	}
}
