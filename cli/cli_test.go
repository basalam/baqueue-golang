package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommands(t *testing.T) {
	for _, command := range []string{"status", "reconcile-indexes", "reconcile-pending"} {
		t.Run(command, func(t *testing.T) {
			var out bytes.Buffer
			if err := Run(context.Background(), []string{command, "--driver", "memory"}, &out, nil); err != nil {
				t.Fatal(err)
			}
			if out.Len() == 0 {
				t.Fatal("missing output")
			}
		})
	}
}

func TestNonfiniteFlagsRejectedBeforeStorageOpens(t *testing.T) {
	for _, name := range []string{"hours", "created-from", "created-to", "stuck-job-timeout-seconds", "prune-completed-seconds", "prune-other-seconds"} {
		for _, value := range []string{"NaN", "+Inf", "-Inf"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "must-not-exist.db")
				var out bytes.Buffer
				err := Run(context.Background(), []string{"prune", "--yes", "--driver", "sqlite", "--driver-url", path, "--" + name, value}, &out, nil)
				if err == nil || !strings.Contains(err.Error(), "--"+name+" must be finite") {
					t.Fatalf("unexpected validation: %v", err)
				}
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("storage was opened: %v", err)
				}
			})
		}
	}
	var out bytes.Buffer
	if err := Run(context.Background(), []string{"prune", "--yes", "--driver", "unknown", "--hours", "1e308"}, &out, nil); err == nil || !strings.Contains(err.Error(), "--hours is too large") {
		t.Fatalf("converted infinite age: %v", err)
	}
}
func TestCLIValidation(t *testing.T) {
	for _, args := range [][]string{{"prune", "--driver", "memory"}, {"retry-failed", "--driver", "memory"}, {"status", "--driver", "unknown"}, {"status", "--created-from", "10", "--created-to", "5"}, {"unknown"}} {
		var out bytes.Buffer
		if err := Run(context.Background(), args, &out, nil); err == nil {
			t.Fatal(args)
		}
	}
	var out bytes.Buffer
	if err := Run(context.Background(), []string{"--help"}, &out, nil); err != nil || !strings.Contains(out.String(), "reconcile-pending") {
		t.Fatal(err, out.String())
	}
}
