package baqueue

import (
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

// NewSQLiteDriver uses a Go-owned schema, separate from Python's tables.
// Configure the exported options before Connect; do not change them concurrently.
func NewSQLiteDriver(path string) *SQLDriver {
	if path == "" {
		path = ".baqueue-go.db"
	}
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	// These options apply to every connection, including replacements in the pool.
	options := url.Values{}
	options.Add("_pragma", "busy_timeout(5000)")
	options.Add("_pragma", "journal_mode(WAL)")
	options.Add("_pragma", "synchronous(FULL)")
	options.Set("_txlock", "immediate")
	return &SQLDriver{driverName: "sqlite", dsn: path + separator + options.Encode(), AutoCleanupOnDiskFull: true}
}
