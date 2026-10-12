package baqueue

import _ "github.com/jackc/pgx/v5/stdlib"

// NewPostgresDriver uses a Go-owned schema, separate from Python's tables.
// Configure the exported options before Connect; do not change them concurrently.
func NewPostgresDriver(url string) *SQLDriver {
	return &SQLDriver{driverName: "pgx", dsn: url, postgres: true, AutoCleanupOnDiskFull: true}
}
