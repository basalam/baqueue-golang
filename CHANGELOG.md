# Changelog

## Unreleased

- Added named/generated dynamic queue handles with immediate, delayed, and bulk
  dispatch plus active metadata across the four built-in backends.
- Added supervisor discovery with configurable prefixes and polling, dynamic-only
  zero-worker baselines, and one shared per-supervisor worker cap.
- Added idle expiry that preserves retained jobs, indexes, metrics, and history;
  archived policies allow manual retry reactivation until jobs are pruned.
- Added repeatable `--dynamic-queue-prefix` and positive finite
  `--dynamic-queue-poll-seconds` flags with explicit CLI-over-config precedence.
- Exposed active dynamic metadata and empty definitions in dashboard queue APIs,
  while keeping retained historical jobs in global overview totals.
- Documented lifecycle, runtime-sharing boundaries, and worker limits, with
  focused CLI and dashboard regression tests.

## 0.1.0 - 2026-10-12

- Ported BaQueue's job payload, queue APIs, four storage drivers, retry policies,
  history, lifecycle hooks, batches, events, cron/interval scheduling, supervisors,
  balancing, metrics, retention, storage-pressure cleanup, and maintenance CLI to Go.
- Added crash-safe Redis claims, pending/index reconciliation, policy preservation,
  and stale-attempt fencing. Added transactional SQLite/PostgreSQL storage.
- Embedded the existing dashboard with matching HTTP/WebSocket APIs, filters,
  actions, worker monitoring, and live centisecond durations.
- Added framework-independent HTTP mounting, authentication middleware, prefix
  support, examples, driver contract tests, concurrency tests, and Redis stress tool.
- Made terminal job state and first batch-member outcomes atomic, added callback
  reconciliation, and prevented manual retries from double-counting batch members.
- Preserved large numeric payload IDs and verified graceful WebSocket shutdown.
- Documented runtime differences, at-least-once delivery, cooperative cancellation,
  SQL schema separation, Redis sharing constraints, and local module usage.
