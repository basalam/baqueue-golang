# Verification

Verified on 2026-10-12 against the integrated Go source in this directory.

## Automated checks

| Check | Environment | Result |
| --- | --- | --- |
| `go test ./... -count=1` | Windows, Go 1.27.1 | Passed; external drivers skip without service URLs |
| `go test -race -json ./... -count=1` | Linux, Go 1.25.5, real Redis 7 and PostgreSQL 17 | 199 tests/subtests passed, 0 failed, 0 skipped; no races reported |
| `go vet ./...` | Windows | Passed |
| `go build -o bin/ ./cmd/... ./examples/...` | Windows | All CLI, stress, and example executables built |
| `gofmt -l .` | Windows | No unformatted files |
| `go mod verify` | Windows | All modules verified |
| `node --check dashboard/static/app.js` | Windows | Passed |
| WebSocket lifecycle regression, 30 repeated runs | Windows | Passed |
| CLI config/status smoke check | Compiled executable, memory driver | Passed |

Machine-readable final race-test output is available locally at
`artifacts/race-results.json` (ignored by Git). Tests use unique Redis prefixes,
temporary SQLite files, and unique PostgreSQL schemas.

Coverage includes claim concurrency, failures before/after EXEC, stale and
malformed records, delayed jobs, retry policies/history, pruning/index consistency,
SQLite writer contention and real storage-full recovery, PostgreSQL SKIP LOCKED,
atomic batch outcomes and callback recovery, scheduler callbacks, worker lifecycle,
pool limits, shutdown, REST filters/actions, authentication/prefix routing, and
both WebSocket feeds. Redis OOM/partial-EXEC cases use injected failures; real
service interruption is covered separately below.

## Redis interruption stress

Ran the final compiled `cmd/stress` with 1,000 jobs and 8 workers against an isolated
Redis container with AOF enabled. After seeding finished and workers began,
stopped Redis, waited two seconds, and restarted the same container. Actual outage
also included container shutdown/startup and Redis dataset loading.

```json
{
  "jobs": 1000,
  "workers": 8,
  "completed": 1000,
  "executions": 1002,
  "observed_connection_errors": 29,
  "elapsed_seconds": 30.6588667
}
```

The final checks found no pending, processing, or failed jobs, and verified
preserved attempt policies and history. Two executions repeated after ambiguous
connection failures/recovery, consistent with at-least-once delivery. This is not
an exactly-once execution claim or a throughput benchmark. The stress namespace
was cleaned up by the tool; test containers were stopped after verification.

## Browser checks

Verified the live Go demo at `http://127.0.0.1:19100`:

- Overview counts and queue breakdown.
- Jobs, status filtering, attempt counts, and live centisecond duration.
- Job details, payload, error, and per-attempt timeline.
- Queues and supervisor/worker monitoring.
- Desktop 1280x900 and mobile 390x844; mobile tables scroll horizontally.
- No browser console errors observed on the final build.

Local screenshots: `artifacts/dashboard-final.jpg` and
`artifacts/dashboard-job.jpg`. The demo uses memory storage and synthetic jobs.

## Boundaries

The Python source repository was left unchanged. This verification does not
include a module release or production deployment. Go handlers must cooperate with
context cancellation; SQL storage uses the separate `baqueue_go_*` schema.
The README documents Redis namespace sharing and the remaining runtime differences.

## Dynamic Queue Verification, 2026-10-15

The following checks cover the integrated dynamic queue implementation, including
all four drivers, supervisor discovery/scaling, CLI configuration, and dashboard
metadata/history totals:

| Check | Environment | Result |
| --- | --- | --- |
| `go test ./... -count=1` | Windows, real Redis 7 and PostgreSQL 17 | Passed |
| `go test -race ./... -count=1 -json` | Linux, Go 1.25, real Redis 7 and PostgreSQL 17 | 302 tests/subtests passed, 0 failed, 0 skipped; no races reported |
| `go vet ./...` | Windows | Passed |
| `go build ./...` | Windows | Passed |
| `gofmt -l .` and `git diff --check` | Windows | Passed |
| `go run ./examples/dynamic` | Windows, memory driver | 50 jobs completed; queue expired and records remained |

Coverage includes atomic registration/enqueue and expiry, concurrent producers
and cleanup, uncertain Redis EXEC outcomes without replay, SQL rollback and
additive schema creation, delayed/backoff protection, retry reactivation with
preserved policy/history, literal discovery prefixes, a zero-worker baseline,
the physical worker cap including retiring workers, and retained dashboard totals.
Machine-readable race results are in `artifacts/dynamic-race.jsonl` (Git-ignored).

The Python counterpart also passed 455 tests with real Redis/PostgreSQL and a
browser lifecycle check. These new results do not rerun or replace the earlier
Redis interruption stress test or full Go browser checks documented above.
Temporary services were stopped. These checks do not include a release tag,
package publication, or production deployment.
