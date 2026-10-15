# BaQueue Go

A Go implementation of the BaQueue library in the sibling `baqueue-fastapi`
directory. It provides durable queues, retries, delayed jobs, batches, scheduling,
worker supervision, maintenance, and the same live monitoring dashboard.

Requires Go 1.25 or newer. SQLite is the default and uses a pure-Go driver; no C
compiler or external service is required. Install the module from GitHub:

```sh
go get github.com/basalam/baqueue-golang@main
```

Import it as `baqueue "github.com/basalam/baqueue-golang"`. No tagged module release
has been published yet. For local development in another application:

```go
require github.com/basalam/baqueue-golang v0.0.0

replace github.com/basalam/baqueue-golang => ../baqeueu-go
```

## Quick start

Run these from this directory in separate terminals:

```sh
go run ./examples/worker work -q emails --workers 3
go run ./examples/dispatch
go run ./cmd/baqueue dashboard
```

Workers, producers, and dashboard share `.baqueue-go.db` in their working directory.
The dashboard is at <http://127.0.0.1:9100>. A self-contained in-memory demo is also
available:

```sh
go run ./examples/dashboard
```

## Use the library

```go
ctx := context.Background()
q, err := baqueue.Open(ctx, baqueue.DefaultConfig())
if err != nil { return err }
defer q.Close(context.Background())

err = q.RegisterFunc("send_email", func(ctx context.Context, data map[string]any) (any, error) {
    return mailer.Send(ctx, data["to"].(string))
}, baqueue.OnQueue("emails"), baqueue.WithMaxAttempts(3))
if err != nil { return err }

id, err := q.Push(ctx, "send_email", map[string]any{"to": "a@example.com"})
id, err = q.Later(ctx, "send_email", time.Minute, map[string]any{"to": "b@example.com"})
ids, err := q.Bulk(ctx, []baqueue.JobRequest{
    {Name: "send_email", Data: map[string]any{"to": "c@example.com"}},
    {Name: "send_email", Data: map[string]any{"to": "d@example.com"}},
})
```

Check each returned error in application code. Dispatch options are `OnQueue`,
`WithMaxAttempts`, `WithBackoff`, `WithTimeout`, and `WithTags`. Registered defaults
are applied first, then dispatch options. Producers can dispatch an unregistered
name; each consuming process must register the corresponding handler. Separate
producers should set the same options explicitly, because policy is stored in the
job at dispatch time.

Numeric values inside `Data` decode as `json.Number`, preserving large IDs without
float64 rounding. Decode into an application struct or call `Int64`/`Float64` as
appropriate.

Implement `Handler` for a struct-based job. Optional `SuccessHandler` and
`FailureHandler` interfaces provide `OnSuccess` and terminal `OnFailure` hooks.
Python's decorators and dynamic imports map to `RegisterFunc` and `Register` in Go;
Python code itself cannot execute inside a Go worker.

## Dynamic queues

Use an open `Queue` to create a temporary queue. An empty name generates
`dynamic-<uuid>`; omitting the optional timeout uses 30 seconds:

```go
temporary, err := q.CreateDynamicQueue(ctx, "")
if err != nil { return err }
fmt.Println(temporary.Name, temporary.IdleTimeoutSeconds)

emails, err := q.CreateDynamicQueue(ctx, "tenant-42-emails", 30*time.Second)
if err != nil { return err }
id, err := emails.Push(ctx, "send_email", map[string]any{"to": "a@example.com"}, baqueue.WithMaxAttempts(3))
if err != nil { return err }
id, err = emails.Later(ctx, "send_email", time.Minute, map[string]any{"to": "b@example.com"})
if err != nil { return err }
ids, err := emails.Bulk(ctx, []baqueue.JobRequest{
    {Name: "send_email", Data: map[string]any{"to": "c@example.com"}, Options: []baqueue.JobOption{baqueue.WithTags("newsletter")}},
})
if err != nil { return err }
active, err := q.DynamicQueues(ctx)
if err != nil { return err }
```

`Push`, `Later`, and `Bulk` use the same arguments and job options as `Queue`.
The handle's name overrides `OnQueue` without changing registered job defaults.
Dispatch atomically registers/reactivates the definition and enqueues its jobs,
so the handle remains usable after expiry. Choose names distinct from ordinary
queues and keep the same idle timeout when registering an existing name.

`DynamicQueues` returns active `DynamicQueueInfo` records with JSON fields `name`,
`idle_timeout_seconds`, `created_at`, `idle_since`, and `active`. Timestamps are Unix
seconds; `idle_since` is null while jobs are pending or processing. All four
built-in backends implement the optional `DynamicQueueDriver` interface.

### Discovery and worker limits

`DefaultSupervisorConfig()` discovers all explicitly registered dynamic queues
with `DynamicQueuePrefixes: []string{""}`. A nil prefix slice also means discover
all; an explicit `[]string{}` disables discovery. Nonempty prefixes such as
`[]string{"tenant-", "request-"}` select matching names. Ordinary queues do not
become dynamic merely because their names match a prefix.

For a dynamic-only supervisor that can scale down to zero:

```go
sc := baqueue.DefaultSupervisorConfig()
sc.Queues = []string{}
sc.MinWorkers, sc.MaxWorkers = 0, 8
sc.DynamicQueuePrefixes = []string{"tenant-"}
sc.DynamicQueuePollSeconds = 1
s := baqueue.NewSupervisor(q, sc)
err = s.Run(ctx)
```

Keep `MaxWorkers >= 1`. Static and dynamic queues share one pool and one total
`MaxWorkers` cap **per supervisor**, including workers still draining a job.
`MinWorkers` remains the baseline; no queue gets a dedicated worker pool.
Scaling changes internal worker goroutines, not processes, Kubernetes pods, or
deployment replicas. Multiple supervisors each have their own cap, not a shared
cluster-wide limit.

`DynamicQueuePollSeconds` defaults to 1; a zero value in Go configuration falls
back to 1. Positive intervals must be finite and representable as a Go duration.
Keep a supervisor running for discovery and idle cleanup. Producers and the
dashboard do not perform that lifecycle themselves.

### Safe idle expiry

With the default timeout, expiry requires at least 30 seconds of idleness from
the first empty observation, plus polling lag. Creating or re-registering an
empty definition starts a fresh idle window. Pending jobs, including delayed
jobs and scheduled retries, and processing jobs prevent expiry.

Expiry removes the active definition from discovery, not jobs, indexes, metrics,
or history. An archived minimal policy stays with retained historical jobs so
manual retry can reactivate the queue; cleanup removes that policy after its
jobs are pruned. Normal retention and emergency cleanup remain independent of
expiry, so an idle timeout does not extend job retention.

Memory is single-process only. Redis dynamic metadata uses the same wire format
in Python and Go, but neither runtime can execute the other's job classes or
handlers. When intentionally sharing a Redis namespace, use runtime-specific
queue prefixes and scope discovery accordingly; the default discovers all
registered dynamic queues. Every consuming Go process must register its handlers.

## Feature parity

| Python feature | Go API |
| --- | --- |
| Queue facade; push/later/bulk | `Queue`, `Push`, `Later`, `Bulk` |
| Dynamic queues and active metadata | `CreateDynamicQueue`, `DynamicQueue.Push/Later/Bulk`, `DynamicQueues` |
| Class/function jobs; success/failure hooks | `Handler`, `HandlerFunc`, registration and hook interfaces |
| Memory, SQLite, Redis, PostgreSQL | `Driver` and four built-in constructors |
| Fixed, linear, exponential, explicit retry delays | `Backoff`, `ComputeDelay`, `ShouldRetry` |
| Per-attempt history, policy, tags, timestamps | `JobPayload`, identical JSON names and Unix seconds |
| Batches and then/catch/finally/allow-failures | `Queue.Batch(...).Then(...).Catch(...).Finally(...)` |
| Cron and interval schedules | `NewScheduler`, `ScheduleEntry`, `Add`, `Run` |
| Worker pools and auto/simple/no balancing | `Supervisor`, `Scale`, `Balancer` |
| Worker recycling | `MaxJobsPerWorker`, `MaxTime` |
| One-hour stuck processing recovery | `RecoverStuckJobs`, `StuckProcessingSeconds` |
| Graceful shutdown | `context` cancellation; `Worker.Stop` drains its current job |
| Pruning, retention overrides, disk-full cleanup | `Pruner`, driver maintenance methods, `Config` |
| Redis orphan/index reconciliation | `ReconcilePendingJobs`, `ReconcileIndexes` |
| Events, metrics, supervisor heartbeats | `EventBus`, `Metrics`, driver reporting methods |
| Dashboard and WebSockets | `dashboard.New`, embedded original UI and API |
| Jobs/queues/workers, filters, details, retry/delete/execute | Same dashboard controls and REST paths |
| Live centisecond processing duration | Browser-local duration timer, independent of feed refresh |
| Work/schedule/dashboard/maintenance CLI | `cmd/baqueue`, embeddable `cli.Run` |

Go timeouts are cooperative: handlers must observe `ctx.Done()` and pass context
to I/O. The worker does not abandon a still-running goroutine and immediately run
the same job again. Use process isolation for untrusted or non-cooperative jobs.
All goroutines share a heap, so `memory_limit` triggers garbage collection under
pressure rather than pretending to enforce a per-worker memory limit. It is not a
process memory cap.

## Storage and delivery

```go
config := baqueue.DefaultConfig()
config.Driver = baqueue.DriverConfig{Name: "redis", URL: "redis://localhost:6379/0"}
config.Prefix = "myapp:baqueue"
// Other choices:
// DriverConfig{Name: "sqlite", URL: "./queue.db"}
// DriverConfig{Name: "postgres", URL: "postgres://user:pass@localhost/db?sslmode=require"}
// DriverConfig{Name: "memory"}
```

Memory is only shared inside the current process. SQLite uses WAL, a busy timeout,
and transactional claims across processes. PostgreSQL uses row locks with
`FOR UPDATE SKIP LOCKED`. SQL tables have a `baqueue_go_` prefix and store the full
payload, including history; they are deliberately separate from the Python SQL
schema. Opening Go against an existing Python database does **not** migrate its jobs.

Redis uses the Python key and payload layout. Use matching prefixes only when
intentionally sharing storage, and separate queues by runtime. Go handlers can be
registered under existing `job_class` strings. Do not let a Python worker consume
a Go-only handler, or the reverse. Redis Cluster is not supported by this
multi-key transaction layout.
Use `maxmemory-policy noeviction` and appropriate Redis persistence; recovery
cannot restore evicted keys or data lost by an unpersisted server restart.

A Redis claim watches the queue and hash, reads the head without removing it,
then commits the payload/status/index updates and ready-entry removal together.
Conflicting workers retry the transaction. A disconnect before EXEC leaves the
entry ready; an uncertain successful EXEC leaves a processing record recoverable
by the supervisor. Future-dated ready entries are restored to delayed storage.
Stale processing attempts cannot complete or release a newer claim.

Delivery is **at least once**, not exactly once. Make externally visible job
effects idempotent. Recovery retains the consumed attempt, `max_attempts`, backoff,
and history. The default retry policy allows three total attempts; exponential
delays start at 5 seconds, double per attempt, add up to 10% jitter, and cap at one
hour. Explicit delays are seconds, for example `Backoff{Delays: []float64{10,60,300}}`.
Manual retry preserves stored attempt counts and policy, as do release and stuck
recovery. It intentionally permits another execution of a terminal failed job.

## Workers, schedules, batches, and events

```go
sc := baqueue.DefaultSupervisorConfig()
sc.Queues = []string{"emails", "payments"}
sc.MinWorkers, sc.MaxWorkers = 3, 10
sc.StuckProcessingSeconds = 3600
s := baqueue.NewSupervisor(q, sc)
s.Pruner = &baqueue.Pruner{Queue:q, Config:q.Config}
err = s.Run(ctx)
```

Raise the stuck timeout above the longest legitimate handler runtime. Give
supervisors distinct names when monitoring multiple processes. The standard CLI
starts automatic pruning once, alongside its supervisor group. Default retention:
completed 5 seconds, failed/cancelled 1 day, metrics 7 days; prune interval 5 seconds.
Nonpositive retention disables pruning for that category. Legacy positive
`prune_*_hours` fields override seconds.

```go
scheduler, err := baqueue.NewScheduler(q, []baqueue.ScheduleEntry{
    {JobClass:"cleanup", Cron:"0 * * * *", Queue:"maintenance"},
    {JobClass:"heartbeat", Every:30, Queue:"maintenance"},
})
if err != nil { return err }
err = scheduler.Run(ctx)

batch, err := q.Batch([]baqueue.JobRequest{
    {Name:"send_email", Data:map[string]any{"to":"a@example.com"}},
    {Name:"send_email", Data:map[string]any{"to":"b@example.com"}},
}).Name("newsletter").Then("all_done").Catch("any_failed").Finally("finished").Dispatch(ctx)
```

Schedules use UTC, five-field cron expressions, or interval seconds. As in the
Python scheduler, the first tick dispatches immediately. Run one scheduler per
schedule set; schedules are not a distributed leader-election mechanism. Batch
callback names persist in storage and therefore work when producer and workers
are separate processes. Counters are atomic; callbacks use deterministic IDs.
Terminal job state and the member's first batch outcome commit together. Manual
retry does not reopen that member's original batch outcome or count it twice.
Callback dispatch has persistent completion flags and deterministic job IDs;
the supervisor retries unfinished dispatch through `ReconcileBatches`. Callbacks
still require idempotent external effects under at-least-once delivery.

Events: `job.pushed`, `job.started`, `job.completed`, `job.failed`, `job.retrying`,
`batch.dispatched`, `batch.completed`, `batch.failed`, `worker.started`,
`worker.stopped`, `supervisor.started`, `supervisor.stopped`, `queue.pruned`, and
`schedule.dispatched`. `q.Events.On(name, fn)` returns an unsubscribe function.
Listeners run locally, synchronously, in registration order. Listener errors and
panics are logged without changing the job outcome.

## Embed the dashboard

```go
handler := dashboard.New(q.Driver,
    dashboard.WithContext(applicationContext),
    dashboard.WithPrefix("/baqueue"),
    dashboard.WithAuth(yourAuthenticationMiddleware),
)
mux.Handle("/baqueue", handler)
mux.Handle("/baqueue/", handler)
```

Use standard `net/http`, Gin `WrapH`, or Echo `WrapHandler`; no particular Go web
framework is required. All API and WebSocket paths honor the prefix. Authentication
wraps every route, including assets and WebSockets. The dashboard is administrative:
use application authentication and a trusted network. The CLI binds loopback by
default. WebSockets accept same-origin requests unless explicitly configured.
The original UI loads Alpine.js from jsDelivr and fonts from Google Fonts, so a
browser needs access to those hosts.

Active empty dynamic definitions appear in queue lists. Overview queue entries
and queue-detail responses include a `dynamic` metadata record, or null when
there is no active definition. Expired definitions disappear from active lists,
but overview status totals still include retained historical jobs. Job details,
queue-detail metrics, and manual retry remain available until normal retention
removes the jobs.

## CLI and configuration

```sh
go run ./cmd/baqueue --config config.json status
go run ./examples/worker work --driver sqlite -q emails --workers 3 --max-workers 10
go run ./examples/worker schedule
go run ./cmd/baqueue dashboard --host 127.0.0.1 --port 9100
go run ./cmd/baqueue prune --status completed --hours 24 --yes
go run ./cmd/baqueue retry-failed --queue emails --tag newsletter --yes
go run ./cmd/baqueue reconcile-indexes --driver redis --driver-url redis://localhost:6379/0
go run ./cmd/baqueue reconcile-pending --driver redis --driver-url redis://localhost:6379/0
```

The standalone CLI cannot dynamically import Go handlers. Build your own executable
with `cli.Run(ctx, os.Args[1:], os.Stdout, register)` as in `examples/worker`.
Configuration field names match Python; load partial JSON with `LoadConfig` to
retain defaults. See [config.example.json](config.example.json). Driver URLs and
credentials should come from application configuration, not source control.

Dynamic discovery flags follow the existing config-file precedence: omitted
flags preserve each supervisor's settings, while supplied flags override every
configured supervisor. Repeated prefixes replace the configured list, not append
to it. With no configuration, all registered dynamic queues are discovered every
second:

```sh
go run ./examples/worker work -q emails --workers 3 --max-workers 10 --dynamic-queue-prefix tenant- --dynamic-queue-prefix request- --dynamic-queue-poll-seconds 0.5
```

`--dynamic-queue-poll-seconds` must be positive and finite; explicit CLI zero is
rejected. To disable discovery, set `"dynamic_queue_prefixes": []` in the relevant
`supervisors` JSON entries and omit `--dynamic-queue-prefix`. Omitted or null
prefix settings discover all. Passing `--dynamic-queue-prefix=` explicitly
restores discovery of all registered dynamic queues.

`driver.options` supports Redis `username`, `password`, `db`, `max_connections`
(`pool_size`), `socket_timeout`, `socket_connect_timeout`, `read_timeout`,
`write_timeout`, and `pool_timeout`. PostgreSQL supports `max_size`
(`max_open_conns`), `min_size` (`max_idle_conns`), and `command_timeout` (seconds,
mapped to server statement timeout). Idle connections are retained lazily, not
pre-opened. SQLite accepts URI pragma options in its URL. Unsupported options
fail explicitly. SQL isolation can use separate database files or PostgreSQL
`search_path` schemas; `prefix` controls Redis namespaces.

## Verification

```sh
go test ./...
go vet ./...
go build ./...
docker compose -f compose.test.yml up -d --wait
```

PowerShell integration environment:

```powershell
$env:BAQUEUE_REDIS_URL = 'redis://127.0.0.1:16389/0'
$env:BAQUEUE_POSTGRES_URL = 'postgres://postgres:baqueue_test@127.0.0.1:15489/baqueue_test?sslmode=disable'
go test ./... -count=1
```

Integration tests use uniquely named Redis prefixes and PostgreSQL schemas. Without
the environment variables they skip external services. SQLite and memory always
run. Use `go test -race ./...` with a supported C compiler, or a Linux Go container.
Never point fault-injection tests at production.

```sh
go run ./cmd/stress --url redis://127.0.0.1:16389/0 --jobs 1000 --workers 8
# In another terminal, while the stress scenario runs:
docker compose -f compose.test.yml restart redis
```

See [docs/verification.md](docs/verification.md) for the recorded validation and
[CHANGELOG.md](CHANGELOG.md) for the initial port scope. Licensed under MIT.
