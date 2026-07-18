# Autoscaler guide

## 1. How scaling works

The crawler has one dispatch actor. At the start of each dispatch pass it reads
`AutoscaledPool::desired_concurrency()`, starts work only while the number of in-flight attempts is
below that target, and owns every deferred start. Retry backoff, host-politeness waits, and global
start-budget waits are held in its deferred heap rather than spawned as sleeping tasks. Deferred
entries do not count as in-flight concurrency.

Once dynamic autoscaling is enabled, AIMD is the default mode. A successful attempt extends a
success streak; every `increase_after_successes` successes, desired concurrency increases by one.
Any failed or retryable attempt is a setback: it clears the streak and sets desired concurrency to
`round(desired * decrease_factor)`. Construction normalizes the streak threshold to at least one,
accepts a decrease factor in `(0, 1]` (otherwise using `0.5`), and clamps desired concurrency to the
normalized `[min_concurrency, max_concurrency]` range.

In `AutoscaleMode::LoadSignals`, the pool evaluates registered signals every `autoscale_interval`
over `snapshotter.window`. If any signal's latest snapshot is overloaded, desired concurrency drops
by `ceil(current * scale_down_step_ratio)`, at least one. Otherwise, signals with at least
`system_status.min_samples.max(1)` observations contribute their healthy-sample ratios; if their
mean is at least `desired_utilization_ratio`, desired concurrency rises by
`ceil(current * scale_up_step_ratio)`, at least one. Both changes are clamped to the configured
bounds; insufficient history holds the target steady.

## 2. Enabling it

Select a dynamic mode after setting the ceiling:

```rust
use millipede_core::{
    autoscale::AutoscaleMode,
    crawler::Crawler,
};

let crawler = Crawler::builder(kind)
    .max_concurrency(200)
    .autoscale_mode(AutoscaleMode::Aimd {
        increase_after_successes: 10,
        decrease_factor: 0.5,
    })
    .request_handler(handler)
    .storage_client(storage)
    .build()
    .await?;
```

Builder ordering is significant. `Crawler::builder(kind)` starts at fixed concurrency `10`.
`max_concurrency(n)` pins fixed concurrency to `n`; a later `autoscale_mode(...)` clears that pin
and retains `n` as the dynamic ceiling. Calling `max_concurrency` after `autoscale_mode` re-pins the
crawler at fixed concurrency.

## 3. Knobs and defaults

These are the actual `AutoscaledPoolOptions::default()` values. The crawler builder overrides the
first two relevant values to `fixed_concurrency: Some(10)` and `max_concurrency: 10`.

| Field | Pool default | Effect |
|---|---:|---|
| `fixed_concurrency` | `None` | `Some(n)` pins the effective desired/min/max value to `max(n, 1)` and disables scaling. |
| `min_concurrency` | `1` | Dynamic lower bound; the pool normalizes it to at least `1`. |
| `max_concurrency` | `200` | Dynamic ceiling; the pool normalizes it to at least the effective minimum. |
| `desired_concurrency` | `None` | Initial dynamic target; `None` uses the effective minimum, and an explicit value is clamped. |
| `scale_up_step_ratio` | `0.05` | Proportional LoadSignals increase, rounded up and at least one task. |
| `scale_down_step_ratio` | `0.05` | Proportional LoadSignals decrease, rounded up and at least one task. |
| `desired_utilization_ratio` | `0.9` | Minimum mean healthy-history ratio for LoadSignals scale-up. |
| `task_timeout` | `None` | Optional shared deadline, starting when an attempt starts, across request preparation, execution, and handler work; the handler also retains its own timeout. Cleanup and post-success work are outside this deadline. |
| `max_tasks_per_minute` | `None` | Optional global token bucket limiting actual task starts. |
| `same_domain_delay` | `0s` | Minimum spacing between reservations for the same URL host. |
| `maybe_run_interval` | `500ms` | Fallback dispatcher tick used as a missed-wakeup safety net; builders reject zero. |
| `autoscale_interval` | `10s` | Period between LoadSignals decisions. |
| `mode` | `Aimd { increase_after_successes: 10, decrease_factor: 0.5 }` | Selects outcome-driven AIMD or periodic load-signal scaling. |
| `snapshotter.window` | `30s` | Recent history requested from every registered signal. |
| `system_status.min_samples` | `0` | Configured minimum history for scale-up; evaluation enforces an effective minimum of one. |

`task_timeout` is one deadline for the attempt, not a fresh timeout per stage. For handler work the
engine uses the earlier of that deadline and `request_handler_timeout`.

## 4. Built-in load signals

Register `Arc<dyn LoadSignal>` values in `SnapshotterOptions.signals`, put those options in
`AutoscaledPoolOptions.snapshotter`, and select `AutoscaleMode::LoadSignals`.

| Signal name | What it samples | Options and default overload threshold |
|---|---|---|
| `cpu` | Aggregate system CPU used fraction, refreshed with `sysinfo`. | `CpuLoadSignalOptions`; `max_used_cpu_ratio: 0.95` (samples every `1s`). |
| `memory` | Used system memory divided by `memory_bytes`, or total system memory when no budget is set. | `MemoryLoadSignalOptions`; `max_used_memory_ratio: 0.9` (samples every `1s`). |
| `tokio-runtime` | Tokio timer scheduling lag using stable timer APIs. | `TokioRuntimeLoadSignalOptions`; `max_lag: 50ms` (probes every `250ms`). |
| `client` | Successful or rate-limited downstream client observations, recorded by a storage wrapper or manually. | No options struct; `ClientLoadSignal::overload_threshold()` returns `1.0`. |

For all four, overload is determined when the signal records each snapshot; `SystemStatus` consumes
the resulting boolean. `TokioRuntimeLoadSignal` deliberately uses stable timer-lag sampling and
does not require `tokio_unstable`. For storage activity, call
`ClientLoadSignal::instrument_storage(storage)` and pass the returned `StorageClient` to the crawler.
The wrapper automatically records successful storage operations as healthy and storage
rate-limit errors as overloaded. For other downstream clients, use `ClientLoadSignal::handle()` and
call `ClientLoadSignalHandle::record_healthy()` or `record_rate_limited()` manually.

If `LoadSignals` is selected with an empty signal list, the pool emits a warning and falls back to
AIMD with `increase_after_successes: 10` and `decrease_factor: 0.5`.

## 5. Rate limiting and politeness

`max_tasks_per_minute` creates one global token bucket. Its capacity is `max(value, 1)`, it begins
with one token, and it refills at `capacity / 60` tokens per second. The dispatcher checks it only
immediately before a task starts, so a token is consumed only by an actual start.

`same_domain_delay` spaces reservations independently by URL host. A 429 adds an exponential
per-host penalty with a one-second minimum base and a five-minute per-penalty cap; a later non-429
HTTP status clears that penalty. The limiter also honors any `Retry-After` duration already carried
by a `CrawlError` by extending the host's next-allowed instant.

`millipede-http` parses `Retry-After` as either delta-seconds or an HTTP date, attaches the duration
to its status error, and trust-caps parsed values at ten minutes. The engine passes that duration to
the host limiter. `AutoscaledPool::set_domain_delay_floor(host, duration)` supplies a persistent
host floor when another policy source provides one.

Politeness-delayed requests are parked by the dispatcher and do not occupy concurrency slots, so a
throttled host does not consume the slots that ready work can use. Held deferred leases are bounded
to `max(max_concurrency, 32)`. The current queue fetch order can still leave later hosts behind a
single-host flood once that buffer is full.

## 6. Observing it

Both `Crawler::autoscaler_snapshot()` and `CrawlerHandle::autoscaler_snapshot()` expose an
`AutoscalerSnapshot`; the handle returns `None` after the crawler is gone. Its fields are
`desired_concurrency`, `min_concurrency`, `max_concurrency`, and `is_fixed`.

```rust
let snapshot = crawler.autoscaler_snapshot();
tracing::info!(?snapshot, "autoscaler state");
```

Whenever the dispatcher observes a changed desired target, it emits a debug event under the
`millipede::concurrency` tracing target with `desired`, `current`, `min`, and `max` fields. For
example, use `RUST_LOG=millipede_core=debug` for crate-wide debugging or
`RUST_LOG=millipede::concurrency=debug` for this target.

The snapshot and tracing event are the shipped ways to observe desired concurrency. They do not
require a metrics exporter.

See `millipede/examples/autoscale_demo.rs` for a live AIMD sampler that polls the handle while crawling a
transiently failing mock server.

## 7. When to use `fixed_concurrency`

Prefer a fixed target for containerized deployments with hard resource budgets, paid-per-minute
proxy plans, reproducible benchmarks, and runs that need to isolate autoscaler behavior while
debugging.

With `fixed_concurrency: Some(n)`, the pool normalizes `n` to at least one, reports the same value as
desired/min/max, ignores the configured scaling mode, and never starts the snapshotter or
system-status background loop. Fixed and dynamic modes share the current engine dispatcher; fixed
mode simply gives it an invariant target.

## 8. Debugging a runaway crawl

| Symptom | What to check or change |
|---|---|
| AIMD is pinned at max | Increase `increase_after_successes`, lower `max_concurrency`, and verify setbacks really return `Err` from attempt work. |
| LoadSignals is pinned at max | Sustained signal histories have a mean healthy ratio at or above `desired_utilization_ratio`, so every evaluation scales up. Inspect each signal's recent observations, raise that threshold, reduce `snapshotter.window`, or lower `max_concurrency`. Attempt failures do not drive this mode. |
| AIMD collapsed to min | The attempt setback rate is high. Inspect terminal failures and your `failed_request_handler`; move `decrease_factor` toward `1.0` for gentler reductions. |
| LoadSignals collapsed to min | Any signal whose latest snapshot is overloaded triggers a proportional scale-down. One overloaded snapshot can remain latest across evaluations and drive the target to `min_concurrency`; inspect signal observations and thresholds. Attempt failures do not drive this mode. |
| One domain is hammered | Set a nonzero `same_domain_delay`; inspect whether your URLs expose the expected host. |
| Global start budget is exceeded | Set or lower `max_tasks_per_minute`; it controls starts, including retry attempts. |
| AIMD oscillates | Require a wider success streak with `increase_after_successes` and use a `decrease_factor` closer to `1.0`. |
| LoadSignals never scales up | Ensure `snapshotter.signals` is nonempty, each signal has at least `system_status.min_samples.max(1)` observations, and `snapshotter.window` retains them. Scale-up occurs only when the contributing signals' mean healthy ratio is at least `desired_utilization_ratio`; otherwise the decision is hold. |
