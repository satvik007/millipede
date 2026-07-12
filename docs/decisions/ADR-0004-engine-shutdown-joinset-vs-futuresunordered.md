# ADR-0004: Engine shutdown — JoinSet vs FuturesUnordered

## Status

Accepted.

## Context

The Phase 2 engine needs both graceful drain and hard abort. `stop()` stops admitting new work while allowing in-flight requests to finish; `abort()` cancels in-flight work and returns its leases. The dispatcher also needs per-task identity so that every completion, including a panic reported by `JoinError::is_panic`, maps back to the retained queue `Lease` that it must mark handled, reclaim, or abandon.

## Decision

The dispatcher is a single task owning a `FuturesUnordered` of spawned `JoinHandle`s paired with slot ids, a `HashMap<slot, (Lease, AbortHandle, Instant)>`, a `tokio::sync::Notify` for wakeups, and two `tokio_util::sync::CancellationToken`s. The drain token is latched into a local flag after its first observation so that its permanently ready `cancelled()` future does not become a hot select arm. The cancel token drives hard cancellation. Spawned tasks provide panic isolation: a panic surfaces as a `JoinError` rather than unwinding the dispatcher.

`JoinSet` is rejected as the dispatcher's primary structure. `JoinSet::shutdown()` conflates drain and abort: it only aborts, as verified by the executable spike. Although `join_next_with_id` provides ids, graceful drain and selective abort would reimplement the same bookkeeping that `FuturesUnordered` makes explicit. The ROADMAP also mandates retaining the `FuturesUnordered + Notify + CancellationToken` shape for the Phase 4 autoscaled scheduler, so adopting `JoinSet` now would create churn. `JoinSet` remains acceptable for incidental owned worker sets outside the dispatcher.

## Consequences

The dispatcher retains leases, so a lost task can never lose a lease. As a corollary, lease-consuming queue operations (`mark_handled`, `reclaim`, and `abandon`) are never wrapped in `tokio::time::timeout`: dropping such a future would drop the non-`Clone` `Lease` mid-operation. `internal_operation_timeout` applies only to droppable operations: `fetch_next`, `add`/`add_batch`, `is_finished`/`is_empty`, and statistics persistence.

`abort()` means cancelling the cancel token, calling `AbortHandle::abort` for every retained task, draining their join results, and abandoning the retained leases. The observed shutdown, wakeup, and cancellation semantics are pinned by `crates/millipede-core/tests/shutdown_spike.rs`.
