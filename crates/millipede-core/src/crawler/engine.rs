use super::{
    AttemptObservation, CrawlerHandle, CrawlerKind, CrawlerShared, RequestEnv, RequestOutcome,
    RequestPrep,
};
use crate::{
    autoscale::AttemptOutcomeKind,
    errors::CrawlError,
    events::{CrawlerEvent, HandledRequest, RequestFinalState},
    handler::{FailedRequestContext, FailedRequestHandler, RequestHandler},
    request::{Request, RequestState},
    retry_strategy::{AttemptOutcome, AttemptOverrides, RetryStrategy, SessionRetryAction},
    statistics::FinalStatistics,
    storage::{KeyValueStore, Lease, ReclaimOptions},
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::{task::AbortHandle, time::Instant};

#[derive(Clone)]
pub(crate) struct EngineOptions {
    pub(crate) max_request_retries: u32,
    pub(crate) max_session_rotations: u32,
    pub(crate) request_handler_timeout: Duration,
    pub(crate) internal_operation_timeout: Duration,
    pub(crate) persist_state_interval: Duration,
    /// Bounds preparation, execution, and handler work after an attempt starts.
    pub(crate) task_timeout: Option<Duration>,
    /// Periodically wakes an idle dispatcher as a fallback for missed notifications.
    pub(crate) maybe_run_interval: Duration,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            max_request_retries: 3,
            max_session_rotations: 10,
            request_handler_timeout: Duration::from_secs(60),
            internal_operation_timeout: Duration::from_secs(30),
            persist_state_interval: Duration::from_secs(60),
            task_timeout: None,
            maybe_run_interval: Duration::from_millis(500),
            retry_strategy: None,
        }
    }
}

pub(crate) struct Engine<K: CrawlerKind> {
    pub(crate) kind: Arc<K>,
    pub(crate) handler: Arc<dyn RequestHandler<K::Context>>,
    pub(crate) failed_handler: Option<Arc<dyn FailedRequestHandler>>,
    pub(crate) shared: Arc<CrawlerShared>,
    pub(crate) kvs: Option<Arc<dyn KeyValueStore>>,
    pub(crate) opts: EngineOptions,
}

struct AttemptOutput {
    result: Result<(), Arc<CrawlError>>,
    duration: Duration,
    request: Request,
    observation: AttemptObservation,
}

struct SlotState {
    lease: Lease,
    abort: AbortHandle,
    started: Instant,
}

struct DeferredStart {
    lease: Lease,
    carried: AttemptOverrides,
    domain_reserved: bool,
    renew_at: Instant,
}

const MIN_DEFERRED_CAPACITY: usize = 32;
const DEFERRED_LEASE_RENEWAL_MARGIN: Duration = Duration::from_secs(30);
const DEFERRED_LEASE_EXTENSION: Duration = Duration::from_secs(180);

type AttemptFuture = BoxFuture<'static, (u64, Result<AttemptOutput, tokio::task::JoinError>)>;

impl<K: CrawlerKind> Engine<K> {
    pub(crate) async fn run(self) -> Result<FinalStatistics, CrawlError> {
        self.shared.stats.mark_run_started();
        let bg_cancel = tokio_util::sync::CancellationToken::new();
        let background = {
            let shared = self.shared.clone();
            self.shared.pool.spawn_background(
                bg_cancel.clone(),
                Box::new(move || shared.notify.notify_waiters()),
            )
        };
        let outcome = self.dispatch_loop().await;
        bg_cancel.cancel();
        if let Some(handle) = background {
            let _ = handle.await;
        }
        match outcome {
            Ok(()) => Ok(self.finish().await),
            Err(error) => {
                let _ = self.finish().await;
                Err(error)
            }
        }
    }

    async fn dispatch_loop(&self) -> Result<(), CrawlError> {
        let mut in_flight: FuturesUnordered<AttemptFuture> = FuturesUnordered::new();
        let mut slots = HashMap::new();
        let mut accumulated = HashMap::new();
        let mut overrides: HashMap<String, AttemptOverrides> = HashMap::new();
        let mut deferred = BinaryHeap::new();
        let mut deferred_entries = HashMap::new();
        let mut next_slot = 0_u64;
        let mut next_deferred = 0_u64;
        let mut draining = false;
        let mut last_desired = self.shared.pool.desired_concurrency();
        let deferred_cap = self
            .shared
            .pool
            .max_concurrency()
            .max(MIN_DEFERRED_CAPACITY);
        let mut ticker = tokio::time::interval(self.opts.persist_state_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut fallback = tokio::time::interval(self.opts.maybe_run_interval);
        fallback.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if self.shared.cancel.is_cancelled() {
                self.abort_remaining(
                    &mut in_flight,
                    &mut slots,
                    &mut deferred,
                    &mut deferred_entries,
                )
                .await;
                return Ok(());
            }
            if !draining && self.shared.drain.is_cancelled() {
                draining = true;
            }
            if draining && !deferred_entries.is_empty() {
                self.abandon_deferred(&mut deferred, &mut deferred_entries)
                    .await;
            }
            if let Err(error) = self.renew_deferred_leases(&mut deferred_entries).await {
                self.abort_remaining(
                    &mut in_flight,
                    &mut slots,
                    &mut deferred,
                    &mut deferred_entries,
                )
                .await;
                return Err(error);
            }

            let desired = self.shared.pool.desired_concurrency();
            if desired != last_desired {
                tracing::debug!(
                    target: "millipede::concurrency",
                    desired,
                    current = in_flight.len(),
                    min = self.shared.pool.min_concurrency(),
                    max = self.shared.pool.max_concurrency(),
                    "desired concurrency changed"
                );
                last_desired = desired;
            }

            loop {
                while in_flight.len() < desired {
                    let Some(Reverse((ready_at, sequence))) = deferred.peek().copied() else {
                        break;
                    };
                    let now = Instant::now();
                    if ready_at > now {
                        break;
                    }
                    deferred.pop();
                    let mut entry = deferred_entries
                        .remove(&sequence)
                        .expect("deferred heap entry has retained state");
                    if !entry.domain_reserved {
                        entry.domain_reserved = true;
                        if let Some(host) = entry.lease.request.url.host_str() {
                            let wait = self.shared.pool.domain_slot_wait(host, now);
                            if !wait.is_zero() {
                                deferred.push(Reverse((now + wait, sequence)));
                                deferred_entries.insert(sequence, entry);
                                continue;
                            }
                        }
                    }
                    if let Some(wait) = self.shared.pool.task_token_wait(now) {
                        deferred.push(Reverse((now + wait, sequence)));
                        deferred_entries.insert(sequence, entry);
                        break;
                    }
                    let slot = next_slot;
                    next_slot = next_slot.wrapping_add(1);
                    let started = Instant::now();
                    let handle = self.spawn_attempt(entry.lease.request.clone(), entry.carried);
                    let abort = handle.abort_handle();
                    in_flight.push(Box::pin(async move { (slot, handle.await) }));
                    slots.insert(
                        slot,
                        SlotState {
                            lease: entry.lease,
                            abort,
                            started,
                        },
                    );
                }

                // The cap bounds held politeness-deferred leases. A single-host flood larger
                // than the cap can still delay later hosts until Phase 5's domain-aware frontier.
                // Fetch one lease at a time and immediately pump it so ready-now leases do not
                // accumulate behind a full in-flight set.
                if draining || in_flight.len() >= desired || deferred_entries.len() >= deferred_cap
                {
                    break;
                }
                match tokio::time::timeout(
                    self.opts.internal_operation_timeout,
                    self.shared.queue.fetch_next(),
                )
                .await
                {
                    Ok(Ok(Some(lease))) => {
                        let carried = overrides
                            .remove(&lease.request.unique_key)
                            .unwrap_or_default();
                        let sequence = next_deferred;
                        next_deferred = next_deferred.wrapping_add(1);
                        let ready_at = Instant::now() + carried.backoff.unwrap_or(Duration::ZERO);
                        let renew_in = lease
                            .expires_at
                            .saturating_duration_since(std::time::Instant::now())
                            .saturating_sub(DEFERRED_LEASE_RENEWAL_MARGIN);
                        deferred.push(Reverse((ready_at, sequence)));
                        deferred_entries.insert(
                            sequence,
                            DeferredStart {
                                lease,
                                carried,
                                domain_reserved: false,
                                renew_at: Instant::now() + renew_in,
                            },
                        );
                    }
                    Ok(Ok(None)) => break,
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "failed to fetch the next request");
                        break;
                    }
                    Err(_) => {
                        tracing::warn!("fetching the next request timed out");
                        break;
                    }
                }
            }

            // A past-ready entry may only be waiting for an in-flight slot. Arming an already
            // elapsed sleep for it would make this actor spin until a task completes.
            let now = Instant::now();
            let ready_wake = deferred
                .peek()
                .map(|entry| entry.0.0)
                .filter(|ready_at| *ready_at > now);
            let renewal_wake = deferred_entries.values().map(|entry| entry.renew_at).min();
            let next_deferred_wake = match (ready_wake, renewal_wake) {
                (Some(ready), Some(renewal)) => Some(ready.min(renewal)),
                (Some(ready), None) => Some(ready),
                (None, Some(renewal)) => Some(renewal),
                (None, None) => None,
            }
            .filter(|wake| *wake > now);
            if in_flight.is_empty() && deferred_entries.is_empty() {
                if draining {
                    break;
                }
                let notified = self.shared.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self
                    .queue_flag("is_finished", self.shared.queue.is_finished())
                    .await
                    == Some(true)
                {
                    break;
                }
                if self
                    .queue_flag("is_empty", self.shared.queue.is_empty())
                    .await
                    == Some(false)
                {
                    continue;
                }
                tokio::select! {
                    _ = &mut notified => {},
                    _ = self.shared.cancel.cancelled() => {},
                    _ = self.shared.drain.cancelled(), if !draining => { draining = true; },
                    _ = ticker.tick() => self.persist_state().await,
                    _ = wait_for_deferred(next_deferred_wake), if next_deferred_wake.is_some() => {},
                    _ = fallback.tick() => {},
                }
                continue;
            }

            tokio::select! {
                Some((slot, joined)) = in_flight.next() => {
                    if let Err(error) = self.handle_completion(slot, joined, &mut slots, &mut accumulated, &mut overrides).await {
                        self.abort_remaining(
                            &mut in_flight,
                            &mut slots,
                            &mut deferred,
                            &mut deferred_entries,
                        ).await;
                        return Err(error);
                    }
                },
                _ = self.shared.cancel.cancelled() => {},
                _ = self.shared.drain.cancelled(), if !draining => { draining = true; },
                _ = self.shared.notify.notified(), if !draining => {},
                _ = ticker.tick() => self.persist_state().await,
                _ = wait_for_deferred(next_deferred_wake), if next_deferred_wake.is_some() => {},
                _ = fallback.tick() => {},
            }
        }

        Ok(())
    }

    fn spawn_attempt(
        &self,
        request: Request,
        carried: AttemptOverrides,
    ) -> tokio::task::JoinHandle<AttemptOutput> {
        let kind = self.kind.clone();
        let handler = self.handler.clone();
        let shared = self.shared.clone();
        let crawler = CrawlerHandle::new(Arc::downgrade(&shared));
        let events = shared.events.clone();
        let request_handler_timeout = self.opts.request_handler_timeout;
        let task_timeout = self.opts.task_timeout;
        tokio::spawn(async move {
            let mut prep = RequestPrep { request };
            let started = Instant::now();
            let deadline = task_timeout.map(|timeout| started + timeout);
            let before_result = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, kind.before_request(&mut prep)).await {
                    Ok(result) => result,
                    Err(_) => Err(task_timeout_error(
                        task_timeout.expect("deadline has timeout"),
                    )),
                }
            } else {
                kind.before_request(&mut prep).await
            };
            if let Err(error) = before_result {
                let error = Arc::new(error);
                let request_owned = prep.request;
                let request_arc = Arc::new(request_owned.clone());
                if let Err(cleanup_error) = kind
                    .cleanup(RequestOutcome::ExecuteFailed {
                        request: request_arc,
                        error: error.clone(),
                    })
                    .await
                {
                    tracing::warn!(%cleanup_error, "request cleanup failed");
                }
                return AttemptOutput {
                    result: Err(error),
                    duration: started.elapsed(),
                    request: request_owned,
                    observation: AttemptObservation::default(),
                };
            }
            let request_owned = prep.request;
            let request_arc = Arc::new(request_owned.clone());
            let execute = kind.execute(RequestEnv {
                request: request_arc.clone(),
                crawler,
                events: &events,
                overrides: carried,
            });
            let execute_result = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, execute).await {
                    Ok(result) => result,
                    Err(_) => Err(task_timeout_error(
                        task_timeout.expect("deadline has timeout"),
                    )),
                }
            } else {
                execute.await
            };
            let mut ctx = match execute_result {
                Ok(ctx) => ctx,
                Err(error) => {
                    let error = Arc::new(error);
                    if let Err(cleanup_error) = kind
                        .cleanup(RequestOutcome::ExecuteFailed {
                            request: request_arc,
                            error: error.clone(),
                        })
                        .await
                    {
                        tracing::warn!(%cleanup_error, "request cleanup failed");
                    }
                    return AttemptOutput {
                        result: Err(error),
                        duration: started.elapsed(),
                        request: request_owned,
                        observation: AttemptObservation::default(),
                    };
                }
            };
            let observation = kind.observe(&ctx);
            let handler_deadline = deadline
                .map(|deadline| deadline.min(Instant::now() + request_handler_timeout))
                .unwrap_or_else(|| Instant::now() + request_handler_timeout);
            let result = match tokio::time::timeout_at(
                handler_deadline,
                handler.handle(ctx.clone()),
            )
            .await
            {
                Ok(Ok(())) => match kind.after_success(&mut ctx).await {
                    Ok(()) => {
                        if let Err(error) = kind.cleanup(RequestOutcome::Handled(ctx)).await {
                            tracing::warn!(%error, "request cleanup failed");
                        }
                        Ok(())
                    }
                    Err(error) => {
                        let error = Arc::new(error);
                        if let Err(cleanup_error) = kind
                            .cleanup(RequestOutcome::HandlerFailed {
                                ctx,
                                error: error.clone(),
                            })
                            .await
                        {
                            tracing::warn!(%cleanup_error, "request cleanup failed");
                        }
                        Err(error)
                    }
                },
                Ok(Err(error)) => {
                    let error = Arc::new(error);
                    if let Err(cleanup_error) = kind
                        .cleanup(RequestOutcome::HandlerFailed {
                            ctx,
                            error: error.clone(),
                        })
                        .await
                    {
                        tracing::warn!(%cleanup_error, "request cleanup failed");
                    }
                    Err(error)
                }
                Err(_) => {
                    let error = Arc::new(CrawlError::retry(anyhow::anyhow!(
                        "request handler timed out after {:?}",
                        request_handler_timeout
                    )));
                    if let Err(cleanup_error) = kind
                        .cleanup(RequestOutcome::HandlerFailed {
                            ctx,
                            error: error.clone(),
                        })
                        .await
                    {
                        tracing::warn!(%cleanup_error, "request cleanup failed");
                    }
                    Err(error)
                }
            };
            AttemptOutput {
                result,
                duration: started.elapsed(),
                request: request_owned,
                observation,
            }
        })
    }

    async fn queue_flag<F>(&self, operation: &str, future: F) -> Option<bool>
    where
        F: Future<Output = crate::storage::StorageResult<bool>>,
    {
        match tokio::time::timeout(self.opts.internal_operation_timeout, future).await {
            Ok(Ok(value)) => Some(value),
            Ok(Err(error)) => {
                tracing::warn!(%error, %operation, "queue operation failed");
                None
            }
            Err(_) => {
                tracing::warn!(%operation, "queue operation timed out");
                None
            }
        }
    }

    async fn handle_completion(
        &self,
        slot: u64,
        joined: Result<AttemptOutput, tokio::task::JoinError>,
        slots: &mut HashMap<u64, SlotState>,
        accumulated: &mut HashMap<String, Duration>,
        overrides: &mut HashMap<String, AttemptOverrides>,
    ) -> Result<(), CrawlError> {
        let SlotState {
            mut lease,
            abort: _,
            started,
        } = slots
            .remove(&slot)
            .expect("completed task has a retained lease");
        let host = lease.request.url.host_str().map(str::to_owned);
        let (result, duration, observation) = match joined {
            Err(error) if error.is_cancelled() => {
                self.shared
                    .queue
                    .abandon(lease)
                    .await
                    .map_err(storage_failure)?;
                return Ok(());
            }
            Err(error) => {
                let message = panic_message(error);
                (
                    Err(Arc::new(CrawlError::retry(anyhow::anyhow!(
                        "request handler panicked: {message}"
                    )))),
                    started.elapsed(),
                    AttemptObservation::default(),
                )
            }
            Ok(output) => {
                let (retry_count, rotations) = (
                    lease.request.retry_count,
                    lease.request.session_rotation_count,
                );
                lease.request = output.request;
                lease.request.retry_count = retry_count;
                lease.request.session_rotation_count = rotations;
                (output.result, output.duration, output.observation)
            }
        };
        let key = lease.request.unique_key.clone();
        let total = accumulated.get(&key).copied().unwrap_or_default() + duration;
        match &result {
            Ok(()) => {
                self.shared.pool.record_outcome(AttemptOutcomeKind::Success);
                if let Some(host) = &host {
                    self.shared
                        .pool
                        .note_response(host, observation.status, None, Instant::now());
                }
            }
            Err(error) => {
                self.shared.pool.record_outcome(AttemptOutcomeKind::Setback);
                if let Some(host) = &host {
                    self.shared.pool.note_response(
                        host,
                        observation.status.or_else(|| error.http_status()),
                        error.retry_after(),
                        Instant::now(),
                    );
                }
            }
        }
        match result {
            Ok(()) => {
                accumulated.remove(&key);
                overrides.remove(&key);
                lease.request.loaded_url = observation.loaded_url.clone();
                lease.request.state = RequestState::Done;
                lease.request.handled_at = Some(OffsetDateTime::now_utc());
                let snapshot = Arc::new(lease.request.clone());
                let retry_count = snapshot.retry_count;
                self.shared
                    .queue
                    .mark_handled(lease)
                    .await
                    .map_err(storage_failure)?;
                self.shared.stats.record_finished(
                    total,
                    observation.status.map(|status| status.as_u16()),
                    retry_count,
                );
                let handled = HandledRequest {
                    request: snapshot,
                    loaded_url: observation.loaded_url,
                    outcome: RequestFinalState::Succeeded,
                    response_status: observation.status,
                    retry_count,
                    duration: total,
                };
                self.shared
                    .events
                    .emit(CrawlerEvent::RequestFinished(handled.clone()));
                let _ = self.shared.results_tx.send(handled);
                Ok(())
            }
            Err(error) if error.is_critical() => {
                push_error(&mut lease.request, &error.to_string());
                accumulated.remove(&key);
                overrides.remove(&key);
                if let Err(abandon_error) = self.shared.queue.abandon(lease).await {
                    tracing::warn!(%abandon_error, "failed to abandon critical request lease");
                }
                // The error is held in an Arc shared with lifecycle code. Recreate the same
                // critical classification and display text as an owned dispatcher signal.
                Err(CrawlError::critical(anyhow::anyhow!(error.to_string())))
            }
            Err(error) => {
                let status = observation.status.or_else(|| error.http_status());
                let mut strategy_rotates = None;
                let eligible = if let Some(strategy) = &self.opts.retry_strategy {
                    if lease.request.no_retry {
                        false
                    } else {
                        let directive = strategy.on_retry(&AttemptOutcome {
                            request: &lease.request,
                            attempt: lease.request.retry_count,
                            status,
                            error: Some(&error),
                            anti_bot: match &*error {
                                CrawlError::AntiBotDetected { tech, .. } => Some(tech.clone()),
                                _ => None,
                            },
                            proxy_info: observation.proxy_info.as_ref(),
                            session_id: observation.session_id.as_ref(),
                            response_bytes: observation.response_bytes,
                        });
                        let rotates = error.rotates_session()
                            || matches!(
                                directive.session_action,
                                SessionRetryAction::Rotate | SessionRetryAction::Retire
                            );
                        let cap = lease.request.max_retries.unwrap_or(strategy.max_retries());
                        let eligible = directive.should_retry
                            && (error.ignores_max_retries()
                                || if rotates {
                                    lease.request.session_rotation_count
                                        < self.opts.max_session_rotations
                                } else {
                                    lease.request.retry_count < cap
                                });
                        if eligible
                            && (directive.proxy_kind.is_some()
                                || directive.user_agent_profile.is_some()
                                || directive.backoff.is_some())
                        {
                            overrides.insert(
                                key.clone(),
                                AttemptOverrides {
                                    proxy_kind: directive.proxy_kind,
                                    user_agent_profile: directive.user_agent_profile,
                                    backoff: directive.backoff,
                                },
                            );
                        }
                        strategy_rotates = Some(rotates);
                        eligible
                    }
                } else {
                    error.ignores_max_retries()
                        || (error.is_retryable()
                            && !lease.request.no_retry
                            && if error.rotates_session() {
                                lease.request.session_rotation_count
                                    < self.opts.max_session_rotations
                            } else {
                                lease.request.retry_count
                                    < lease
                                        .request
                                        .max_retries
                                        .unwrap_or(self.opts.max_request_retries)
                            })
                };
                if eligible {
                    accumulated.insert(key.clone(), total);
                    let error_text = error.to_string();
                    push_error(&mut lease.request, &error_text);
                    self.shared.stats.record_retry(&error_text);
                    let rotates = strategy_rotates.unwrap_or_else(|| error.rotates_session());
                    if rotates {
                        lease.request.session_rotation_count += 1;
                    }
                    self.shared
                        .queue
                        .reclaim(
                            lease,
                            ReclaimOptions {
                                forefront: false,
                                increment_retry: !rotates,
                            },
                        )
                        .await
                        .map_err(storage_failure)?;
                    return Ok(());
                }
                accumulated.remove(&key);
                overrides.remove(&key);
                let error_text = error.to_string();
                push_error(&mut lease.request, &error_text);
                lease.request.state = RequestState::Error;
                lease.request.handled_at = Some(OffsetDateTime::now_utc());
                let snapshot = Arc::new(lease.request.clone());
                let retry_count = snapshot.retry_count;
                if let Some(handler) = &self.failed_handler {
                    match tokio::time::timeout(
                        self.opts.request_handler_timeout,
                        AssertUnwindSafe(handler.handle(FailedRequestContext::new(
                            snapshot.clone(),
                            error,
                            retry_count,
                        )))
                        .catch_unwind(),
                    )
                    .await
                    {
                        Ok(Ok(Ok(()))) => {}
                        Ok(Ok(Err(handler_error))) => {
                            tracing::warn!(%handler_error, "failed request handler failed")
                        }
                        Ok(Err(payload)) => tracing::warn!(
                            panic = %panic_payload_message(payload),
                            "failed request handler panicked"
                        ),
                        Err(_) => tracing::warn!("failed request handler timed out"),
                    }
                }
                self.shared
                    .queue
                    .mark_handled(lease)
                    .await
                    .map_err(storage_failure)?;
                self.shared
                    .stats
                    .record_failed(total, &error_text, retry_count);
                self.shared.events.emit(CrawlerEvent::RequestFailed {
                    request: snapshot.clone(),
                    error: error_text,
                });
                let _ = self.shared.results_tx.send(HandledRequest {
                    request: snapshot,
                    loaded_url: observation.loaded_url,
                    outcome: RequestFinalState::Failed,
                    response_status: status,
                    retry_count,
                    duration: total,
                });
                Ok(())
            }
        }
    }

    async fn abort_remaining(
        &self,
        in_flight: &mut FuturesUnordered<AttemptFuture>,
        slots: &mut HashMap<u64, SlotState>,
        deferred: &mut BinaryHeap<Reverse<(Instant, u64)>>,
        deferred_entries: &mut HashMap<u64, DeferredStart>,
    ) {
        self.shared.events.emit(CrawlerEvent::Aborting);
        for state in slots.values() {
            state.abort.abort();
        }
        while in_flight.next().await.is_some() {}
        for (_, state) in slots.drain() {
            if let Err(error) = self.shared.queue.abandon(state.lease).await {
                tracing::warn!(%error, "failed to abandon request lease");
            }
        }
        self.abandon_deferred(deferred, deferred_entries).await;
    }

    async fn abandon_deferred(
        &self,
        deferred: &mut BinaryHeap<Reverse<(Instant, u64)>>,
        deferred_entries: &mut HashMap<u64, DeferredStart>,
    ) {
        deferred.clear();
        for (_, entry) in deferred_entries.drain() {
            if let Err(error) = self.shared.queue.abandon(entry.lease).await {
                tracing::warn!(%error, "failed to abandon deferred request lease");
            }
        }
    }

    async fn renew_deferred_leases(
        &self,
        deferred_entries: &mut HashMap<u64, DeferredStart>,
    ) -> Result<(), CrawlError> {
        let now = Instant::now();
        let due: Vec<_> = deferred_entries
            .iter()
            .filter_map(|(sequence, entry)| (entry.renew_at <= now).then_some(*sequence))
            .collect();
        for sequence in due {
            let entry = deferred_entries
                .get_mut(&sequence)
                .expect("due deferred lease has retained state");
            self.shared
                .queue
                .renew(&entry.lease.lease_id, DEFERRED_LEASE_EXTENSION)
                .await
                .map_err(storage_failure)?;
            entry.lease.expires_at += DEFERRED_LEASE_EXTENSION;
            entry.renew_at += DEFERRED_LEASE_EXTENSION;
        }
        Ok(())
    }

    async fn persist_state(&self) {
        self.shared.events.emit(CrawlerEvent::PersistState {
            is_migrating: false,
        });
        if let Some(kvs) = &self.kvs {
            match tokio::time::timeout(
                self.opts.internal_operation_timeout,
                self.shared.stats.persist(kvs.as_ref()),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "statistics persistence failed"),
                Err(_) => tracing::warn!("statistics persistence timed out"),
            }
        }
    }

    async fn finish(&self) -> FinalStatistics {
        self.shared.events.emit(CrawlerEvent::Exiting);
        self.persist_state().await;
        self.shared.stats.mark_run_stopped();
        self.shared.stats.finalize()
    }
}

fn storage_failure(error: crate::storage::StorageError) -> CrawlError {
    CrawlError::critical(anyhow::anyhow!(error.to_string()))
}

fn task_timeout_error(timeout: Duration) -> CrawlError {
    CrawlError::retry(anyhow::anyhow!("task timed out after {timeout:?}"))
}

async fn wait_for_deferred(wake: Option<Instant>) {
    if let Some(wake) = wake {
        tokio::time::sleep_until(wake).await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn push_error(request: &mut Request, error: &str) {
    request
        .error_messages
        .push(error.chars().take(200).collect());
}

fn panic_message(error: tokio::task::JoinError) -> String {
    match error.try_into_panic() {
        Ok(payload) => panic_payload_message(payload),
        Err(_) => "cancelled task".to_owned(),
    }
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_client::MemoryStorageClient;
    use crate::{
        autoscale::{
            AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions, LoadSignal, LoadSnapshot,
        },
        crawler::{BasicContext, BasicKind},
        events::EventBus,
        router::Router,
        statistics::STATISTICS_PERSIST_KEY,
        storage::{
            AddOptions, BatchAddHandle, LeaseId, QueueOpInfo, ReclaimOptions, RequestQueue,
            RequestSource, StorageClient, StorageResult,
        },
    };
    use std::{
        collections::HashSet,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use tokio::sync::{Barrier, Notify, mpsc};

    struct IdleWindowQueue {
        inner: Arc<dyn RequestQueue>,
        blocked_empty_check: AtomicBool,
        empty_check_started: Arc<Notify>,
        release_empty_check: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl RequestQueue for IdleWindowQueue {
        async fn add(&self, request: Request, options: AddOptions) -> StorageResult<QueueOpInfo> {
            self.inner.add(request, options).await
        }

        async fn add_batch(
            &self,
            requests: Vec<RequestSource>,
            options: AddOptions,
        ) -> StorageResult<BatchAddHandle> {
            self.inner.add_batch(requests, options).await
        }

        async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
            self.inner.fetch_next().await
        }

        async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
            self.inner.mark_handled(lease).await
        }

        async fn reclaim(&self, lease: Lease, options: ReclaimOptions) -> StorageResult<()> {
            self.inner.reclaim(lease, options).await
        }

        async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
            self.inner.renew(lease_id, extend_by).await
        }

        async fn abandon(&self, lease: Lease) -> StorageResult<()> {
            self.inner.abandon(lease).await
        }

        async fn is_empty(&self) -> StorageResult<bool> {
            let is_empty = self.inner.is_empty().await?;
            if is_empty && !self.blocked_empty_check.swap(true, Ordering::SeqCst) {
                self.empty_check_started.notify_one();
                self.release_empty_check.notified().await;
            }
            Ok(is_empty)
        }

        async fn is_finished(&self) -> StorageResult<bool> {
            self.inner.is_finished().await.map(|_| false)
        }

        async fn handled_count(&self) -> StorageResult<u64> {
            self.inner.handled_count().await
        }

        async fn pending_count(&self) -> StorageResult<u64> {
            self.inner.pending_count().await
        }
    }

    async fn engine_with<H, F>(
        n_requests: usize,
        max_concurrency: usize,
        handler: H,
        opts_mutator: F,
    ) -> (Engine<BasicKind>, Arc<CrawlerShared>)
    where
        H: RequestHandler<BasicContext>,
        F: FnOnce(&mut EngineOptions),
    {
        let storage = MemoryStorageClient::new();
        let queue = storage.open_request_queue(None).await.unwrap();
        let kvs = storage.open_key_value_store(None).await.unwrap();
        for index in 0..n_requests {
            queue
                .add(
                    Request::get(format!("https://example.invalid/{index}"))
                        .build()
                        .unwrap(),
                    AddOptions::default(),
                )
                .await
                .unwrap();
        }
        let mut opts = EngineOptions::default();
        opts_mutator(&mut opts);
        let pool = Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
            fixed_concurrency: Some(max_concurrency),
            max_concurrency,
            ..Default::default()
        }));
        let shared = Arc::new(CrawlerShared::new(
            queue,
            EventBus::default(),
            2048,
            opts.internal_operation_timeout,
            pool,
        ));
        (
            Engine {
                kind: Arc::new(BasicKind),
                handler: Arc::new(handler),
                failed_handler: None,
                shared: shared.clone(),
                kvs: Some(kvs),
                opts,
            },
            shared,
        )
    }

    fn replace_pool(engine: &mut Engine<BasicKind>, pool: Arc<AutoscaledPool>) {
        engine.shared = Arc::new(CrawlerShared::new(
            engine.shared.queue.clone(),
            engine.shared.events.clone(),
            2048,
            engine.opts.internal_operation_timeout,
            pool,
        ));
    }

    #[tokio::test]
    async fn respects_max_concurrency() {
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let handler = {
            let current = current.clone();
            let peak = peak.clone();
            move |_ctx: BasicContext| {
                let current = current.clone();
                let peak = peak.clone();
                let barrier = barrier.clone();
                async move {
                    let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    barrier.wait().await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };
        let (engine, _) = engine_with(16, 4, handler, |_| {}).await;
        engine.run().await.unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn past_ready_deferred_entry_does_not_starve_task_timer() {
        let (engine, _) = engine_with(
            2,
            1,
            |_ctx: BasicContext| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
            |_| {},
        )
        .await;

        let stats = engine.run().await.unwrap();

        assert_eq!(stats.requests_finished, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_lease_is_renewed_before_expiry() {
        let (engine, shared) =
            engine_with(1, 1, |_ctx: BasicContext| async { Ok(()) }, |_| {}).await;
        let lease = shared.queue.fetch_next().await.unwrap().unwrap();
        let original_expiry = lease.expires_at;
        let mut entries = HashMap::from([(
            0,
            DeferredStart {
                lease,
                carried: AttemptOverrides::default(),
                domain_reserved: false,
                renew_at: Instant::now(),
            },
        )]);

        engine.renew_deferred_leases(&mut entries).await.unwrap();

        let entry = entries.remove(&0).unwrap();
        assert_eq!(
            entry.lease.expires_at,
            original_expiry + DEFERRED_LEASE_EXTENSION
        );
        shared.queue.abandon(entry.lease).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_mode_grows_desired_concurrency_on_sustained_success() {
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let handler = {
            let current = current.clone();
            let peak = peak.clone();
            move |_ctx: BasicContext| {
                let current = current.clone();
                let peak = peak.clone();
                async move {
                    let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };
        let (mut engine, _) = engine_with(64, 2, handler, |_| {}).await;
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: None,
                min_concurrency: 1,
                max_concurrency: 8,
                desired_concurrency: Some(2),
                mode: AutoscaleMode::Aimd {
                    increase_after_successes: 1,
                    decrease_factor: 0.5,
                },
                ..Default::default()
            })),
        );

        engine.run().await.unwrap();
        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak > 2,
            "dynamic concurrency never grew beyond its initial value"
        );
        assert!(peak <= 8);
    }

    #[tokio::test(start_paused = true)]
    async fn same_domain_delay_enforced() {
        let times = Arc::new(Mutex::new(Vec::new()));
        let handler = {
            let times = times.clone();
            move |_ctx: BasicContext| {
                let times = times.clone();
                async move {
                    times.lock().unwrap().push(Instant::now());
                    Ok(())
                }
            }
        };
        let (mut engine, _) = engine_with(2, 2, handler, |_| {}).await;
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(2),
                max_concurrency: 2,
                same_domain_delay: Duration::from_millis(200),
                ..Default::default()
            })),
        );

        engine.run().await.unwrap();
        let times = times.lock().unwrap();
        assert_eq!(times.len(), 2);
        assert!(times[1].duration_since(times[0]) >= Duration::from_millis(200));
    }

    #[tokio::test(start_paused = true)]
    async fn same_domain_delay_does_not_block_a_different_host() {
        let times = Arc::new(Mutex::new(Vec::new()));
        let handler = {
            let times = times.clone();
            move |ctx: BasicContext| {
                let times = times.clone();
                async move {
                    times.lock().unwrap().push((
                        ctx.request.url.host_str().unwrap().to_owned(),
                        Instant::now(),
                    ));
                    Ok(())
                }
            }
        };
        let (mut engine, _) = engine_with(2, 3, handler, |_| {}).await;
        engine
            .shared
            .queue
            .add(
                Request::get("https://other.invalid/item").build().unwrap(),
                AddOptions::default(),
            )
            .await
            .unwrap();
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(3),
                max_concurrency: 3,
                same_domain_delay: Duration::from_millis(200),
                ..Default::default()
            })),
        );
        let started = Instant::now();

        engine.run().await.unwrap();
        let times = times.lock().unwrap();
        let other = times
            .iter()
            .find(|(host, _)| host == "other.invalid")
            .unwrap()
            .1;
        let mut same_host: Vec<_> = times
            .iter()
            .filter(|(host, _)| host == "example.invalid")
            .map(|(_, at)| *at)
            .collect();
        same_host.sort_unstable();
        assert!(other.duration_since(started) < Duration::from_millis(200));
        assert!(same_host[1].duration_since(same_host[0]) >= Duration::from_millis(200));
    }

    #[tokio::test(start_paused = true)]
    async fn domain_deferred_requests_do_not_burn_concurrency_slots() {
        let starts = Arc::new(Mutex::new(Vec::new()));
        let handler = {
            let starts = starts.clone();
            move |ctx: BasicContext| {
                let starts = starts.clone();
                async move {
                    starts.lock().unwrap().push((
                        ctx.request.url.host_str().unwrap().to_owned(),
                        Instant::now(),
                    ));
                    Ok(())
                }
            }
        };
        let (mut engine, _) = engine_with(3, 2, handler, |_| {}).await;
        engine
            .shared
            .queue
            .add(
                Request::get("https://b.invalid/item").build().unwrap(),
                AddOptions::default(),
            )
            .await
            .unwrap();
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(2),
                max_concurrency: 2,
                same_domain_delay: Duration::from_secs(5),
                ..Default::default()
            })),
        );
        let started = Instant::now();

        engine.run().await.unwrap();
        let starts = starts.lock().unwrap();
        let b_start = starts
            .iter()
            .find(|(host, _)| host == "b.invalid")
            .unwrap()
            .1;
        let mut a_starts: Vec<_> = starts
            .iter()
            .filter(|(host, _)| host == "example.invalid")
            .map(|(_, at)| *at)
            .collect();
        a_starts.sort_unstable();
        assert!(b_start.duration_since(started) < Duration::from_secs(1));
        assert!(a_starts[1].duration_since(a_starts[0]) >= Duration::from_secs(5));
        assert!(a_starts[2].duration_since(a_starts[0]) >= Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn max_tasks_per_minute_throttles_dispatch() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (mut engine, _) = engine_with(
            3,
            3,
            move |_ctx: BasicContext| {
                let tx = tx.clone();
                async move {
                    tx.send(Instant::now()).unwrap();
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(3),
                max_concurrency: 3,
                max_tasks_per_minute: Some(2),
                ..Default::default()
            })),
        );
        let started = Instant::now();
        let run = tokio::spawn(engine.run());

        let first = rx.recv().await.unwrap();
        assert_eq!(first.duration_since(started), Duration::ZERO);
        let second = rx.recv().await.unwrap();
        assert!(second.duration_since(started) >= Duration::from_secs(30));
        let third = rx.recv().await.unwrap();
        assert!(third.duration_since(started) >= Duration::from_secs(60));
        run.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn budget_token_not_lost_on_idle_empty_fetches() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (mut engine, original_shared) = engine_with(
            1,
            2,
            move |_ctx: BasicContext| {
                let tx = tx.clone();
                async move {
                    tx.send(Instant::now()).unwrap();
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let empty_check_started = Arc::new(Notify::new());
        let release_empty_check = Arc::new(Notify::new());
        let queue: Arc<dyn RequestQueue> = Arc::new(IdleWindowQueue {
            inner: original_shared.queue.clone(),
            blocked_empty_check: AtomicBool::new(false),
            empty_check_started: empty_check_started.clone(),
            release_empty_check: release_empty_check.clone(),
        });
        let shared = Arc::new(CrawlerShared::new(
            queue,
            EventBus::default(),
            2048,
            engine.opts.internal_operation_timeout,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(2),
                max_concurrency: 2,
                max_tasks_per_minute: Some(2),
                ..Default::default()
            })),
        ));
        engine.shared = shared.clone();
        let handle = CrawlerHandle::new(Arc::downgrade(&shared));
        let started = Instant::now();
        let run = tokio::spawn(engine.run());

        assert_eq!(rx.recv().await.unwrap(), started);
        empty_check_started.notified().await;
        release_empty_check.notify_one();
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(9)).await;
        handle
            .add_requests([Request::get("https://example.invalid/late")
                .build()
                .unwrap()])
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();

        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the available token should start the late request promptly")
            .unwrap();
        assert!(second.duration_since(started) < Duration::from_secs(41));
        handle.stop();
        run.await.unwrap().unwrap();
    }

    struct HealthySignal {
        stopped: Mutex<Option<mpsc::UnboundedSender<()>>>,
    }

    #[async_trait::async_trait]
    impl LoadSignal for HealthySignal {
        fn name(&self) -> &str {
            "healthy"
        }

        fn overload_threshold(&self) -> f32 {
            1.0
        }

        async fn stop(&self) -> Result<(), CrawlError> {
            if let Some(stopped) = self.stopped.lock().unwrap().take() {
                let _ = stopped.send(());
            }
            Ok(())
        }

        fn sample(&self, _window: Duration) -> Vec<LoadSnapshot> {
            vec![LoadSnapshot {
                at: Instant::now(),
                overloaded: false,
            }]
        }
    }

    #[tokio::test]
    async fn background_task_stops_after_run_completes() {
        let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
        let signal = Arc::new(HealthySignal {
            stopped: Mutex::new(Some(stopped_tx)),
        });
        let (mut engine, _) =
            engine_with(1, 1, |_ctx: BasicContext| async { Ok(()) }, |_| {}).await;
        let mut options = AutoscaledPoolOptions {
            fixed_concurrency: None,
            min_concurrency: 1,
            max_concurrency: 2,
            mode: AutoscaleMode::LoadSignals,
            ..Default::default()
        };
        options.snapshotter.signals.push(signal);
        replace_pool(&mut engine, Arc::new(AutoscaledPool::new(options)));

        engine.run().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), stopped_rx.recv())
            .await
            .expect("background signal was not stopped")
            .expect("stop notification channel closed");
    }

    #[tokio::test]
    async fn scale_change_callback_wakes_dispatcher() {
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let handler = {
            let started = started.clone();
            let release = release.clone();
            move |_ctx: BasicContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    let count = started.fetch_add(1, Ordering::SeqCst) + 1;
                    if count == 1 {
                        release.notified().await;
                    } else {
                        release.notify_waiters();
                    }
                    Ok(())
                }
            }
        };
        let (mut engine, _) = engine_with(4, 1, handler, |_| {}).await;
        let signal = Arc::new(HealthySignal {
            stopped: Mutex::new(None),
        });
        let mut options = AutoscaledPoolOptions {
            fixed_concurrency: None,
            min_concurrency: 1,
            max_concurrency: 4,
            desired_concurrency: Some(1),
            scale_up_step_ratio: 1.0,
            autoscale_interval: Duration::from_millis(10),
            mode: AutoscaleMode::LoadSignals,
            ..Default::default()
        };
        options.snapshotter.signals.push(signal);
        replace_pool(&mut engine, Arc::new(AutoscaledPool::new(options)));

        tokio::time::timeout(Duration::from_secs(1), engine.run())
            .await
            .expect("scale change did not wake the blocked dispatcher")
            .unwrap();
        assert!(started.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn retry_increments_retry_count() {
        let (engine, shared) = engine_with(
            3,
            2,
            |ctx: BasicContext| async move {
                if ctx.request.retry_count == 0 {
                    Err(CrawlError::retry(anyhow::anyhow!("again")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let mut results = shared.results_tx.subscribe();
        let stats = engine.run().await.unwrap();
        for _ in 0..3 {
            assert_eq!(results.recv().await.unwrap().retry_count, 1);
        }
        assert_eq!(stats.requests_retries, 3);
        assert_eq!(stats.requests_finished, 3);
    }

    #[tokio::test]
    async fn session_error_increments_session_rotation_count() {
        let (engine, shared) = engine_with(
            1,
            1,
            |ctx: BasicContext| async move {
                if ctx.request.session_rotation_count == 0 {
                    Err(CrawlError::session(anyhow::anyhow!("rotate")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let mut results = shared.results_tx.subscribe();
        engine.run().await.unwrap();
        let result = results.recv().await.unwrap();
        assert_eq!(result.request.session_rotation_count, 1);
        assert_eq!(result.retry_count, 0);
    }

    #[tokio::test]
    async fn force_retry_ignores_max_retries() {
        let (engine, shared) = engine_with(
            1,
            1,
            |ctx: BasicContext| async move {
                if ctx.request.retry_count < 5 {
                    Err(CrawlError::force_retry(anyhow::anyhow!("force")))
                } else {
                    Ok(())
                }
            },
            |opts| opts.max_request_retries = 1,
        )
        .await;
        let mut results = shared.results_tx.subscribe();
        let stats = engine.run().await.unwrap();
        assert_eq!(stats.requests_finished, 1);
        assert_eq!(results.recv().await.unwrap().retry_count, 5);
    }

    #[tokio::test]
    async fn non_retryable_calls_failure_handler() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut engine, shared) = engine_with(
            3,
            2,
            |_ctx: BasicContext| async {
                Err(CrawlError::non_retryable(anyhow::anyhow!("permanent")))
            },
            |_| {},
        )
        .await;
        let captured = calls.clone();
        engine.failed_handler = Some(Arc::new(move |ctx: FailedRequestContext| {
            let captured = captured.clone();
            async move {
                captured
                    .lock()
                    .unwrap()
                    .push((ctx.request.url.clone(), ctx.error.to_string()));
                Ok(())
            }
        }));
        let mut results = shared.results_tx.subscribe();
        let stats = engine.run().await.unwrap();
        assert_eq!(calls.lock().unwrap().len(), 3);
        assert_eq!(stats.requests_failed, 3);
        for _ in 0..3 {
            assert_eq!(
                results.recv().await.unwrap().outcome,
                RequestFinalState::Failed
            );
        }
    }

    #[tokio::test]
    async fn critical_halts_engine() {
        let (engine, shared) = engine_with(
            10,
            2,
            |ctx: BasicContext| async move {
                if ctx.request.url.path() == "/0" {
                    Err(CrawlError::critical(anyhow::anyhow!("halt")))
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        assert!(matches!(engine.run().await, Err(CrawlError::Critical(_))));
        assert!(shared.queue.handled_count().await.unwrap() < 10);
    }

    #[tokio::test]
    async fn handler_panic_reclaims_with_error() {
        let (engine, shared) = engine_with(
            1,
            1,
            |ctx: BasicContext| async move {
                if ctx.request.retry_count == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    panic!("boom");
                }
                Ok(())
            },
            |_| {},
        )
        .await;
        let mut results = shared.results_tx.subscribe();
        let stats = engine.run().await.unwrap();
        assert_eq!(results.recv().await.unwrap().retry_count, 1);
        assert!(
            stats
                .retry_errors
                .keys()
                .any(|key| key.contains("panicked"))
        );
    }

    #[tokio::test]
    async fn failed_request_handler_panic_does_not_stop_engine_or_leak_leases() {
        let (mut engine, shared) = engine_with(
            4,
            2,
            |ctx: BasicContext| async move {
                if ctx.request.url.path() == "/0" {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("permanent")))
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        engine.failed_handler = Some(Arc::new(|_ctx: FailedRequestContext| async move {
            panic!("failure handler boom");
        }));

        let stats = engine.run().await.unwrap();
        assert_eq!(stats.requests_finished, 3);
        assert_eq!(stats.requests_failed, 1);
        assert_eq!(shared.queue.handled_count().await.unwrap(), 4);
        assert_eq!(shared.queue.pending_count().await.unwrap(), 0);
        assert!(shared.queue.is_finished().await.unwrap());
    }

    #[tokio::test]
    async fn stop_drains_in_flight() {
        let started = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let handler = {
            let started = started.clone();
            let completed = completed.clone();
            move |_ctx: BasicContext| {
                let started = started.clone();
                let completed = completed.clone();
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };
        let (engine, shared) = engine_with(20, 4, handler, |_| {}).await;
        let run = tokio::spawn(engine.run());
        while started.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        let stopped = Instant::now();
        shared.drain.cancel();
        shared.notify.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(stopped.elapsed() < Duration::from_secs(2));
        assert_eq!(
            completed.load(Ordering::SeqCst),
            started.load(Ordering::SeqCst)
        );
        assert!(shared.queue.pending_count().await.unwrap() > 0);
    }

    #[tokio::test]
    async fn abort_cancels_in_flight() {
        let started = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let handler = {
            let started = started.clone();
            let notify = notify.clone();
            move |_ctx: BasicContext| {
                let started = started.clone();
                let notify = notify.clone();
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    notify.notify_waiters();
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok(())
                }
            }
        };
        let (engine, shared) = engine_with(4, 4, handler, |_| {}).await;
        let run = tokio::spawn(engine.run());
        loop {
            let notified = notify.notified();
            if started.load(Ordering::SeqCst) >= 4 {
                break;
            }
            notified.await;
        }
        shared.cancel.cancel();
        let stats = tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stats.requests_finished, 0);
        assert_eq!(shared.queue.pending_count().await.unwrap(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_leases_abandoned_on_abort() {
        let (mut engine, _) =
            engine_with(3, 1, |_ctx: BasicContext| async { Ok(()) }, |_| {}).await;
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(1),
                max_concurrency: 1,
                same_domain_delay: Duration::from_secs(60),
                ..Default::default()
            })),
        );
        let shared = engine.shared.clone();
        let mut results = shared.results_tx.subscribe();
        let run = tokio::spawn(engine.run());

        results.recv().await.unwrap();
        shared.cancel.cancel();
        shared.notify.notify_waiters();
        run.await.unwrap().unwrap();

        assert_eq!(shared.queue.pending_count().await.unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_leases_abandoned_on_graceful_stop() {
        let (mut engine, _) =
            engine_with(3, 1, |_ctx: BasicContext| async { Ok(()) }, |_| {}).await;
        replace_pool(
            &mut engine,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(1),
                max_concurrency: 1,
                same_domain_delay: Duration::from_secs(60),
                ..Default::default()
            })),
        );
        let shared = engine.shared.clone();
        let mut results = shared.results_tx.subscribe();
        let run = tokio::spawn(engine.run());

        results.recv().await.unwrap();
        shared.drain.cancel();
        shared.notify.notify_waiters();
        let stopped = Instant::now();
        run.await.unwrap().unwrap();

        assert!(stopped.elapsed() < Duration::from_secs(1));
        assert_eq!(shared.queue.pending_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn handler_timeout_is_retryable() {
        let (engine, _) = engine_with(
            1,
            1,
            |ctx: BasicContext| async move {
                if ctx.request.retry_count == 0 {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
                Ok(())
            },
            |opts| opts.request_handler_timeout = Duration::from_millis(50),
        )
        .await;
        let stats = engine.run().await.unwrap();
        assert!(
            stats
                .retry_errors
                .keys()
                .any(|key| key.contains("timed out"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn task_timeout_bounds_an_attempt() {
        let crawler = crate::crawler::CrawlerBuilder::new(BasicKind)
            .request_handler(|ctx: BasicContext| async move {
                if ctx.request.retry_count == 0 {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
                Ok(())
            })
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .autoscaled_pool_options(AutoscaledPoolOptions {
                fixed_concurrency: Some(1),
                max_concurrency: 1,
                task_timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        assert_eq!(crawler.opts.task_timeout, Some(Duration::from_millis(50)));

        let stats = crawler
            .run([Request::get("https://example.invalid/timeout")
                .build()
                .unwrap()])
            .await
            .unwrap();
        assert_eq!(stats.requests_finished, 1);
        assert!(
            stats
                .retry_errors
                .keys()
                .any(|key| key.contains("timed out"))
        );
    }

    #[tokio::test]
    async fn duration_accumulates_across_attempts() {
        let (engine, shared) = engine_with(
            1,
            1,
            |ctx: BasicContext| async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                if ctx.request.retry_count == 0 {
                    Err(CrawlError::retry(anyhow::anyhow!("again")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let mut results = shared.results_tx.subscribe();
        engine.run().await.unwrap();
        assert!(results.recv().await.unwrap().duration >= Duration::from_millis(60));
    }

    #[tokio::test]
    async fn missing_route_is_terminal_failure() {
        let router = Router::<BasicContext>::new();
        let errors = Arc::new(Mutex::new(Vec::new()));
        let (mut engine, _) = engine_with(1, 1, router, |_| {}).await;
        let captured = errors.clone();
        engine.failed_handler = Some(Arc::new(move |ctx: FailedRequestContext| {
            let captured = captured.clone();
            async move {
                captured.lock().unwrap().push(ctx.error.to_string());
                Ok(())
            }
        }));
        engine.run().await.unwrap();
        assert!(errors.lock().unwrap()[0].contains("missing route"));
    }

    #[tokio::test]
    async fn persist_state_emitted_and_stats_persisted() {
        let (engine, shared) = engine_with(
            8,
            2,
            |_ctx: BasicContext| async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(())
            },
            |opts| opts.persist_state_interval = Duration::from_millis(25),
        )
        .await;
        let kvs = engine.kvs.clone().unwrap();
        let mut events = shared.events.subscribe();
        engine.run().await.unwrap();
        let mut persisted = false;
        while let Ok(event) = events.try_recv() {
            persisted |= matches!(
                event,
                CrawlerEvent::PersistState {
                    is_migrating: false
                }
            );
        }
        assert!(persisted);
        assert!(
            kvs.get_bytes(STATISTICS_PERSIST_KEY)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[derive(Clone)]
    struct MutatingKind;

    impl CrawlerKind for MutatingKind {
        type Context = BasicContext;
        fn before_request<'a>(
            &'a self,
            prep: &'a mut RequestPrep,
        ) -> BoxFuture<'a, Result<(), CrawlError>> {
            Box::pin(async move {
                if prep.request.retry_count == 0 {
                    prep.request.headers.insert(
                        "x-engine-marker",
                        http::HeaderValue::from_static("first-attempt"),
                    );
                }
                Ok(())
            })
        }
        fn execute<'a>(
            &'a self,
            env: RequestEnv<'a>,
        ) -> BoxFuture<'a, Result<Self::Context, CrawlError>> {
            Box::pin(async move {
                Ok(BasicContext {
                    request: env.request,
                    crawler: env.crawler,
                })
            })
        }
        fn cleanup(
            &self,
            _: RequestOutcome<Self::Context>,
        ) -> BoxFuture<'_, Result<(), CrawlError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn before_request_mutation_survives_retry() {
        let (basic, shared) = engine_with(
            1,
            1,
            |ctx: BasicContext| async move {
                if ctx.request.retry_count == 0 {
                    Err(CrawlError::retry(anyhow::anyhow!("again")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let engine = Engine {
            kind: Arc::new(MutatingKind),
            handler: basic.handler,
            failed_handler: None,
            shared: shared.clone(),
            kvs: None,
            opts: basic.opts,
        };
        let mut results = shared.results_tx.subscribe();
        engine.run().await.unwrap();
        let result = results.recv().await.unwrap();
        assert_eq!(result.retry_count, 1);
        assert_eq!(result.request.headers["x-engine-marker"], "first-attempt");
    }

    #[derive(Clone)]
    struct CountingKind {
        handled: Arc<AtomicUsize>,
        handler_failed: Arc<AtomicUsize>,
        execute_failed: Arc<AtomicUsize>,
    }

    impl CrawlerKind for CountingKind {
        type Context = BasicContext;
        fn before_request<'a>(
            &'a self,
            prep: &'a mut RequestPrep,
        ) -> BoxFuture<'a, Result<(), CrawlError>> {
            Box::pin(async move {
                if prep.request.url.path() == "/3" {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("before")))
                } else {
                    Ok(())
                }
            })
        }
        fn execute<'a>(
            &'a self,
            env: RequestEnv<'a>,
        ) -> BoxFuture<'a, Result<Self::Context, CrawlError>> {
            Box::pin(async move {
                if env.request.url.path() == "/2" {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("execute")))
                } else {
                    Ok(BasicContext {
                        request: env.request,
                        crawler: env.crawler,
                    })
                }
            })
        }
        fn cleanup(
            &self,
            outcome: RequestOutcome<Self::Context>,
        ) -> BoxFuture<'_, Result<(), CrawlError>> {
            match outcome {
                RequestOutcome::Handled(_) => {
                    self.handled.fetch_add(1, Ordering::SeqCst);
                }
                RequestOutcome::HandlerFailed { .. } => {
                    self.handler_failed.fetch_add(1, Ordering::SeqCst);
                }
                RequestOutcome::ExecuteFailed { .. } => {
                    self.execute_failed.fetch_add(1, Ordering::SeqCst);
                }
            }
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn cleanup_called_on_every_outcome() {
        let handled = Arc::new(AtomicUsize::new(0));
        let handler_failed = Arc::new(AtomicUsize::new(0));
        let execute_failed = Arc::new(AtomicUsize::new(0));
        let (basic, shared) = engine_with(
            5,
            5,
            |ctx: BasicContext| async move {
                if ctx.request.url.path() == "/1" {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("handler")))
                } else if ctx.request.url.path() == "/4" {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("second handler")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let engine = Engine {
            kind: Arc::new(CountingKind {
                handled: handled.clone(),
                handler_failed: handler_failed.clone(),
                execute_failed: execute_failed.clone(),
            }),
            handler: basic.handler,
            failed_handler: None,
            shared,
            kvs: None,
            opts: basic.opts,
        };
        engine.run().await.unwrap();
        assert_eq!(handled.load(Ordering::SeqCst), 1);
        assert_eq!(handler_failed.load(Ordering::SeqCst), 2);
        assert_eq!(execute_failed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn result_stream_exactly_one_terminal_snapshot_and_never_blocks() {
        let (mut engine, _) = engine_with(
            100,
            8,
            |ctx: BasicContext| async move {
                if ctx.request.url.path().ends_with('0') {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("selected")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let small = Arc::new(CrawlerShared::new(
            engine.shared.queue.clone(),
            EventBus::default(),
            8,
            engine.opts.internal_operation_timeout,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(8),
                max_concurrency: 8,
                ..Default::default()
            })),
        ));
        engine.shared = small;
        let mut lagging_results = engine.shared.results_tx.subscribe();
        let stats = tokio::time::timeout(Duration::from_secs(1), engine.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stats.requests_finished + stats.requests_failed, 100);
        assert!(matches!(
            lagging_results.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));

        let (engine, shared) = engine_with(
            100,
            8,
            |ctx: BasicContext| async move {
                if ctx.request.url.path().ends_with('0') {
                    Err(CrawlError::non_retryable(anyhow::anyhow!("selected")))
                } else {
                    Ok(())
                }
            },
            |_| {},
        )
        .await;
        let mut results = shared.results_tx.subscribe();
        engine.run().await.unwrap();
        let mut keys = HashSet::new();
        let mut succeeded = 0;
        let mut failed = 0;
        for _ in 0..100 {
            let result = results.recv().await.unwrap();
            assert!(keys.insert(result.request.unique_key.clone()));
            match result.outcome {
                RequestFinalState::Succeeded => succeeded += 1,
                RequestFinalState::Failed => failed += 1,
                RequestFinalState::Skipped => {}
            }
        }
        assert_eq!((succeeded, failed), (90, 10));
        assert!(matches!(
            results.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
                | Err(tokio::sync::broadcast::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn idle_engine_picks_up_late_external_add() {
        let completed = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let handler = {
            let completed = completed.clone();
            let current = current.clone();
            let peak = peak.clone();
            move |_ctx: BasicContext| {
                let completed = completed.clone();
                let current = current.clone();
                let peak = peak.clone();
                async move {
                    let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };
        let (engine, shared) = engine_with(1, 2, handler, |opts| {
            opts.persist_state_interval = Duration::from_secs(60)
        })
        .await;
        let handle = CrawlerHandle::new(Arc::downgrade(&shared));
        let mut results = shared.results_tx.subscribe();
        let run = tokio::spawn(engine.run());
        tokio::time::sleep(Duration::from_millis(20)).await;
        handle
            .add_requests([Request::get("https://example.invalid/late")
                .build()
                .unwrap()])
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late external request should complete without waiting for the persist ticker");
        results.recv().await.unwrap();
        results.recv().await.unwrap();
        handle.stop();
        let stats = tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stats.requests_finished, 2);
        assert!(peak.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn idle_engine_preserves_notify_during_queue_check() {
        let completed = Arc::new(AtomicUsize::new(0));
        let (mut engine, original_shared) = engine_with(
            1,
            1,
            {
                let completed = completed.clone();
                move |_ctx: BasicContext| {
                    let completed = completed.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            },
            |opts| opts.persist_state_interval = Duration::from_secs(60),
        )
        .await;
        let empty_check_started = Arc::new(Notify::new());
        let release_empty_check = Arc::new(Notify::new());
        let queue: Arc<dyn RequestQueue> = Arc::new(IdleWindowQueue {
            inner: original_shared.queue.clone(),
            blocked_empty_check: AtomicBool::new(false),
            empty_check_started: empty_check_started.clone(),
            release_empty_check: release_empty_check.clone(),
        });
        let shared = Arc::new(CrawlerShared::new(
            queue,
            EventBus::default(),
            2048,
            engine.opts.internal_operation_timeout,
            Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
                fixed_concurrency: Some(1),
                max_concurrency: 1,
                ..Default::default()
            })),
        ));
        engine.shared = shared.clone();
        let handle = CrawlerHandle::new(Arc::downgrade(&shared));
        let run = tokio::spawn(engine.run());

        tokio::time::timeout(Duration::from_secs(1), empty_check_started.notified())
            .await
            .expect("engine should reach the empty-queue idle check");
        handle
            .add_requests([Request::get("https://example.invalid/late-idle")
                .build()
                .unwrap()])
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        release_empty_check.notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("enabled idle notification should survive the queue check await");
        handle.stop();
        let stats = tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stats.requests_finished, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_tick_recovers_missed_wakeup() {
        let completed = Arc::new(AtomicUsize::new(0));
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let (mut engine, original_shared) = engine_with(
            1,
            1,
            {
                let completed = completed.clone();
                let completed_tx = completed_tx.clone();
                move |_ctx: BasicContext| {
                    let completed = completed.clone();
                    let completed_tx = completed_tx.clone();
                    async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        completed_tx.send(()).unwrap();
                        Ok(())
                    }
                }
            },
            |opts| opts.maybe_run_interval = Duration::from_millis(100),
        )
        .await;
        let empty_check_started = Arc::new(Notify::new());
        let release_empty_check = Arc::new(Notify::new());
        let queue: Arc<dyn RequestQueue> = Arc::new(IdleWindowQueue {
            inner: original_shared.queue.clone(),
            blocked_empty_check: AtomicBool::new(false),
            empty_check_started: empty_check_started.clone(),
            release_empty_check: release_empty_check.clone(),
        });
        let shared = Arc::new(CrawlerShared::new(
            queue,
            EventBus::default(),
            2048,
            engine.opts.internal_operation_timeout,
            original_shared.pool.clone(),
        ));
        engine.shared = shared.clone();
        let handle = CrawlerHandle::new(Arc::downgrade(&shared));
        let run = tokio::spawn(engine.run());

        completed_rx.recv().await.unwrap();
        empty_check_started.notified().await;
        release_empty_check.notify_one();
        tokio::task::yield_now().await;
        shared
            .queue
            .add(
                Request::get("https://example.invalid/no-notify")
                    .build()
                    .unwrap(),
                AddOptions::default(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(350), completed_rx.recv())
            .await
            .expect("fallback tick did not recover the direct queue add")
            .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 2);

        handle.stop();
        run.await.unwrap().unwrap();
    }
}
