use super::{CrawlerHandle, CrawlerKind, CrawlerShared, RequestEnv, RequestOutcome, RequestPrep};
use crate::{
    errors::CrawlError,
    events::{CrawlerEvent, HandledRequest, RequestFinalState},
    handler::{FailedRequestContext, FailedRequestHandler, RequestHandler},
    request::{Request, RequestState},
    statistics::FinalStatistics,
    storage::{KeyValueStore, Lease, ReclaimOptions},
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use time::OffsetDateTime;
use tokio::{task::AbortHandle, time::Instant};

pub(crate) struct EngineOptions {
    pub(crate) max_concurrency: usize,
    pub(crate) max_request_retries: u32,
    pub(crate) max_session_rotations: u32,
    pub(crate) request_handler_timeout: Duration,
    pub(crate) internal_operation_timeout: Duration,
    pub(crate) persist_state_interval: Duration,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            max_concurrency: 1,
            max_request_retries: 3,
            max_session_rotations: 10,
            request_handler_timeout: Duration::from_secs(60),
            internal_operation_timeout: Duration::from_secs(30),
            persist_state_interval: Duration::from_secs(60),
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
}

struct SlotState {
    lease: Lease,
    abort: AbortHandle,
    started: Instant,
}

type AttemptFuture = BoxFuture<'static, (u64, Result<AttemptOutput, tokio::task::JoinError>)>;

impl<K: CrawlerKind> Engine<K> {
    pub(crate) async fn run(self) -> Result<FinalStatistics, CrawlError> {
        assert!(
            self.opts.max_concurrency >= 1,
            "max_concurrency must be at least one"
        );
        self.shared.stats.mark_run_started();
        let mut in_flight: FuturesUnordered<AttemptFuture> = FuturesUnordered::new();
        let mut slots = HashMap::new();
        let mut accumulated = HashMap::new();
        let mut next_slot = 0_u64;
        let mut draining = false;
        let mut ticker = tokio::time::interval(self.opts.persist_state_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if self.shared.cancel.is_cancelled() {
                self.abort_remaining(&mut in_flight, &mut slots).await;
                return Ok(self.finish().await);
            }
            if !draining && self.shared.drain.is_cancelled() {
                draining = true;
            }

            while !draining && in_flight.len() < self.opts.max_concurrency {
                match tokio::time::timeout(
                    self.opts.internal_operation_timeout,
                    self.shared.queue.fetch_next(),
                )
                .await
                {
                    Ok(Ok(Some(lease))) => {
                        let slot = next_slot;
                        next_slot = next_slot.wrapping_add(1);
                        let started = Instant::now();
                        let handle = self.spawn_attempt(lease.request.clone());
                        let abort = handle.abort_handle();
                        in_flight.push(Box::pin(async move { (slot, handle.await) }));
                        slots.insert(
                            slot,
                            SlotState {
                                lease,
                                abort,
                                started,
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

            if in_flight.is_empty() {
                if draining {
                    break;
                }
                let notified = self.shared.notify.notified();
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
                    _ = notified => {},
                    _ = self.shared.cancel.cancelled() => {},
                    _ = self.shared.drain.cancelled(), if !draining => { draining = true; },
                    _ = ticker.tick() => self.persist_state().await,
                }
                continue;
            }

            tokio::select! {
                Some((slot, joined)) = in_flight.next() => {
                    if let Err(error) = self.handle_completion(slot, joined, &mut slots, &mut accumulated).await {
                        self.abort_remaining(&mut in_flight, &mut slots).await;
                        let _ = self.finish().await;
                        return Err(error);
                    }
                },
                _ = self.shared.cancel.cancelled() => {},
                _ = self.shared.drain.cancelled(), if !draining => { draining = true; },
                _ = self.shared.notify.notified(), if !draining => {},
                _ = ticker.tick() => self.persist_state().await,
            }
        }

        Ok(self.finish().await)
    }

    fn spawn_attempt(&self, request: Request) -> tokio::task::JoinHandle<AttemptOutput> {
        let kind = self.kind.clone();
        let handler = self.handler.clone();
        let shared = self.shared.clone();
        let crawler = CrawlerHandle::new(Arc::downgrade(&shared));
        let events = shared.events.clone();
        let request_handler_timeout = self.opts.request_handler_timeout;
        tokio::spawn(async move {
            let started = Instant::now();
            let mut prep = RequestPrep { request };
            if let Err(error) = kind.before_request(&mut prep).await {
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
                };
            }
            let request_owned = prep.request;
            let request_arc = Arc::new(request_owned.clone());
            let mut ctx = match kind
                .execute(RequestEnv {
                    request: request_arc.clone(),
                    crawler,
                    events: &events,
                })
                .await
            {
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
                    };
                }
            };
            let result =
                match tokio::time::timeout(request_handler_timeout, handler.handle(ctx.clone()))
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
    ) -> Result<(), CrawlError> {
        let SlotState {
            mut lease,
            abort: _,
            started,
        } = slots
            .remove(&slot)
            .expect("completed task has a retained lease");
        let (result, duration) = match joined {
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
                (output.result, output.duration)
            }
        };
        let key = lease.request.unique_key.clone();
        let total = accumulated.get(&key).copied().unwrap_or_default() + duration;
        match result {
            Ok(()) => {
                accumulated.remove(&key);
                lease.request.state = RequestState::Done;
                lease.request.handled_at = Some(OffsetDateTime::now_utc());
                let snapshot = Arc::new(lease.request.clone());
                let retry_count = snapshot.retry_count;
                self.shared
                    .queue
                    .mark_handled(lease)
                    .await
                    .map_err(storage_failure)?;
                self.shared.stats.record_finished(total, None, retry_count);
                let handled = HandledRequest {
                    request: snapshot,
                    loaded_url: None,
                    outcome: RequestFinalState::Succeeded,
                    response_status: None,
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
                if let Err(abandon_error) = self.shared.queue.abandon(lease).await {
                    tracing::warn!(%abandon_error, "failed to abandon critical request lease");
                }
                // The error is held in an Arc shared with lifecycle code. Recreate the same
                // critical classification and display text as an owned dispatcher signal.
                Err(CrawlError::critical(anyhow::anyhow!(error.to_string())))
            }
            Err(error) => {
                let eligible = error.ignores_max_retries()
                    || (error.is_retryable()
                        && !lease.request.no_retry
                        && if error.rotates_session() {
                            lease.request.session_rotation_count < self.opts.max_session_rotations
                        } else {
                            lease.request.retry_count
                                < lease
                                    .request
                                    .max_retries
                                    .unwrap_or(self.opts.max_request_retries)
                        });
                if eligible {
                    accumulated.insert(key, total);
                    let error_text = error.to_string();
                    push_error(&mut lease.request, &error_text);
                    self.shared.stats.record_retry(&error_text);
                    let rotates = error.rotates_session();
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
                    loaded_url: None,
                    outcome: RequestFinalState::Failed,
                    response_status: None,
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
        crawler::{BasicContext, BasicKind},
        events::EventBus,
        router::Router,
        statistics::STATISTICS_PERSIST_KEY,
        storage::{AddOptions, StorageClient},
    };
    use std::{
        collections::HashSet,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::sync::{Barrier, Notify};

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
        let mut opts = EngineOptions {
            max_concurrency,
            ..EngineOptions::default()
        };
        opts_mutator(&mut opts);
        let shared = Arc::new(CrawlerShared::new(
            queue,
            EventBus::default(),
            2048,
            opts.internal_operation_timeout,
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
        while completed.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        handle.stop();
        let stats = tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stats.requests_finished, 2);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }
}
