//! Crawl statistics: live counters, sliding-window rates, and persistence.

use crate::storage::{KeyValueStore, KeyValueStoreExt, StorageResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// KVS key used for statistics persistence (matches Crawlee).
pub const STATISTICS_PERSIST_KEY: &str = "SDK_CRAWLER_STATISTICS_0";

const DEFAULT_WINDOW: Duration = Duration::from_secs(60);
const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const ERROR_KEY_MAX_CHARS: usize = 200;

/// A cheaply cloned handle for recording and reading crawl statistics.
#[derive(Clone)]
pub struct StatisticsHandle {
    inner: Arc<StatisticsInner>,
}

struct StatisticsInner {
    state: Mutex<State>,
    subscribers: Mutex<Vec<mpsc::Sender<StatisticsSnapshot>>>,
    window: Duration,
}

struct State {
    requests_finished: u64,
    requests_failed: u64,
    requests_retries: u64,
    request_duration_sum: Duration,
    request_min_duration: Duration,
    request_max_duration: Duration,
    status_codes: BTreeMap<u16, u64>,
    retry_histogram: Vec<u64>,
    errors: BTreeMap<String, u64>,
    retry_errors: BTreeMap<String, u64>,
    accumulated_runtime: Duration,
    started_at: Option<Instant>,
    recent_events: VecDeque<(Instant, bool)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            requests_finished: 0,
            requests_failed: 0,
            requests_retries: 0,
            request_duration_sum: Duration::ZERO,
            request_min_duration: Duration::ZERO,
            request_max_duration: Duration::ZERO,
            status_codes: BTreeMap::new(),
            retry_histogram: Vec::new(),
            errors: BTreeMap::new(),
            retry_errors: BTreeMap::new(),
            accumulated_runtime: Duration::ZERO,
            started_at: None,
            recent_events: VecDeque::new(),
        }
    }
}

/// A point-in-time view of crawl statistics.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatisticsSnapshot {
    /// Number of successfully completed requests.
    pub requests_finished: u64,
    /// Number of terminally failed requests.
    pub requests_failed: u64,
    /// Number of retry attempts recorded.
    pub requests_retries: u64,
    /// Successful completions per minute in the configured sliding window.
    pub requests_finished_per_minute: f64,
    /// Terminal failures per minute in the configured sliding window.
    pub requests_failed_per_minute: f64,
    /// Average cumulative processing duration of terminal requests.
    pub request_avg_duration: Duration,
    /// Minimum cumulative processing duration, or zero before any completion.
    pub request_min_duration: Duration,
    /// Maximum cumulative processing duration, or zero before any completion.
    pub request_max_duration: Duration,
    /// Terminal successful responses grouped by HTTP status code.
    pub status_codes: BTreeMap<u16, u64>,
    /// Total time for which crawl runs have been active.
    pub crawler_runtime: Duration,
    /// Terminal completions grouped by retry count, indexed by retry count.
    pub retry_histogram: Vec<u64>,
    /// Terminal failures grouped by their capped error key.
    pub errors: BTreeMap<String, u64>,
    /// Retried failures grouped by their capped error key.
    pub retry_errors: BTreeMap<String, u64>,
}

/// Crawl statistics returned when a run finishes.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FinalStatistics {
    /// Number of successfully completed requests.
    pub requests_finished: u64,
    /// Number of terminally failed requests.
    pub requests_failed: u64,
    /// Number of retry attempts recorded.
    pub requests_retries: u64,
    /// Successful completions per minute in the configured sliding window.
    pub requests_finished_per_minute: f64,
    /// Terminal failures per minute in the configured sliding window.
    pub requests_failed_per_minute: f64,
    /// Average cumulative processing duration of terminal requests.
    pub request_avg_duration: Duration,
    /// Minimum cumulative processing duration, or zero before any completion.
    pub request_min_duration: Duration,
    /// Maximum cumulative processing duration, or zero before any completion.
    pub request_max_duration: Duration,
    /// Terminal successful responses grouped by HTTP status code.
    pub status_codes: BTreeMap<u16, u64>,
    /// Total time for which crawl runs have been active.
    pub crawler_runtime: Duration,
    /// Terminal completions grouped by retry count, indexed by retry count.
    pub retry_histogram: Vec<u64>,
    /// Terminal failures grouped by their capped error key.
    pub errors: BTreeMap<String, u64>,
    /// Retried failures grouped by their capped error key.
    pub retry_errors: BTreeMap<String, u64>,
}

impl From<StatisticsSnapshot> for FinalStatistics {
    fn from(snapshot: StatisticsSnapshot) -> Self {
        Self {
            requests_finished: snapshot.requests_finished,
            requests_failed: snapshot.requests_failed,
            requests_retries: snapshot.requests_retries,
            requests_finished_per_minute: snapshot.requests_finished_per_minute,
            requests_failed_per_minute: snapshot.requests_failed_per_minute,
            request_avg_duration: snapshot.request_avg_duration,
            request_min_duration: snapshot.request_min_duration,
            request_max_duration: snapshot.request_max_duration,
            status_codes: snapshot.status_codes,
            crawler_runtime: snapshot.crawler_runtime,
            retry_histogram: snapshot.retry_histogram,
            errors: snapshot.errors,
            retry_errors: snapshot.retry_errors,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedStatistics {
    requests_finished: u64,
    requests_failed: u64,
    requests_retries: u64,
    request_duration_sum: Duration,
    request_min_duration: Duration,
    request_max_duration: Duration,
    status_codes: BTreeMap<u16, u64>,
    retry_histogram: Vec<u64>,
    errors: BTreeMap<String, u64>,
    retry_errors: BTreeMap<String, u64>,
    accumulated_runtime: Duration,
}

impl StatisticsHandle {
    /// Creates an empty statistics handle with a 60-second rate window.
    #[must_use]
    pub fn new() -> Self {
        Self::with_window(DEFAULT_WINDOW)
    }

    /// Creates an empty statistics handle with a custom sliding-window duration.
    pub(crate) fn with_window(window: Duration) -> Self {
        Self {
            inner: Arc::new(StatisticsInner {
                state: Mutex::new(State::default()),
                subscribers: Mutex::new(Vec::new()),
                window,
            }),
        }
    }

    /// Records a successful terminal request.
    ///
    /// `duration` is the cumulative per-request processing time across all attempts.
    pub fn record_finished(&self, duration: Duration, status_code: Option<u16>, retry_count: u32) {
        let now = Instant::now();
        let mut state = self.lock();
        state.requests_finished += 1;
        state.record_duration(duration);
        state.record_terminal_retry_count(retry_count);
        if let Some(status_code) = status_code {
            *state.status_codes.entry(status_code).or_default() += 1;
        }
        state.recent_events.push_back((now, true));
        prune_window(&mut state, now, self.inner.window);
        drop(state);
        self.emit_snapshot();
    }

    /// Records a terminally failed request.
    ///
    /// `duration` is the cumulative per-request processing time across all attempts.
    pub fn record_failed(&self, duration: Duration, error_key: &str, retry_count: u32) {
        let now = Instant::now();
        let mut state = self.lock();
        state.requests_failed += 1;
        state.record_duration(duration);
        state.record_terminal_retry_count(retry_count);
        *state
            .errors
            .entry(normalize_error_key(error_key))
            .or_default() += 1;
        state.recent_events.push_back((now, false));
        prune_window(&mut state, now, self.inner.window);
        drop(state);
        self.emit_snapshot();
    }

    /// Records an error that caused a request retry.
    pub fn record_retry(&self, error_key: &str) {
        let now = Instant::now();
        let mut state = self.lock();
        state.requests_retries += 1;
        *state
            .retry_errors
            .entry(normalize_error_key(error_key))
            .or_default() += 1;
        prune_window(&mut state, now, self.inner.window);
        drop(state);
        self.emit_snapshot();
    }

    /// Starts runtime measurement if it is not already running.
    pub fn mark_run_started(&self) {
        let mut state = self.lock();
        if state.started_at.is_none() {
            state.started_at = Some(Instant::now());
        }
    }

    /// Stops runtime measurement if it is running and accumulates the elapsed time.
    pub fn mark_run_stopped(&self) {
        let mut state = self.lock();
        if let Some(started_at) = state.started_at.take() {
            state.accumulated_runtime += started_at.elapsed();
        }
    }

    /// Returns a point-in-time copy of the current statistics.
    #[must_use]
    pub fn snapshot(&self) -> StatisticsSnapshot {
        let now = Instant::now();
        let mut state = self.lock();
        prune_window(&mut state, now, self.inner.window);
        let (finished_in_window, failed_in_window) = state.recent_events.iter().fold(
            (0_u64, 0_u64),
            |(finished, failed), (_, succeeded)| {
                if *succeeded {
                    (finished + 1, failed)
                } else {
                    (finished, failed + 1)
                }
            },
        );
        let rate_factor = if self.inner.window.is_zero() {
            0.0
        } else {
            60.0 / self.inner.window.as_secs_f64()
        };
        let completed = state.requests_finished + state.requests_failed;
        StatisticsSnapshot {
            requests_finished: state.requests_finished,
            requests_failed: state.requests_failed,
            requests_retries: state.requests_retries,
            requests_finished_per_minute: finished_in_window as f64 * rate_factor,
            requests_failed_per_minute: failed_in_window as f64 * rate_factor,
            request_avg_duration: if completed == 0 {
                Duration::ZERO
            } else {
                duration_average(state.request_duration_sum, completed)
            },
            request_min_duration: state.request_min_duration,
            request_max_duration: state.request_max_duration,
            status_codes: state.status_codes.clone(),
            crawler_runtime: state.accumulated_runtime
                + state.started_at.map_or(Duration::ZERO, |start| now - start),
            retry_histogram: state.retry_histogram.clone(),
            errors: state.errors.clone(),
            retry_errors: state.retry_errors.clone(),
        }
    }

    /// Subscribes to live snapshots emitted after statistics are recorded.
    ///
    /// Updates are best effort: a slow receiver may skip intermediate snapshots, while a later
    /// record operation will still deliver the newest state once capacity is available.
    /// When called from within a Tokio runtime, snapshots are also emitted periodically. Outside
    /// a runtime, the receiver remains usable for snapshots triggered by record operations.
    #[must_use]
    pub fn subscribe(&self) -> mpsc::Receiver<StatisticsSnapshot> {
        let (sender, receiver) = mpsc::channel(16);
        self.inner
            .subscribers
            .lock()
            .expect("statistics subscribers mutex poisoned")
            .push(sender.clone());

        let statistics = self.clone();
        let update_interval = if self.inner.window.is_zero() {
            DEFAULT_UPDATE_INTERVAL
        } else {
            self.inner.window.min(DEFAULT_UPDATE_INTERVAL)
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut interval = tokio::time::interval(update_interval);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if sender.send(statistics.snapshot()).await.is_err() {
                        statistics
                            .inner
                            .subscribers
                            .lock()
                            .expect("statistics subscribers mutex poisoned")
                            .retain(|subscriber| !subscriber.is_closed());
                        break;
                    }
                }
            });
        }
        receiver
    }

    /// Returns the current statistics in their final run-result form.
    #[must_use]
    pub fn finalize(&self) -> FinalStatistics {
        self.snapshot().into()
    }

    /// Persists lossless accumulator state to a key-value store.
    pub async fn persist(&self, kvs: &dyn KeyValueStore) -> StorageResult<()> {
        let persisted = {
            let now = Instant::now();
            let state = self.lock();
            PersistedStatistics {
                requests_finished: state.requests_finished,
                requests_failed: state.requests_failed,
                requests_retries: state.requests_retries,
                request_duration_sum: state.request_duration_sum,
                request_min_duration: state.request_min_duration,
                request_max_duration: state.request_max_duration,
                status_codes: state.status_codes.clone(),
                retry_histogram: state.retry_histogram.clone(),
                errors: state.errors.clone(),
                retry_errors: state.retry_errors.clone(),
                accumulated_runtime: state.accumulated_runtime
                    + state.started_at.map_or(Duration::ZERO, |start| now - start),
            }
        };
        kvs.set(STATISTICS_PERSIST_KEY, &persisted).await
    }

    /// Restores persisted accumulator state, returning whether the key existed.
    ///
    /// Sliding-window events are intentionally not restored, and runtime resumes stopped.
    pub async fn restore(&self, kvs: &dyn KeyValueStore) -> StorageResult<bool> {
        let Some(persisted) = kvs
            .get::<PersistedStatistics>(STATISTICS_PERSIST_KEY)
            .await?
        else {
            return Ok(false);
        };
        let mut state = self.lock();
        *state = State {
            requests_finished: persisted.requests_finished,
            requests_failed: persisted.requests_failed,
            requests_retries: persisted.requests_retries,
            request_duration_sum: persisted.request_duration_sum,
            request_min_duration: persisted.request_min_duration,
            request_max_duration: persisted.request_max_duration,
            status_codes: persisted.status_codes,
            retry_histogram: persisted.retry_histogram,
            errors: persisted.errors,
            retry_errors: persisted.retry_errors,
            accumulated_runtime: persisted.accumulated_runtime,
            started_at: None,
            recent_events: VecDeque::new(),
        };
        Ok(true)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner.state.lock().expect("statistics mutex poisoned")
    }

    fn emit_snapshot(&self) {
        let snapshot = self.snapshot();
        self.inner
            .subscribers
            .lock()
            .expect("statistics subscribers mutex poisoned")
            .retain(|subscriber| {
                !matches!(
                    subscriber.try_send(snapshot.clone()),
                    Err(mpsc::error::TrySendError::Closed(_))
                )
            });
    }
}

impl Default for StatisticsHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    fn record_duration(&mut self, duration: Duration) {
        let first = self.requests_finished + self.requests_failed == 1;
        self.request_duration_sum += duration;
        if first || duration < self.request_min_duration {
            self.request_min_duration = duration;
        }
        if first || duration > self.request_max_duration {
            self.request_max_duration = duration;
        }
    }

    fn record_terminal_retry_count(&mut self, retry_count: u32) {
        let index = retry_count as usize;
        if self.retry_histogram.len() <= index {
            self.retry_histogram.resize(index + 1, 0);
        }
        self.retry_histogram[index] += 1;
    }
}

fn normalize_error_key(error_key: &str) -> String {
    let first_line = error_key.lines().next().unwrap_or(error_key);
    let mut normalized = String::new();

    for token in first_line.split_whitespace() {
        if !normalized.is_empty() {
            normalized.push(' ');
        }

        if token.starts_with("http://") || token.starts_with("https://") {
            normalized.push_str("<url>");
        } else if is_uuid_token(token) {
            normalized.push_str("<uuid>");
        } else {
            let mut previous_was_digit = false;
            for character in token.chars() {
                if character.is_ascii_digit() {
                    if !previous_was_digit {
                        normalized.push('#');
                    }
                    previous_was_digit = true;
                } else {
                    normalized.push(character);
                    previous_was_digit = false;
                }
            }
        }
    }

    normalized.chars().take(ERROR_KEY_MAX_CHARS).collect()
}

fn is_uuid_token(token: &str) -> bool {
    let expected_lengths = [8, 4, 4, 4, 12];
    let mut parts = token.split('-');

    for expected_length in expected_lengths {
        let Some(part) = parts.next() else {
            return false;
        };
        if part.len() != expected_length || !part.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return false;
        }
    }

    parts.next().is_none()
}

fn duration_average(total: Duration, count: u64) -> Duration {
    let seconds = total.as_secs() / count;
    let remaining_seconds = total.as_secs() % count;
    let remaining_nanos =
        u128::from(remaining_seconds) * 1_000_000_000 + u128::from(total.subsec_nanos());
    Duration::new(seconds, (remaining_nanos / u128::from(count)) as u32)
}

fn prune_window(state: &mut State, now: Instant, window: Duration) {
    while state
        .recent_events
        .front()
        .is_some_and(|(recorded_at, _)| now.duration_since(*recorded_at) > window)
    {
        state.recent_events.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters() {
        let statistics = StatisticsHandle::new();
        statistics.record_finished(Duration::from_millis(10), None, 0);
        statistics.record_finished(Duration::from_millis(20), None, 1);
        statistics.record_finished(Duration::from_millis(30), None, 1);
        statistics.record_failed(Duration::from_millis(40), "non-retryable: boom", 2);
        for _ in 0..3 {
            statistics.record_retry("retryable: flaky");
        }

        let snapshot = statistics.snapshot();
        assert_eq!(snapshot.requests_finished, 3);
        assert_eq!(snapshot.requests_failed, 1);
        assert_eq!(snapshot.requests_retries, 3);
        assert_eq!(snapshot.retry_histogram, vec![1, 2, 1]);
        assert_eq!(
            snapshot.errors,
            BTreeMap::from([("non-retryable: boom".to_owned(), 1)])
        );
        assert_eq!(
            snapshot.retry_errors,
            BTreeMap::from([("retryable: flaky".to_owned(), 3)])
        );
        assert_eq!(snapshot.request_min_duration, Duration::from_millis(10));
        assert_eq!(snapshot.request_max_duration, Duration::from_millis(40));
        assert_eq!(snapshot.request_avg_duration, Duration::from_millis(25));
    }

    #[test]
    fn normalize_error_key_collapses_digits() {
        assert_eq!(
            normalize_error_key("non-retryable: boom 42"),
            "non-retryable: boom #"
        );
        assert_eq!(
            normalize_error_key("non-retryable: boom 999"),
            "non-retryable: boom #"
        );
        assert_eq!(
            normalize_error_key("timeout after 5031ms"),
            "timeout after #ms"
        );
    }

    #[test]
    fn normalize_error_key_masks_urls() {
        assert_eq!(
            normalize_error_key("non-retryable: fetch https://a.com/1 failed"),
            "non-retryable: fetch <url> failed"
        );
        assert_eq!(
            normalize_error_key("non-retryable: fetch https://b.com/2 failed"),
            "non-retryable: fetch <url> failed"
        );
    }

    #[test]
    fn normalize_error_key_masks_uuid() {
        assert_eq!(
            normalize_error_key("session: id 550e8400-e29b-41d4-a716-446655440000 failed"),
            "session: id <uuid> failed"
        );
    }

    #[test]
    fn normalize_error_key_only_first_line() {
        assert_eq!(
            normalize_error_key("retryable: boom\ncaused by: id 42"),
            "retryable: boom"
        );
    }

    #[tokio::test]
    async fn similar_failures_group_into_one_bucket() {
        let statistics = StatisticsHandle::new();
        statistics.record_failed(
            Duration::from_millis(10),
            "non-retryable: fetch https://x/1 failed",
            0,
        );
        statistics.record_failed(
            Duration::from_millis(10),
            "non-retryable: fetch https://y/2 failed",
            0,
        );

        let snapshot = statistics.snapshot();
        assert_eq!(snapshot.errors.len(), 1);
        assert_eq!(snapshot.errors.values().next(), Some(&2));

        statistics.record_failed(
            Duration::from_millis(10),
            "session: fetch https://z/3 failed",
            0,
        );
        let snapshot = statistics.snapshot();
        assert_eq!(snapshot.errors.len(), 2);
        assert!(
            snapshot
                .errors
                .contains_key("non-retryable: fetch <url> failed")
        );
        assert!(snapshot.errors.contains_key("session: fetch <url> failed"));
    }

    #[test]
    fn subscribe_outside_tokio_runtime_does_not_panic() {
        let statistics = StatisticsHandle::new();
        let mut snapshots = statistics.subscribe();

        statistics.record_finished(Duration::ZERO, None, 0);

        assert_eq!(
            snapshots
                .try_recv()
                .expect("record operation should emit a snapshot")
                .requests_finished,
            1
        );
    }

    #[tokio::test]
    async fn sliding_window() {
        let statistics = StatisticsHandle::with_window(Duration::from_millis(50));
        for _ in 0..3 {
            statistics.record_finished(Duration::ZERO, None, 0);
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        statistics.record_finished(Duration::ZERO, None, 0);

        let snapshot = statistics.snapshot();
        assert_eq!(snapshot.requests_finished_per_minute, 1_200.0);
        assert_eq!(snapshot.requests_finished, 4);
    }

    #[tokio::test]
    async fn subscriber_receives_recorded_state() {
        let statistics = StatisticsHandle::new();
        let mut snapshots = statistics.subscribe();

        statistics.record_finished(Duration::from_millis(10), Some(200), 0);

        let snapshot = snapshots.recv().await.expect("snapshot channel closed");
        assert_eq!(snapshot.requests_finished, 1);
        assert_eq!(snapshot.status_codes, BTreeMap::from([(200, 1)]));
    }

    #[tokio::test]
    async fn subscriber_receives_periodic_updates_without_new_events() {
        let statistics = StatisticsHandle::with_window(Duration::from_millis(20));
        statistics.mark_run_started();
        let mut snapshots = statistics.subscribe();
        statistics.record_finished(Duration::ZERO, None, 0);

        let recorded = snapshots.recv().await.expect("snapshot channel closed");
        assert!(recorded.requests_finished_per_minute > 0.0);
        let recorded_runtime = recorded.crawler_runtime;

        tokio::time::sleep(Duration::from_millis(50)).await;
        let decayed = loop {
            let snapshot = snapshots.recv().await.expect("snapshot channel closed");
            if snapshot.requests_finished_per_minute == 0.0 {
                break snapshot;
            }
        };
        assert!(decayed.crawler_runtime > recorded_runtime);
    }

    #[tokio::test]
    async fn runtime_accumulates_across_start_stop_and_is_monotonic_while_running() {
        let statistics = StatisticsHandle::new();
        statistics.mark_run_started();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let first = statistics.snapshot().crawler_runtime;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second = statistics.snapshot().crawler_runtime;
        assert!(second >= first);
        statistics.mark_run_stopped();
        let stopped = statistics.snapshot().crawler_runtime;
        statistics.mark_run_stopped();
        statistics.mark_run_started();
        tokio::time::sleep(Duration::from_millis(5)).await;
        statistics.mark_run_stopped();
        assert!(statistics.snapshot().crawler_runtime > stopped);
    }

    #[test]
    fn status_codes() {
        let statistics = StatisticsHandle::new();
        statistics.record_finished(Duration::ZERO, Some(200), 0);
        statistics.record_finished(Duration::ZERO, Some(200), 0);
        statistics.record_finished(Duration::ZERO, Some(404), 0);
        assert_eq!(
            statistics.snapshot().status_codes,
            BTreeMap::from([(200, 2), (404, 1)])
        );
    }
}
