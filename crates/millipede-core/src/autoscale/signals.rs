use super::{LoadSignal, LoadSnapshot};
use crate::errors::CrawlError;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

const HISTORY_MAX_AGE: Duration = Duration::from_secs(300);
const HISTORY_MAX_LEN: usize = 4096;
const MIN_SAMPLE_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Default)]
struct SnapshotHistory {
    snapshots: Mutex<VecDeque<LoadSnapshot>>,
}

impl SnapshotHistory {
    fn push(&self, snapshot: LoadSnapshot) {
        let mut snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        snapshots.push_back(snapshot);

        if let Some(cutoff) = snapshot.at.checked_sub(HISTORY_MAX_AGE) {
            while snapshots.front().is_some_and(|entry| entry.at < cutoff) {
                snapshots.pop_front();
            }
        }
        while snapshots.len() > HISTORY_MAX_LEN {
            snapshots.pop_front();
        }
    }

    fn sample(&self, window: Duration) -> Vec<LoadSnapshot> {
        let snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(cutoff) = Instant::now().checked_sub(window) else {
            return snapshots.iter().copied().collect();
        };
        snapshots
            .iter()
            .filter(|snapshot| snapshot.at >= cutoff)
            .copied()
            .collect()
    }
}

/// Options for periodic system CPU load sampling.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use = "CPU load signal options do nothing unless passed to CpuLoadSignal::new"]
pub struct CpuLoadSignalOptions {
    /// Maximum used CPU fraction before the signal reports overload.
    pub max_used_cpu_ratio: f32,
    /// Interval between CPU usage samples.
    pub sample_interval: Duration,
}

impl Default for CpuLoadSignalOptions {
    fn default() -> Self {
        Self {
            max_used_cpu_ratio: 0.95,
            sample_interval: Duration::from_secs(1),
        }
    }
}

/// Periodically samples aggregate system CPU usage.
pub struct CpuLoadSignal {
    options: CpuLoadSignalOptions,
    history: Arc<SnapshotHistory>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl CpuLoadSignal {
    /// Creates a CPU load signal with the supplied options.
    pub fn new(options: CpuLoadSignalOptions) -> Self {
        Self {
            options,
            history: Arc::new(SnapshotHistory::default()),
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        }
    }
}

impl Default for CpuLoadSignal {
    fn default() -> Self {
        Self::new(CpuLoadSignalOptions::default())
    }
}

#[async_trait::async_trait]
impl LoadSignal for CpuLoadSignal {
    fn name(&self) -> &str {
        "cpu"
    }

    fn overload_threshold(&self) -> f32 {
        self.options.max_used_cpu_ratio
    }

    async fn start(&self) -> Result<(), CrawlError> {
        let mut task = self.task.lock().unwrap_or_else(|error| error.into_inner());
        if task.is_some() || self.cancel.is_cancelled() {
            return Ok(());
        }

        let history = Arc::clone(&self.history);
        let cancel = self.cancel.child_token();
        let threshold = self.options.max_used_cpu_ratio;
        let interval = self
            .options
            .sample_interval
            .max(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        *task = Some(tokio::spawn(async move {
            let mut system = sysinfo::System::new();
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        system.refresh_cpu_usage();
                        let usage = system.global_cpu_usage() / 100.0;
                        history.push(LoadSnapshot {
                            at: Instant::now(),
                            overloaded: usage > threshold,
                        });
                    }
                }
            }
        }));
        Ok(())
    }

    async fn stop(&self) -> Result<(), CrawlError> {
        self.cancel.cancel();
        let task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
        Ok(())
    }

    fn sample(&self, window: Duration) -> Vec<LoadSnapshot> {
        self.history.sample(window)
    }
}

/// Options for periodic system memory load sampling.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use = "memory load signal options do nothing unless passed to MemoryLoadSignal::new"]
pub struct MemoryLoadSignalOptions {
    /// Maximum used memory fraction before the signal reports overload.
    pub max_used_memory_ratio: f32,
    /// Optional byte budget used instead of total system memory.
    pub memory_bytes: Option<u64>,
    /// Interval between memory usage samples. Values below 1 ms are clamped to 1 ms.
    pub sample_interval: Duration,
}

impl Default for MemoryLoadSignalOptions {
    fn default() -> Self {
        Self {
            max_used_memory_ratio: 0.9,
            memory_bytes: None,
            sample_interval: Duration::from_secs(1),
        }
    }
}

/// Periodically samples used system memory against a configurable budget.
pub struct MemoryLoadSignal {
    options: MemoryLoadSignalOptions,
    history: Arc<SnapshotHistory>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl MemoryLoadSignal {
    /// Creates a memory load signal with the supplied options.
    pub fn new(options: MemoryLoadSignalOptions) -> Self {
        Self {
            options,
            history: Arc::new(SnapshotHistory::default()),
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        }
    }
}

impl Default for MemoryLoadSignal {
    fn default() -> Self {
        Self::new(MemoryLoadSignalOptions::default())
    }
}

#[async_trait::async_trait]
impl LoadSignal for MemoryLoadSignal {
    fn name(&self) -> &str {
        "memory"
    }

    fn overload_threshold(&self) -> f32 {
        self.options.max_used_memory_ratio
    }

    async fn start(&self) -> Result<(), CrawlError> {
        let mut task = self.task.lock().unwrap_or_else(|error| error.into_inner());
        if task.is_some() || self.cancel.is_cancelled() {
            return Ok(());
        }

        let history = Arc::clone(&self.history);
        let cancel = self.cancel.child_token();
        let threshold = self.options.max_used_memory_ratio;
        let memory_bytes = self.options.memory_bytes;
        let sample_interval = self.options.sample_interval.max(MIN_SAMPLE_INTERVAL);
        *task = Some(tokio::spawn(async move {
            let mut system = sysinfo::System::new();
            let mut ticker = tokio::time::interval(sample_interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        system.refresh_memory();
                        let budget = memory_bytes.unwrap_or_else(|| system.total_memory());
                        let overloaded = budget != 0
                            && system.used_memory() as f64 / budget as f64 > f64::from(threshold);
                        history.push(LoadSnapshot { at: Instant::now(), overloaded });
                    }
                }
            }
        }));
        Ok(())
    }

    async fn stop(&self) -> Result<(), CrawlError> {
        self.cancel.cancel();
        let task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
        Ok(())
    }

    fn sample(&self, window: Duration) -> Vec<LoadSnapshot> {
        self.history.sample(window)
    }
}

/// Options for detecting Tokio executor scheduling lag.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use = "Tokio load signal options do nothing unless passed to TokioRuntimeLoadSignal::new"]
pub struct TokioRuntimeLoadSignalOptions {
    /// Maximum scheduling lag before the signal reports overload.
    pub max_lag: Duration,
    /// Interval between scheduling-lag probes. Values below 1 ms are clamped to 1 ms.
    pub sample_interval: Duration,
}

impl Default for TokioRuntimeLoadSignalOptions {
    fn default() -> Self {
        Self {
            max_lag: Duration::from_millis(50),
            sample_interval: Duration::from_millis(250),
        }
    }
}

/// Detects Tokio executor load by measuring stable-API timer scheduling lag.
///
/// This deliberately uses timer lag instead of the unstable Tokio runtime metrics
/// sketched in `INTERFACE.md` section 13, so it does not require `tokio_unstable`.
pub struct TokioRuntimeLoadSignal {
    options: TokioRuntimeLoadSignalOptions,
    history: Arc<SnapshotHistory>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl TokioRuntimeLoadSignal {
    /// Creates a Tokio runtime load signal with the supplied options.
    pub fn new(options: TokioRuntimeLoadSignalOptions) -> Self {
        Self {
            options,
            history: Arc::new(SnapshotHistory::default()),
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        }
    }
}

impl Default for TokioRuntimeLoadSignal {
    fn default() -> Self {
        Self::new(TokioRuntimeLoadSignalOptions::default())
    }
}

fn lag_overloaded(lag: Duration, max_lag: Duration) -> bool {
    lag > max_lag
}

#[async_trait::async_trait]
impl LoadSignal for TokioRuntimeLoadSignal {
    fn name(&self) -> &str {
        "tokio-runtime"
    }

    /// Returns the lag-to-sampling-interval ratio as informational metadata.
    ///
    /// [`SystemStatus`](super::SystemStatus) consumes recorded overload flags
    /// directly rather than interpreting this value.
    fn overload_threshold(&self) -> f32 {
        self.options.max_lag.as_secs_f32()
            / self
                .options
                .sample_interval
                .max(MIN_SAMPLE_INTERVAL)
                .as_secs_f32()
    }

    async fn start(&self) -> Result<(), CrawlError> {
        let mut task = self.task.lock().unwrap_or_else(|error| error.into_inner());
        if task.is_some() || self.cancel.is_cancelled() {
            return Ok(());
        }

        let history = Arc::clone(&self.history);
        let cancel = self.cancel.child_token();
        let max_lag = self.options.max_lag;
        let sample_interval = self.options.sample_interval.max(MIN_SAMPLE_INTERVAL);
        *task = Some(tokio::spawn(async move {
            loop {
                let target = Instant::now() + sample_interval;
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep_until(target) => {
                        let lag = Instant::now().saturating_duration_since(target);
                        history.push(LoadSnapshot {
                            at: Instant::now(),
                            overloaded: lag_overloaded(lag, max_lag),
                        });
                    }
                }
            }
        }));
        Ok(())
    }

    async fn stop(&self) -> Result<(), CrawlError> {
        self.cancel.cancel();
        let task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
        Ok(())
    }

    fn sample(&self, window: Duration) -> Vec<LoadSnapshot> {
        self.history.sample(window)
    }
}

/// A manually-fed signal representing downstream client throttling.
///
/// Use [`ClientLoadSignal::instrument_storage`] to automatically record successful storage
/// operations and backend rate-limit errors. Callers can also feed observations directly through
/// [`ClientLoadSignalHandle`].
pub struct ClientLoadSignal {
    history: Arc<SnapshotHistory>,
}

impl ClientLoadSignal {
    /// Creates an empty client load signal.
    pub fn new() -> Self {
        Self {
            history: Arc::new(SnapshotHistory::default()),
        }
    }

    /// Returns a cloneable handle for recording client health observations.
    pub fn handle(&self) -> ClientLoadSignalHandle {
        ClientLoadSignalHandle {
            history: Arc::clone(&self.history),
        }
    }

    /// Wraps a storage client so its successful and rate-limited operations feed this signal.
    pub fn instrument_storage(
        &self,
        client: std::sync::Arc<dyn crate::storage::StorageClient>,
    ) -> std::sync::Arc<dyn crate::storage::StorageClient> {
        crate::storage::RateLimitReportingClient::new(client, self.handle())
    }
}

impl Default for ClientLoadSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LoadSignal for ClientLoadSignal {
    fn name(&self) -> &str {
        "client"
    }

    fn overload_threshold(&self) -> f32 {
        1.0
    }

    fn sample(&self, window: Duration) -> Vec<LoadSnapshot> {
        self.history.sample(window)
    }
}

/// Cloneable manual observation handle for a [`ClientLoadSignal`].
#[derive(Clone)]
pub struct ClientLoadSignalHandle {
    history: Arc<SnapshotHistory>,
}

impl ClientLoadSignalHandle {
    /// Records a rate-limit response as an overloaded observation.
    pub fn record_rate_limited(&self) {
        self.history.push(LoadSnapshot {
            at: Instant::now(),
            overloaded: true,
        });
    }

    /// Records a successful client interaction as a healthy observation.
    pub fn record_healthy(&self) {
        self.history.push(LoadSnapshot {
            at: Instant::now(),
            overloaded: false,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_overload_uses_strict_boundary() {
        let max_lag = Duration::from_millis(50);
        assert!(!lag_overloaded(max_lag, max_lag));
        assert!(lag_overloaded(max_lag + Duration::from_nanos(1), max_lag));
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_history_caps_length_and_prunes_old_entries() {
        let history = SnapshotHistory::default();
        for _ in 0..=HISTORY_MAX_LEN {
            history.push(LoadSnapshot {
                at: Instant::now(),
                overloaded: false,
            });
        }
        assert_eq!(history.sample(Duration::MAX).len(), HISTORY_MAX_LEN);

        tokio::time::advance(HISTORY_MAX_AGE + Duration::from_secs(1)).await;
        history.push(LoadSnapshot {
            at: Instant::now(),
            overloaded: true,
        });
        let samples = history.sample(Duration::MAX);
        assert_eq!(samples.len(), 1);
        assert!(samples[0].overloaded);
    }
}
