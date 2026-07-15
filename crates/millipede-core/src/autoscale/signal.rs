use crate::errors::CrawlError;
use std::time::Duration;
use tokio::time::Instant;

/// A point-in-time overload observation from a load signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadSnapshot {
    /// The Tokio-clock instant at which the observation was captured.
    pub at: Instant,
    /// Whether the signal was overloaded at that instant.
    pub overloaded: bool,
}

/// A source of recent system or client load observations.
#[async_trait::async_trait]
pub trait LoadSignal: Send + Sync + 'static {
    /// Returns the stable name of this signal.
    fn name(&self) -> &str;

    /// Returns the utilization threshold at which this signal is overloaded.
    fn overload_threshold(&self) -> f32;

    /// Starts any sampling work required by this signal.
    async fn start(&self) -> Result<(), CrawlError> {
        Ok(())
    }

    /// Stops any sampling work required by this signal.
    async fn stop(&self) -> Result<(), CrawlError> {
        Ok(())
    }

    /// Returns observations from the requested recent window.
    fn sample(&self, window: Duration) -> Vec<LoadSnapshot>;
}
