use super::LoadSignal;
use crate::errors::CrawlError;
use std::{fmt, sync::Arc, time::Duration};

/// Configuration for a collection of load signals and their sampling window.
#[derive(Clone)]
pub struct SnapshotterOptions {
    /// Signals evaluated by the autoscaler.
    pub signals: Vec<Arc<dyn LoadSignal>>,
    /// Sliding window requested from each signal.
    pub window: Duration,
}

impl Default for SnapshotterOptions {
    fn default() -> Self {
        Self {
            signals: Vec::new(),
            window: Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for SnapshotterOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotterOptions")
            .field("signal_count", &self.signals.len())
            .field("window", &self.window)
            .finish()
    }
}

/// Coordinates lifecycle and access to configured load signals.
pub struct Snapshotter {
    options: SnapshotterOptions,
}

impl Snapshotter {
    /// Creates a snapshotter from the supplied options.
    pub fn new(options: SnapshotterOptions) -> Self {
        Self { options }
    }

    /// Returns the configured load signals.
    pub fn signals(&self) -> &[Arc<dyn LoadSignal>] {
        &self.options.signals
    }

    /// Returns the sampling window requested from each signal.
    pub fn window(&self) -> Duration {
        self.options.window
    }

    /// Starts each signal, stopping at the first error.
    pub async fn start(&self) -> Result<(), CrawlError> {
        for signal in &self.options.signals {
            signal.start().await?;
        }
        Ok(())
    }

    /// Stops every signal and returns the first error encountered.
    pub async fn stop(&self) -> Result<(), CrawlError> {
        let mut first_error = None;
        for signal in &self.options.signals {
            if let Err(error) = signal.stop().await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
