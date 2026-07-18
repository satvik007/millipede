use super::Snapshotter;
use tokio::time::Instant;

/// The concurrency adjustment recommended by current system load.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDecision {
    /// Increase desired concurrency.
    ScaleUp,
    /// Decrease desired concurrency.
    ScaleDown,
    /// Keep desired concurrency unchanged.
    Hold,
}

/// Options controlling load-history evaluation.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[must_use = "system status options do nothing unless passed to SystemStatus::new"]
pub struct SystemStatusOptions {
    /// Minimum observations a signal needs before it contributes to scale-up.
    pub min_samples: usize,
}

/// Evaluates load-signal histories into scaling decisions.
pub struct SystemStatus {
    options: SystemStatusOptions,
}

impl SystemStatus {
    /// Creates a status evaluator from the supplied options.
    pub fn new(options: SystemStatusOptions) -> Self {
        Self { options }
    }

    /// Evaluates all configured signals at the current Tokio-clock instant.
    pub fn evaluate(
        &self,
        snapshotter: &Snapshotter,
        desired_utilization_ratio: f32,
        _now: Instant,
    ) -> ScaleDecision {
        let min_samples = self.options.min_samples.max(1);
        let mut ratios = Vec::new();

        for signal in snapshotter.signals() {
            let samples = signal.sample(snapshotter.window());

            if samples
                .iter()
                .max_by_key(|sample| sample.at)
                .is_some_and(|sample| sample.overloaded)
            {
                return ScaleDecision::ScaleDown;
            }

            if samples.len() >= min_samples {
                let healthy = samples.iter().filter(|sample| !sample.overloaded).count();
                ratios.push(healthy as f32 / samples.len() as f32);
            }
        }

        if ratios.is_empty() {
            return ScaleDecision::Hold;
        }

        let mean = ratios.iter().sum::<f32>() / ratios.len() as f32;
        if mean >= desired_utilization_ratio {
            ScaleDecision::ScaleUp
        } else {
            ScaleDecision::Hold
        }
    }
}
