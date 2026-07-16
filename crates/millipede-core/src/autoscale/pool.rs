use super::{
    AimdController, ScaleDecision, Snapshotter, SnapshotterOptions, SystemStatus,
    SystemStatusOptions,
    rate_limit::{DomainLimiter, TaskRateLimiter},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Configuration for concurrency scaling and request politeness limits.
#[derive(Debug, Clone)]
pub struct AutoscaledPoolOptions {
    /// Pins concurrency and disables all autoscaling when set.
    pub fixed_concurrency: Option<usize>,
    /// Minimum dynamic concurrency.
    pub min_concurrency: usize,
    /// Maximum dynamic concurrency.
    pub max_concurrency: usize,
    /// Initial desired concurrency, or the minimum when omitted.
    pub desired_concurrency: Option<usize>,
    /// Proportional increase applied by load-signal scaling.
    pub scale_up_step_ratio: f32,
    /// Proportional decrease applied by load-signal scaling.
    pub scale_down_step_ratio: f32,
    /// Mean healthy-history ratio required to scale up.
    pub desired_utilization_ratio: f32,
    /// Optional deadline applied to each dispatched request attempt.
    pub task_timeout: Option<Duration>,
    /// Optional global task-start budget per minute.
    pub max_tasks_per_minute: Option<u32>,
    /// Minimum delay between reservations for the same host.
    pub same_domain_delay: Duration,
    /// Dispatcher fallback tick for reconsidering whether more work can run.
    pub maybe_run_interval: Duration,
    /// Interval between load-signal scaling decisions.
    pub autoscale_interval: Duration,
    /// Concurrency scaling strategy.
    pub mode: AutoscaleMode,
    /// Load-signal collection configuration.
    pub snapshotter: SnapshotterOptions,
    /// Load-history evaluation configuration.
    pub system_status: SystemStatusOptions,
}

impl Default for AutoscaledPoolOptions {
    fn default() -> Self {
        Self {
            fixed_concurrency: None,
            min_concurrency: 1,
            max_concurrency: 200,
            desired_concurrency: None,
            scale_up_step_ratio: 0.05,
            scale_down_step_ratio: 0.05,
            desired_utilization_ratio: 0.9,
            task_timeout: None,
            max_tasks_per_minute: None,
            same_domain_delay: Duration::ZERO,
            maybe_run_interval: Duration::from_millis(500),
            autoscale_interval: Duration::from_secs(10),
            mode: AutoscaleMode::Aimd {
                increase_after_successes: 10,
                decrease_factor: 0.5,
            },
            snapshotter: SnapshotterOptions::default(),
            system_status: SystemStatusOptions::default(),
        }
    }
}

/// Strategy used to adjust desired concurrency.
#[derive(Debug, Clone)]
pub enum AutoscaleMode {
    /// Deterministic additive-increase, multiplicative-decrease scaling.
    Aimd {
        /// Number of consecutive successes required for one additive increase.
        increase_after_successes: usize,
        /// Multiplicative factor applied after a setback.
        decrease_factor: f32,
    },
    /// Periodic scaling based on registered load-signal histories.
    LoadSignals,
}

/// Coarse attempt result consumed by AIMD scaling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptOutcomeKind {
    /// A successful attempt.
    Success,
    /// A retryable or failed attempt.
    Setback,
}

enum Mode {
    Fixed(usize),
    Aimd(AimdController),
    LoadSignals {
        desired: AtomicUsize,
        snapshotter: Snapshotter,
        system_status: SystemStatus,
    },
}

/// Concurrency decision and politeness facade consulted by a crawler dispatch loop.
pub struct AutoscaledPool {
    mode: Mode,
    min: usize,
    max: usize,
    scale_up_step_ratio: f32,
    scale_down_step_ratio: f32,
    desired_utilization_ratio: f32,
    autoscale_interval: Duration,
    task_rate_limiter: Option<TaskRateLimiter>,
    domain_limiter: DomainLimiter,
}

impl AutoscaledPool {
    /// Creates a pool, normalizing concurrency bounds and selecting its scaling mode.
    pub fn new(options: AutoscaledPoolOptions) -> Self {
        let min = options.min_concurrency.max(1);
        let max = options.max_concurrency.max(min);
        let initial = options.desired_concurrency.unwrap_or(min);
        let mode = if let Some(fixed) = options.fixed_concurrency {
            Mode::Fixed(fixed.max(1))
        } else {
            match options.mode {
                AutoscaleMode::Aimd {
                    increase_after_successes,
                    decrease_factor,
                } => Mode::Aimd(AimdController::new(
                    min,
                    max,
                    initial,
                    increase_after_successes,
                    decrease_factor,
                )),
                AutoscaleMode::LoadSignals => {
                    if options.snapshotter.signals.is_empty() {
                        tracing::warn!(
                            "AutoscaleMode::LoadSignals configured with no registered signals; falling back to AIMD defaults"
                        );
                        Mode::Aimd(AimdController::new(min, max, initial, 10, 0.5))
                    } else {
                        Mode::LoadSignals {
                            desired: AtomicUsize::new(initial.clamp(min, max)),
                            snapshotter: Snapshotter::new(options.snapshotter),
                            system_status: SystemStatus::new(options.system_status),
                        }
                    }
                }
            }
        };

        Self {
            mode,
            min,
            max,
            scale_up_step_ratio: options.scale_up_step_ratio,
            scale_down_step_ratio: options.scale_down_step_ratio,
            desired_utilization_ratio: options.desired_utilization_ratio,
            autoscale_interval: options.autoscale_interval,
            task_rate_limiter: options.max_tasks_per_minute.map(TaskRateLimiter::new),
            domain_limiter: DomainLimiter::new(options.same_domain_delay),
        }
    }

    /// Returns whether concurrency is explicitly fixed.
    pub fn is_fixed(&self) -> bool {
        matches!(self.mode, Mode::Fixed(_))
    }

    /// Returns the concurrency currently desired by the selected mode.
    pub fn desired_concurrency(&self) -> usize {
        match &self.mode {
            Mode::Fixed(value) => *value,
            Mode::Aimd(controller) => controller.desired_concurrency(),
            Mode::LoadSignals { desired, .. } => desired.load(Ordering::Acquire),
        }
    }

    /// Returns the effective minimum concurrency.
    pub fn min_concurrency(&self) -> usize {
        match self.mode {
            Mode::Fixed(value) => value,
            _ => self.min,
        }
    }

    /// Returns the effective maximum concurrency.
    pub fn max_concurrency(&self) -> usize {
        match self.mode {
            Mode::Fixed(value) => value,
            _ => self.max,
        }
    }

    /// Sets a persistent minimum delay between reservations for one host.
    pub fn set_domain_delay_floor(&self, host: &str, floor: Duration) {
        self.domain_limiter.set_delay_floor(host, floor);
    }

    /// Records an attempt result when using AIMD mode.
    pub(crate) fn record_outcome(&self, outcome: AttemptOutcomeKind) {
        if let Mode::Aimd(controller) = &self.mode {
            match outcome {
                AttemptOutcomeKind::Success => controller.record_success(),
                AttemptOutcomeKind::Setback => controller.record_setback(),
            }
        }
    }

    /// Acquires a global task token or returns the required wait.
    pub(crate) fn task_token_wait(&self, now: Instant) -> Option<Duration> {
        self.task_rate_limiter
            .as_ref()
            .and_then(|limiter| limiter.try_acquire(now).err())
    }

    /// Reserves a host slot and returns the required wait.
    pub(crate) fn domain_slot_wait(&self, host: &str, now: Instant) -> Duration {
        self.domain_limiter.reserve_slot(host, now)
    }

    /// Updates host politeness state from a response.
    pub(crate) fn note_response(
        &self,
        host: &str,
        status: Option<http::StatusCode>,
        retry_after: Option<Duration>,
        now: Instant,
    ) {
        self.domain_limiter
            .note_response(host, status, retry_after, now);
    }

    /// Spawns periodic scaling for load-signal mode only.
    pub(crate) fn spawn_background(
        self: &Arc<Self>,
        cancel: CancellationToken,
        on_scale_change: Box<dyn Fn() + Send + Sync>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !matches!(self.mode, Mode::LoadSignals { .. }) {
            return None;
        }

        let pool = Arc::clone(self);
        Some(tokio::spawn(async move {
            let Mode::LoadSignals {
                desired,
                snapshotter,
                system_status,
            } = &pool.mode
            else {
                return;
            };

            if let Err(error) = snapshotter.start().await {
                tracing::warn!(%error, "autoscale snapshotter start failed");
            }

            let mut ticker = tokio::time::interval(pool.autoscale_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let decision = system_status.evaluate(
                            snapshotter,
                            pool.desired_utilization_ratio,
                            Instant::now(),
                        );
                        let previous = desired.fetch_update(
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            |current| Some(apply_scale_decision(
                                current,
                                decision,
                                pool.min,
                                pool.max,
                                pool.scale_up_step_ratio,
                                pool.scale_down_step_ratio,
                            )),
                        );
                        if let Ok(previous) = previous {
                            if desired.load(Ordering::Acquire) != previous {
                                on_scale_change();
                            }
                        }
                    }
                }
            }

            if let Err(error) = snapshotter.stop().await {
                tracing::warn!(%error, "autoscale snapshotter stop failed");
            }
        }))
    }
}

/// Applies one proportional load-signal scaling decision within normalized bounds.
pub(crate) fn apply_scale_decision(
    current: usize,
    decision: ScaleDecision,
    min: usize,
    max: usize,
    up_ratio: f32,
    down_ratio: f32,
) -> usize {
    match decision {
        ScaleDecision::Hold => current,
        ScaleDecision::ScaleUp => current
            .saturating_add(((current as f32 * up_ratio).ceil() as usize).max(1))
            .min(max.max(min)),
        ScaleDecision::ScaleDown => current
            .saturating_sub(((current as f32 * down_ratio).ceil() as usize).max(1))
            .max(min),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoscale::{LoadSignal, LoadSnapshot};
    use proptest::prelude::*;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    struct StartTrackingSignal {
        started: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl LoadSignal for StartTrackingSignal {
        fn name(&self) -> &str {
            "start-tracking"
        }

        fn overload_threshold(&self) -> f32 {
            1.0
        }

        async fn start(&self) -> Result<(), crate::errors::CrawlError> {
            self.started.store(true, AtomicOrdering::SeqCst);
            Ok(())
        }

        fn sample(&self, _window: Duration) -> Vec<LoadSnapshot> {
            Vec::new()
        }
    }

    proptest! {
        #[test]
        fn apply_scale_decision_stays_in_bounds(
            min in 1_usize..100,
            width in 0_usize..100,
            offset in 0_usize..100,
            decision_index in 0_u8..3,
            up_ratio in 0.0001_f32..=1.0,
            down_ratio in 0.0001_f32..=1.0,
        ) {
            let max = min + width;
            let current = min + offset.min(width);
            let decision = match decision_index {
                0 => ScaleDecision::ScaleDown,
                1 => ScaleDecision::Hold,
                _ => ScaleDecision::ScaleUp,
            };
            let result = apply_scale_decision(
                current,
                decision,
                min,
                max,
                up_ratio,
                down_ratio,
            );

            prop_assert!((min..=max).contains(&result));
            match decision {
                ScaleDecision::ScaleUp => prop_assert!(result >= current),
                ScaleDecision::ScaleDown => prop_assert!(result <= current),
                ScaleDecision::Hold => prop_assert_eq!(result, current),
            }
        }

        #[test]
        fn desired_concurrency_tracks_signal_direction_and_stays_bounded(
            readings in proptest::collection::vec(any::<bool>(), 1..200),
            min in 1_usize..20,
            width in 0_usize..50,
            up in 0.01_f32..=1.0,
            down in 0.01_f32..=1.0,
        ) {
            struct FakeSignal {
                samples: Vec<LoadSnapshot>,
            }

            #[async_trait::async_trait]
            impl LoadSignal for FakeSignal {
                fn name(&self) -> &str {
                    "property"
                }

                fn overload_threshold(&self) -> f32 {
                    1.0
                }

                fn sample(&self, _window: Duration) -> Vec<LoadSnapshot> {
                    self.samples.clone()
                }
            }

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let transitions = runtime.block_on(async {
                let now = Instant::now();
                let max = min + width;
                let mut desired = min + width / 2;
                let mut transitions = Vec::with_capacity(readings.len());

                for index in 0..readings.len() {
                    let prefix = &readings[..=index];
                    let samples = prefix
                        .iter()
                        .enumerate()
                        .map(|(sample_index, overloaded)| LoadSnapshot {
                            at: now
                                - Duration::from_millis(
                                    (prefix.len() - sample_index) as u64,
                                ),
                            overloaded: *overloaded,
                        })
                        .collect();
                    let snapshotter = Snapshotter::new(SnapshotterOptions {
                        signals: vec![Arc::new(FakeSignal { samples })],
                        window: Duration::from_secs(1),
                    });
                    let decision = SystemStatus::new(SystemStatusOptions { min_samples: 1 })
                        .evaluate(&snapshotter, 0.9, now);
                    let next = apply_scale_decision(
                        desired,
                        decision,
                        min,
                        max,
                        up,
                        down,
                    );
                    transitions.push((readings[index], desired, next));
                    desired = next;
                }

                transitions
            });

            for (overloaded, desired, next) in transitions {
                prop_assert!((min..=min + width).contains(&next));
                if overloaded {
                    prop_assert!(next <= desired);
                } else {
                    prop_assert!(next >= desired);
                }
            }
        }
    }

    #[tokio::test]
    async fn fixed_mode_ignores_signals_and_never_spawns_background() {
        let started = Arc::new(AtomicBool::new(false));
        let signal = Arc::new(StartTrackingSignal {
            started: Arc::clone(&started),
        });
        let pool = Arc::new(AutoscaledPool::new(AutoscaledPoolOptions {
            fixed_concurrency: Some(4),
            mode: AutoscaleMode::LoadSignals,
            snapshotter: SnapshotterOptions {
                signals: vec![signal],
                ..SnapshotterOptions::default()
            },
            ..AutoscaledPoolOptions::default()
        }));

        assert!(pool.is_fixed());
        assert_eq!(pool.desired_concurrency(), 4);
        assert_eq!(pool.min_concurrency(), 4);
        assert_eq!(pool.max_concurrency(), 4);
        assert!(
            pool.spawn_background(CancellationToken::new(), Box::new(|| {}))
                .is_none()
        );
        assert!(!started.load(AtomicOrdering::SeqCst));
    }

    #[tokio::test]
    async fn aimd_mode_spawn_background_returns_none() {
        let pool = Arc::new(AutoscaledPool::new(AutoscaledPoolOptions::default()));
        assert!(
            pool.spawn_background(CancellationToken::new(), Box::new(|| {}))
                .is_none()
        );
    }

    #[test]
    fn autoscale_interval_is_copied_verbatim() {
        let pool = AutoscaledPool::new(AutoscaledPoolOptions {
            autoscale_interval: Duration::ZERO,
            ..AutoscaledPoolOptions::default()
        });

        assert_eq!(pool.autoscale_interval, Duration::ZERO);
    }
}
