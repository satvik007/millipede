//! Public autoscaling API integration tests.

use millipede_core::autoscale::{
    AimdController, ClientLoadSignal, CpuLoadSignal, CpuLoadSignalOptions, LoadSignal,
    LoadSnapshot, MemoryLoadSignal, MemoryLoadSignalOptions, ScaleDecision, Snapshotter,
    SnapshotterOptions, SystemStatus, SystemStatusOptions, TokioRuntimeLoadSignal,
    TokioRuntimeLoadSignalOptions,
};
use proptest::prelude::*;
use std::{
    collections::HashSet,
    sync::{Arc, Barrier},
    time::Duration,
};
use tokio::time::Instant;

#[test]
fn aimd_increments_after_sustained_successes() {
    let controller = AimdController::new(1, 100, 1, 3, 0.5);
    controller.record_success();
    controller.record_success();
    assert_eq!(controller.desired_concurrency(), 1);
    controller.record_success();
    assert_eq!(controller.desired_concurrency(), 2);
}

#[test]
fn aimd_setback_multiplicatively_decreases_floored_at_min() {
    let controller = AimdController::new(1, 100, 8, 10, 0.5);
    controller.record_setback();
    assert_eq!(controller.desired_concurrency(), 4);
    controller.record_setback();
    assert_eq!(controller.desired_concurrency(), 2);
    controller.record_setback();
    assert_eq!(controller.desired_concurrency(), 1);
    controller.record_setback();
    assert_eq!(controller.desired_concurrency(), 1);
}

#[test]
fn aimd_never_exceeds_max_concurrency() {
    let controller = AimdController::new(1, 3, 1, 2, 0.5);
    for _ in 0..100 {
        controller.record_success();
        assert!(controller.desired_concurrency() <= 3);
    }
}

#[test]
fn aimd_setback_clamps_f32_rounding_to_large_max_concurrency() {
    const MAX: usize = 16_777_219;
    let controller = AimdController::new(1, MAX, MAX, 10, 1.0);
    controller.record_setback();
    assert_eq!(controller.desired_concurrency(), MAX);
}

#[test]
fn aimd_concurrent_successes_cross_each_threshold_once() {
    const SUCCESS_COUNT: usize = 65;
    const THRESHOLD: usize = 2;
    let controller = Arc::new(AimdController::new(1, 100, 1, THRESHOLD, 0.5));
    let barrier = Arc::new(Barrier::new(SUCCESS_COUNT));
    let mut threads = Vec::with_capacity(SUCCESS_COUNT);

    for _ in 0..SUCCESS_COUNT {
        let controller = Arc::clone(&controller);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            controller.record_success();
        }));
    }
    for thread in threads {
        thread.join().expect("success recorder thread panicked");
    }

    assert_eq!(
        controller.desired_concurrency(),
        1 + SUCCESS_COUNT / THRESHOLD
    );
    controller.record_success();
    assert_eq!(
        controller.desired_concurrency(),
        1 + (SUCCESS_COUNT + 1) / THRESHOLD
    );
}

#[test]
fn aimd_concurrent_successes_and_setbacks_match_a_serial_execution() {
    const SUCCESSES: usize = 4;
    const SETBACKS: usize = 2;
    let mut serial_results = HashSet::new();
    collect_serial_results(
        SUCCESSES,
        SETBACKS,
        &mut Vec::with_capacity(SUCCESSES + SETBACKS),
        &mut serial_results,
    );

    for _ in 0..64 {
        let controller = Arc::new(AimdController::new(1, 100, 16, 3, 0.5));
        let barrier = Arc::new(Barrier::new(SUCCESSES + SETBACKS));
        let mut threads = Vec::with_capacity(SUCCESSES + SETBACKS);
        for success in [true; SUCCESSES].into_iter().chain([false; SETBACKS]) {
            let controller = Arc::clone(&controller);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                if success {
                    controller.record_success();
                } else {
                    controller.record_setback();
                }
            }));
        }
        for thread in threads {
            thread.join().expect("AIMD recorder thread panicked");
        }

        assert!(serial_results.contains(&controller.desired_concurrency()));
    }
}

fn collect_serial_results(
    successes: usize,
    setbacks: usize,
    outcomes: &mut Vec<bool>,
    results: &mut HashSet<usize>,
) {
    if successes == 0 && setbacks == 0 {
        let controller = AimdController::new(1, 100, 16, 3, 0.5);
        for success in outcomes {
            if *success {
                controller.record_success();
            } else {
                controller.record_setback();
            }
        }
        results.insert(controller.desired_concurrency());
        return;
    }
    if successes > 0 {
        outcomes.push(true);
        collect_serial_results(successes - 1, setbacks, outcomes, results);
        outcomes.pop();
    }
    if setbacks > 0 {
        outcomes.push(false);
        collect_serial_results(successes, setbacks - 1, outcomes, results);
        outcomes.pop();
    }
}

proptest! {
    #[test]
    fn aimd_stays_in_bounds_and_is_monotonic(
        outcomes in proptest::collection::vec(any::<bool>(), 0..500),
        min in 1_usize..50,
        width in 0_usize..100,
        threshold in 1_usize..20,
        factor in 0.01_f32..=1.0,
    ) {
        let max = min + width;
        let controller = AimdController::new(min, max, min + width / 2, threshold, factor);
        for success in outcomes {
            let previous = controller.desired_concurrency();
            if success {
                controller.record_success();
            } else {
                controller.record_setback();
            }
            let desired = controller.desired_concurrency();
            prop_assert!((min..=max).contains(&desired));
            if success {
                prop_assert!(desired >= previous);
            } else {
                prop_assert!(desired <= previous);
            }
        }
    }
}

struct FakeSignal {
    name: &'static str,
    samples: Vec<LoadSnapshot>,
}

#[async_trait::async_trait]
impl LoadSignal for FakeSignal {
    fn name(&self) -> &str {
        self.name
    }

    fn overload_threshold(&self) -> f32 {
        0.9
    }

    fn sample(&self, _window: Duration) -> Vec<LoadSnapshot> {
        self.samples.clone()
    }
}

fn signal(name: &'static str, now: Instant, overloaded: &[bool]) -> Arc<dyn LoadSignal> {
    let sample_count = overloaded.len();
    Arc::new(FakeSignal {
        name,
        samples: overloaded
            .iter()
            .enumerate()
            .map(|(index, overloaded)| LoadSnapshot {
                at: now - Duration::from_millis((sample_count - index) as u64),
                overloaded: *overloaded,
            })
            .collect(),
    })
}

fn snapshotter(signals: Vec<Arc<dyn LoadSignal>>) -> Snapshotter {
    Snapshotter::new(SnapshotterOptions {
        signals,
        window: Duration::from_secs(30),
    })
}

#[tokio::test(start_paused = true)]
async fn system_status_scales_down_on_any_current_overload() {
    let now = Instant::now();
    let snapshotter = snapshotter(vec![
        signal("healthy", now, &[false, false, false]),
        signal("overloaded", now, &[false, false, true]),
    ]);
    let status = SystemStatus::new(SystemStatusOptions { min_samples: 3 });
    assert_eq!(
        status.evaluate(&snapshotter, 0.9, now),
        ScaleDecision::ScaleDown
    );
}

#[tokio::test(start_paused = true)]
async fn system_status_uses_latest_snapshot_even_when_timestamp_is_after_now() {
    let now = Instant::now();
    let snapshotter = snapshotter(vec![Arc::new(FakeSignal {
        name: "future-overload",
        samples: vec![
            LoadSnapshot {
                at: now - Duration::from_millis(1),
                overloaded: false,
            },
            LoadSnapshot {
                at: now + Duration::from_millis(1),
                overloaded: true,
            },
        ],
    })]);
    let status = SystemStatus::new(SystemStatusOptions { min_samples: 2 });

    assert_eq!(
        status.evaluate(&snapshotter, 0.9, now),
        ScaleDecision::ScaleDown
    );
}

#[tokio::test(start_paused = true)]
async fn system_status_scales_up_only_when_sustained_healthy_ratio_met() {
    let now = Instant::now();
    let status = SystemStatus::new(SystemStatusOptions { min_samples: 4 });
    let healthy = snapshotter(vec![signal(
        "healthy-enough",
        now,
        &[false, true, false, false],
    )]);
    assert_eq!(status.evaluate(&healthy, 0.75, now), ScaleDecision::ScaleUp);

    let mixed = snapshotter(vec![signal(
        "not-healthy-enough",
        now,
        &[true, true, false, false],
    )]);
    assert_eq!(status.evaluate(&mixed, 0.75, now), ScaleDecision::Hold);
}

#[tokio::test(start_paused = true)]
async fn system_status_holds_below_min_samples() {
    let now = Instant::now();
    let snapshotter = snapshotter(vec![signal("cold", now, &[false, false])]);
    let status = SystemStatus::new(SystemStatusOptions { min_samples: 3 });
    assert_eq!(status.evaluate(&snapshotter, 0.5, now), ScaleDecision::Hold);
}

#[tokio::test(start_paused = true)]
async fn system_status_ordering_is_monotonic_with_signal_direction() {
    let now = Instant::now();
    let status = SystemStatus::new(SystemStatusOptions { min_samples: 4 });
    let overloaded = snapshotter(vec![signal("overloaded", now, &[true, true, true, true])]);
    let mixed = snapshotter(vec![signal("mixed", now, &[true, true, false, false])]);
    let healthy = snapshotter(vec![signal("healthy", now, &[false, false, false, false])]);

    let down = status.evaluate(&overloaded, 0.9, now);
    let hold = status.evaluate(&mixed, 0.9, now);
    let up = status.evaluate(&healthy, 0.9, now);
    assert_eq!(down, ScaleDecision::ScaleDown);
    assert_eq!(hold, ScaleDecision::Hold);
    assert_eq!(up, ScaleDecision::ScaleUp);

    let current = 10_usize;
    let result_for = |decision| {
        if decision == ScaleDecision::ScaleDown {
            current - 1
        } else if decision == ScaleDecision::ScaleUp {
            current + 1
        } else {
            current
        }
    };
    assert!(result_for(down) <= result_for(hold));
    assert!(result_for(hold) <= result_for(up));
}

#[tokio::test(start_paused = true)]
async fn client_signal_drives_scale_down_then_recovers() {
    let signal = Arc::new(ClientLoadSignal::new());
    let handle = signal.handle();
    let snapshotter = Snapshotter::new(SnapshotterOptions {
        signals: vec![signal],
        window: Duration::from_secs(5),
    });
    let status = SystemStatus::new(SystemStatusOptions { min_samples: 1 });

    handle.record_rate_limited();
    assert_eq!(
        status.evaluate(&snapshotter, 0.9, Instant::now()),
        ScaleDecision::ScaleDown
    );

    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(
        status.evaluate(&snapshotter, 0.9, Instant::now()),
        ScaleDecision::Hold
    );
}

#[tokio::test(start_paused = true)]
async fn cpu_and_memory_signals_lifecycle() {
    let cpu = CpuLoadSignal::new(CpuLoadSignalOptions {
        sample_interval: Duration::ZERO,
        ..CpuLoadSignalOptions::default()
    });
    let memory = MemoryLoadSignal::new(MemoryLoadSignalOptions {
        sample_interval: Duration::ZERO,
        ..MemoryLoadSignalOptions::default()
    });

    cpu.start().await.unwrap();
    memory.start().await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    assert!(!cpu.sample(Duration::from_secs(60)).is_empty());
    assert!(!memory.sample(Duration::from_secs(60)).is_empty());
    tokio::time::timeout(Duration::from_secs(5), cpu.stop())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), memory.stop())
        .await
        .unwrap()
        .unwrap();
    cpu.stop().await.unwrap();
    memory.stop().await.unwrap();

    // Cancellation tokens are one-shot, so starting after stop is an intentional no-op.
    cpu.start().await.unwrap();
    memory.start().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn tokio_runtime_signal_healthy_under_paused_clock() {
    let signal = TokioRuntimeLoadSignal::new(TokioRuntimeLoadSignalOptions {
        max_lag: Duration::from_millis(50),
        sample_interval: Duration::ZERO,
    });
    assert!(signal.overload_threshold().is_finite());
    signal.start().await.unwrap();
    tokio::task::yield_now().await;
    for _ in 0..4 {
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
    }

    let samples = signal.sample(Duration::from_secs(60));
    assert!(!samples.is_empty());
    assert!(samples.iter().all(|sample| !sample.overloaded));
    signal.stop().await.unwrap();
}
