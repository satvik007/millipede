//! Deterministic integration coverage for the Phase 4 dynamic scheduler.

use http::StatusCode;
use millipede_core::{
    autoscale::{
        AutoscaleMode, AutoscaledPoolOptions, LoadSignal, LoadSnapshot, SnapshotterOptions,
        SystemStatusOptions,
    },
    crawler::{BasicContext, BasicKind, Crawler},
    errors::CrawlError,
    handler::RequestHandler,
    http_client::HttpStatusError,
    statistics::FinalStatistics,
};
use millipede_storage_memory::MemoryStorageClient;
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Barrier,
    task::JoinHandle,
    time::{Instant, advance, pause, resume, timeout},
};

const RUN_TIMEOUT: Duration = Duration::from_secs(600);

struct FlagSignal {
    flag: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl LoadSignal for FlagSignal {
    fn name(&self) -> &str {
        "flag"
    }

    fn overload_threshold(&self) -> f32 {
        1.0
    }

    fn sample(&self, _window: Duration) -> Vec<LoadSnapshot> {
        vec![LoadSnapshot {
            at: Instant::now(),
            overloaded: self.flag.load(Ordering::SeqCst),
        }]
    }
}

struct StartStopTrackingSignal {
    started: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl LoadSignal for StartStopTrackingSignal {
    fn name(&self) -> &str {
        "start-stop-tracking"
    }

    fn overload_threshold(&self) -> f32 {
        1.0
    }

    async fn start(&self) -> Result<(), CrawlError> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop(&self) -> Result<(), CrawlError> {
        self.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn sample(&self, _window: Duration) -> Vec<LoadSnapshot> {
        Vec::new()
    }
}

struct ActiveGuard(Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn note_active(active: &Arc<AtomicUsize>, peak: &Arc<AtomicUsize>) -> ActiveGuard {
    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
    peak.fetch_max(current, Ordering::SeqCst);
    ActiveGuard(active.clone())
}

fn urls(hosts: &[&str], count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("https://{}/{index}", hosts[index % hosts.len()]))
        .collect()
}

async fn crawler_over<H>(
    hosts: &[&str],
    count: usize,
    handler: H,
    options: AutoscaledPoolOptions,
) -> (Crawler<BasicKind>, Vec<String>)
where
    H: RequestHandler<BasicContext>,
{
    let crawler = Crawler::builder(BasicKind)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_handler(handler)
        .autoscaled_pool_options(options)
        .build()
        .await
        .expect("crawler should build");
    (crawler, urls(hosts, count))
}

fn load_options(
    signal: Arc<dyn LoadSignal>,
    min: usize,
    max: usize,
    desired: usize,
    interval: Duration,
) -> AutoscaledPoolOptions {
    AutoscaledPoolOptions {
        fixed_concurrency: None,
        min_concurrency: min,
        max_concurrency: max,
        desired_concurrency: Some(desired),
        scale_up_step_ratio: 1.0,
        scale_down_step_ratio: 1.0,
        desired_utilization_ratio: 0.5,
        autoscale_interval: interval,
        mode: AutoscaleMode::LoadSignals,
        snapshotter: SnapshotterOptions {
            signals: vec![signal],
            window: Duration::from_secs(1),
        },
        system_status: SystemStatusOptions { min_samples: 1 },
        ..AutoscaledPoolOptions::default()
    }
}

async fn finish_run(task: JoinHandle<Result<FinalStatistics, CrawlError>>) -> FinalStatistics {
    timeout(RUN_TIMEOUT, task)
        .await
        .expect("crawler run exceeded 600 seconds of virtual time")
        .expect("crawler run task panicked")
        .expect("crawler run failed")
}

async fn yield_a_few_times() {
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
}

async fn pause_resume_tick() {
    advance(Duration::from_millis(7)).await;
    yield_a_few_times().await;
    resume();
    tokio::task::yield_now().await;
    pause();
}

async fn drive_until_finished(task: &JoinHandle<Result<FinalStatistics, CrawlError>>) {
    for _ in 0..5_000 {
        if task.is_finished() {
            return;
        }
        pause_resume_tick().await;
    }
    panic!("crawler did not finish while its virtual clock was driven");
}

async fn assert_no_live_guards(guards: &Arc<Mutex<Vec<Weak<()>>>>) {
    for _ in 0..10 {
        if guards
            .lock()
            .expect("guard list mutex poisoned")
            .iter()
            .all(|guard| guard.upgrade().is_none())
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        guards
            .lock()
            .expect("guard list mutex poisoned")
            .iter()
            .all(|guard| guard.upgrade().is_none()),
        "at least one handler task retained its guard"
    );
}

#[tokio::test(start_paused = true)]
async fn dynamic_concurrency_tracks_fake_signals_up_and_down() {
    let overloaded = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let phase_peak = Arc::new(AtomicUsize::new(0));
    let starts = Arc::new(AtomicUsize::new(0));
    let handler_active = active.clone();
    let handler_peak = peak.clone();
    let handler_phase_peak = phase_peak.clone();
    let handler_starts = starts.clone();
    let (crawler, requests) = crawler_over(
        &["a.invalid"],
        200,
        move |_ctx: BasicContext| {
            let active = handler_active.clone();
            let peak = handler_peak.clone();
            let phase_peak = handler_phase_peak.clone();
            let starts = handler_starts.clone();
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                let active = note_active(&active, &peak);
                phase_peak.fetch_max(active.0.load(Ordering::SeqCst), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            }
        },
        load_options(
            Arc::new(FlagSignal {
                flag: overloaded.clone(),
            }),
            1,
            6,
            2,
            Duration::from_millis(20),
        ),
    )
    .await;
    let crawler = Arc::new(crawler);
    let handle = crawler.handle();
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(requests).await });

    let mut healthy_target = 2;
    for _ in 0..20 {
        advance(Duration::from_millis(20)).await;
        yield_a_few_times().await;
        healthy_target = healthy_target.max(
            handle
                .autoscaler_snapshot()
                .expect("crawler should still be alive")
                .desired_concurrency,
        );
        if healthy_target == 6 && peak.load(Ordering::SeqCst) >= healthy_target {
            break;
        }
    }
    let healthy_observed = peak.load(Ordering::SeqCst);
    assert!(healthy_target > 2, "healthy signals did not scale above 2");
    assert!(
        healthy_observed >= healthy_target,
        "dispatcher only reached {healthy_observed} concurrent handlers for healthy target {healthy_target}"
    );
    assert!(
        healthy_observed <= healthy_target,
        "dispatcher exceeded healthy target {healthy_target} with {healthy_observed} concurrent handlers"
    );

    overloaded.store(true, Ordering::SeqCst);
    let mut reduced = healthy_target;
    for _ in 0..6 {
        advance(Duration::from_millis(20)).await;
        yield_a_few_times().await;
        reduced = handle
            .autoscaler_snapshot()
            .expect("crawler should still be alive")
            .desired_concurrency;
        if reduced < healthy_target {
            break;
        }
    }
    assert!(
        reduced < healthy_target,
        "overload did not reduce concurrency"
    );
    for _ in 0..20 {
        if active.load(Ordering::SeqCst) <= reduced {
            break;
        }
        advance(Duration::from_millis(10)).await;
        yield_a_few_times().await;
    }
    assert!(
        active.load(Ordering::SeqCst) <= reduced,
        "handler concurrency did not drain to reduced target {reduced}"
    );
    phase_peak.store(active.load(Ordering::SeqCst), Ordering::SeqCst);
    let starts_after_drain = starts.load(Ordering::SeqCst);
    for _ in 0..20 {
        if starts.load(Ordering::SeqCst) >= starts_after_drain + 2 {
            break;
        }
        advance(Duration::from_millis(10)).await;
        yield_a_few_times().await;
    }
    assert!(
        starts.load(Ordering::SeqCst) >= starts_after_drain + 2,
        "dispatcher did not keep making progress at reduced target {reduced}"
    );
    assert!(
        phase_peak.load(Ordering::SeqCst) <= reduced,
        "dispatcher exceeded reduced target {reduced} after existing work drained"
    );

    overloaded.store(false, Ordering::SeqCst);
    phase_peak.store(active.load(Ordering::SeqCst), Ordering::SeqCst);
    let mut recovered = reduced;
    for _ in 0..20 {
        advance(Duration::from_millis(20)).await;
        yield_a_few_times().await;
        recovered = handle
            .autoscaler_snapshot()
            .expect("crawler should still be alive")
            .desired_concurrency;
        if recovered > reduced && phase_peak.load(Ordering::SeqCst) >= recovered {
            break;
        }
    }
    assert!(
        recovered > reduced,
        "healthy signals did not scale up again"
    );
    assert!(
        phase_peak.load(Ordering::SeqCst) >= recovered,
        "dispatcher did not rise to recovered target {recovered}"
    );

    let stats = finish_run(run).await;
    assert_eq!(stats.requests_finished, 200);
    assert!(peak.load(Ordering::SeqCst) <= 6);
}

#[tokio::test(start_paused = true)]
async fn fixed_concurrency_never_starts_signal_loops() {
    let started = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(3));
    let calls = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler_barrier = barrier.clone();
    let handler_calls = calls.clone();
    let handler_active = active.clone();
    let handler_peak = peak.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |_ctx: BasicContext| {
                let barrier = handler_barrier.clone();
                let calls = handler_calls.clone();
                let active = handler_active.clone();
                let peak = handler_peak.clone();
                async move {
                    let _active = note_active(&active, &peak);
                    if calls.fetch_add(1, Ordering::SeqCst) < 3 {
                        barrier.wait().await;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(())
                }
            })
            .autoscaled_pool_options(AutoscaledPoolOptions {
                fixed_concurrency: Some(3),
                min_concurrency: 1,
                max_concurrency: 3,
                desired_concurrency: Some(3),
                mode: AutoscaleMode::LoadSignals,
                snapshotter: SnapshotterOptions {
                    signals: vec![Arc::new(StartStopTrackingSignal {
                        started: started.clone(),
                        stopped: stopped.clone(),
                    })],
                    ..SnapshotterOptions::default()
                },
                ..AutoscaledPoolOptions::default()
            })
            .build()
            .await
            .expect("crawler should build"),
    );
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(urls(&["fixed.invalid"], 30)).await });
    let stats = finish_run(run).await;
    let snapshot = crawler.autoscaler_snapshot();

    assert_eq!(stats.requests_finished, 30);
    assert!(!started.load(Ordering::SeqCst));
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(snapshot.is_fixed);
    assert_eq!(snapshot.desired_concurrency, 3);
    assert_eq!(peak.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn clock_pause_resume_across_ticks_leaks_nothing() {
    // Phase A: repeatedly pause and resume across autoscale ticks, then finish normally.
    let phase_a_guards = Arc::new(Mutex::new(Vec::new()));
    let handler_guards = phase_a_guards.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |_ctx: BasicContext| {
                let guards = handler_guards.clone();
                async move {
                    let guard = Arc::new(());
                    guards
                        .lock()
                        .expect("guard list mutex poisoned")
                        .push(Arc::downgrade(&guard));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            })
            .autoscaled_pool_options(load_options(
                Arc::new(FlagSignal {
                    flag: Arc::new(AtomicBool::new(false)),
                }),
                1,
                6,
                2,
                Duration::from_millis(5),
            ))
            .build()
            .await
            .expect("crawler should build"),
    );
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(urls(&["phase-a.invalid"], 100)).await });
    drive_until_finished(&run).await;
    let stats = finish_run(run).await;
    assert_eq!(stats.requests_finished, 100);
    assert_eq!(
        phase_a_guards
            .lock()
            .expect("guard list mutex poisoned")
            .len(),
        100
    );
    assert_no_live_guards(&phase_a_guards).await;

    // Phase B: graceful stop drains active tasks but preserves unstarted queue work.
    let phase_b_guards = Arc::new(Mutex::new(Vec::new()));
    let phase_b_started = Arc::new(AtomicUsize::new(0));
    let phase_b_completed = Arc::new(AtomicUsize::new(0));
    let handler_guards = phase_b_guards.clone();
    let handler_started = phase_b_started.clone();
    let handler_completed = phase_b_completed.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |_ctx: BasicContext| {
                let guards = handler_guards.clone();
                let started = handler_started.clone();
                let completed = handler_completed.clone();
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let guard = Arc::new(());
                    guards
                        .lock()
                        .expect("guard list mutex poisoned")
                        .push(Arc::downgrade(&guard));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .autoscaled_pool_options(load_options(
                Arc::new(FlagSignal {
                    flag: Arc::new(AtomicBool::new(false)),
                }),
                1,
                6,
                2,
                Duration::from_millis(5),
            ))
            .build()
            .await
            .expect("crawler should build"),
    );
    let handle = crawler.handle();
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(urls(&["phase-b.invalid"], 100)).await });
    for _ in 0..2_000 {
        if handle
            .stats()
            .expect("crawler should still be alive")
            .requests_finished
            >= 10
            && phase_b_started.load(Ordering::SeqCst) > phase_b_completed.load(Ordering::SeqCst)
        {
            break;
        }
        pause_resume_tick().await;
    }
    assert!(
        handle
            .stats()
            .expect("crawler should still be alive")
            .requests_finished
            >= 10,
        "phase B did not reach the stop threshold"
    );
    let started_at_stop = phase_b_started.load(Ordering::SeqCst);
    let completed_at_stop = phase_b_completed.load(Ordering::SeqCst);
    let in_flight_at_stop = started_at_stop - completed_at_stop;
    assert!(
        in_flight_at_stop > 0,
        "phase B must stop while handlers are in flight"
    );
    handle.stop();
    drive_until_finished(&run).await;
    let stats = finish_run(run).await;
    assert!(stats.requests_finished < 100);
    assert!(stats.requests_finished + stats.requests_failed < 100);
    let final_started = phase_b_started.load(Ordering::SeqCst);
    let final_completed = phase_b_completed.load(Ordering::SeqCst);
    assert_eq!(
        final_completed, final_started,
        "graceful stop must let every started handler complete normally"
    );
    assert!(
        final_completed - completed_at_stop >= in_flight_at_stop,
        "handlers in flight when stop was called did not all complete normally"
    );
    assert_no_live_guards(&phase_b_guards).await;

    // Phase C: an independently spawned monitor aborts after the first completion.
    let phase_c_guards = Arc::new(Mutex::new(Vec::new()));
    let handler_guards = phase_c_guards.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |_ctx: BasicContext| {
                let guards = handler_guards.clone();
                async move {
                    let guard = Arc::new(());
                    guards
                        .lock()
                        .expect("guard list mutex poisoned")
                        .push(Arc::downgrade(&guard));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            })
            .autoscaled_pool_options(load_options(
                Arc::new(FlagSignal {
                    flag: Arc::new(AtomicBool::new(false)),
                }),
                1,
                6,
                2,
                Duration::from_millis(5),
            ))
            .build()
            .await
            .expect("crawler should build"),
    );
    let handle = crawler.handle();
    let abort_handle = handle.clone();
    let aborter = tokio::spawn(async move {
        loop {
            if abort_handle
                .stats()
                .map(|stats| stats.requests_finished >= 1)
                .unwrap_or(true)
            {
                abort_handle.abort();
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(urls(&["phase-c.invalid"], 100)).await });
    drive_until_finished(&run).await;
    let stats = finish_run(run).await;
    timeout(RUN_TIMEOUT, aborter)
        .await
        .expect("abort monitor exceeded 600 seconds of virtual time")
        .expect("abort monitor should not panic");
    assert!(stats.requests_finished >= 1);
    assert!(stats.requests_finished + stats.requests_failed < 100);
    assert_no_live_guards(&phase_c_guards).await;
}

#[tokio::test(start_paused = true)]
async fn hammered_host_never_starves_healthy_hosts() {
    let calm_completions = Arc::new(Mutex::new(Vec::<(String, Instant)>::new()));
    let handler_completions = calm_completions.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |ctx: BasicContext| {
                let completions = handler_completions.clone();
                async move {
                    let host = ctx
                        .request
                        .url
                        .host_str()
                        .expect("test URL should have a host");
                    if host == "storm.invalid" {
                        Err(CrawlError::retry(HttpStatusError::new(
                            StatusCode::TOO_MANY_REQUESTS,
                        )))
                    } else {
                        completions
                            .lock()
                            .expect("completion list mutex poisoned")
                            .push((host.to_owned(), Instant::now()));
                        Ok(())
                    }
                }
            })
            .failed_request_handler(|_ctx| async { Ok(()) })
            .max_concurrency(4)
            .same_domain_delay(Duration::from_millis(10))
            .max_request_retries(8)
            .build()
            .await
            .expect("crawler should build"),
    );
    let requests = urls(&["storm.invalid", "calm-a.invalid", "calm-b.invalid"], 3);
    let started_at = Instant::now();
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(requests).await });
    let stats = timeout(Duration::from_secs(600), run)
        .await
        .expect("crawler run exceeded 600 seconds of virtual time")
        .expect("crawler run task panicked")
        .expect("crawler run failed");
    let elapsed = started_at.elapsed();

    assert_eq!(stats.requests_finished, 2);
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(stats.requests_retries, 8);
    let completions = calm_completions
        .lock()
        .expect("completion list mutex poisoned");
    assert_eq!(completions.len(), 2);
    for host in ["calm-a.invalid", "calm-b.invalid"] {
        let last = completions
            .iter()
            .filter(|(seen_host, _)| seen_host == host)
            .map(|(_, at)| *at)
            .max()
            .expect("each calm host should complete requests");
        assert!(
            last.duration_since(started_at) <= Duration::from_secs(2),
            "{host} was starved behind the storm host"
        );
    }
    assert!(
        elapsed > Duration::from_secs(2),
        "the storm penalties should dominate total runtime"
    );
}

#[tokio::test(start_paused = true)]
async fn task_budget_spreads_across_domains() {
    let starts = Arc::new(Mutex::new(Vec::<(Instant, String)>::new()));
    let handler_starts = starts.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |ctx: BasicContext| {
                let starts = handler_starts.clone();
                async move {
                    starts.lock().expect("start list mutex poisoned").push((
                        Instant::now(),
                        ctx.request
                            .url
                            .host_str()
                            .expect("test URL should have a host")
                            .to_owned(),
                    ));
                    Ok(())
                }
            })
            .max_concurrency(2)
            .max_tasks_per_minute(4)
            .build()
            .await
            .expect("crawler should build"),
    );
    let started_at = Instant::now();
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move {
        run_crawler
            .run(urls(&["budget-a.invalid", "budget-b.invalid"], 6))
            .await
    });
    let stats = finish_run(run).await;
    assert_eq!(stats.requests_finished, 6);

    let starts = starts.lock().expect("start list mutex poisoned");
    assert_eq!(starts.len(), 6);
    for (index, (actual, _)) in starts.iter().enumerate() {
        let expected = Duration::from_secs(index as u64 * 15);
        let actual = actual.duration_since(started_at);
        let delta = actual.abs_diff(expected);
        assert!(
            delta <= Duration::from_secs(1),
            "start {index} occurred at {actual:?}, expected {expected:?}"
        );
    }
    let actual_hosts: Vec<_> = starts.iter().map(|(_, host)| host.as_str()).collect();
    assert_eq!(
        actual_hosts,
        [
            "budget-a.invalid",
            "budget-b.invalid",
            "budget-a.invalid",
            "budget-b.invalid",
            "budget-a.invalid",
            "budget-b.invalid",
        ],
        "FIFO fairness should alternate hosts across the complete request sequence"
    );
}

#[tokio::test(start_paused = true)]
async fn aimd_desired_stays_in_bounds_under_chaotic_outcomes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = calls.clone();
    let handler_samples = samples.clone();
    let crawler = Arc::new(
        Crawler::builder(BasicKind)
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .request_handler(move |ctx: BasicContext| {
                let calls = handler_calls.clone();
                let samples = handler_samples.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    let desired = ctx
                        .crawler
                        .autoscaler_snapshot()
                        .expect("crawler should be alive in its handler")
                        .desired_concurrency;
                    samples
                        .lock()
                        .expect("sample list mutex poisoned")
                        .push(desired);
                    if call % 3 == 2 {
                        Err(CrawlError::retry(anyhow::anyhow!("chaos")))
                    } else {
                        Ok(())
                    }
                }
            })
            .max_concurrency(6)
            .autoscale_mode(AutoscaleMode::Aimd {
                increase_after_successes: 1,
                decrease_factor: 0.5,
            })
            .min_concurrency(2)
            .desired_concurrency(2)
            .max_request_retries(10)
            .build()
            .await
            .expect("crawler should build"),
    );
    let run_crawler = crawler.clone();
    let run = tokio::spawn(async move { run_crawler.run(urls(&["chaos.invalid"], 150)).await });
    let stats = finish_run(run).await;
    assert_eq!(stats.requests_finished, 150);
    let final_desired = crawler.autoscaler_snapshot().desired_concurrency;

    let samples = samples.lock().expect("sample list mutex poisoned");
    assert!(samples.iter().all(|desired| (2..=6).contains(desired)));
    assert!(
        (2..=6).contains(&final_desired),
        "post-completion desired concurrency {final_desired} escaped configured bounds"
    );
    assert!(samples.iter().any(|desired| *desired > 2));
    assert!(samples.windows(2).any(|pair| pair[1] < pair[0]));
}
