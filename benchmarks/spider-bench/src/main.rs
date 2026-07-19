//! spider-bench: millipede vs spider head-to-head harness (PLAN.md).
//!
//! Subcommands:
//! - `orchestrate` — pre-render the site, start the instrumented axum server
//!   in-process, and run interleaved fresh-child trials per engine.
//! - `run` — child mode: one engine, one trial, ready/go handshake, one JSON
//!   result line.
//! - `report` — `samples.jsonl` -> `summary.md`.

mod engines;
mod measure;
mod report;
mod scenario;
mod scenarios;
mod server;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::{Parser, Subcommand};

use engines::Engine;

#[derive(Parser)]
#[command(name = "spider-bench", version, about = "millipede vs spider benchmark harness")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the full measurement protocol for one scenario or all of them.
    Orchestrate {
        /// Scenario name, or `all`.
        #[arg(long, default_value = "all")]
        scenario: String,
        /// Measured trials per engine (>= 5 required to publish a row).
        #[arg(long, default_value_t = 5)]
        iters: usize,
        /// Fixed fetch concurrency C for every engine.
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        /// Tokio worker threads in every child, identical across engines.
        #[arg(long, default_value_t = 4)]
        runtime_workers: usize,
        /// Scenario-specific depth override (depth-scalable scenarios only).
        #[arg(long)]
        depth: Option<u32>,
        /// One trial per engine, validation only; never publishable.
        #[arg(long)]
        quick: bool,
        /// Also run clearly-labelled sensitivity rows (not implemented in the
        /// scaffold; reserved for M-http-raw per PLAN.md §7).
        #[arg(long)]
        sensitivity: bool,
        /// Output directory for <timestamp>-{samples.jsonl,metadata.json,summary.md}.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Child mode: one engine trial with the ready/go handshake.
    Run {
        #[arg(long)]
        scenario: String,
        #[arg(long, value_enum)]
        engine: Engine,
        /// Root URL to crawl (http://127.0.0.1:PORT/<nonce>/...).
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        #[arg(long, default_value_t = 4)]
        runtime_workers: usize,
        /// Run nonce; must match the URL's nonce path segment.
        #[arg(long)]
        nonce: String,
        /// Scenario-specific depth override (must match the orchestrator's).
        #[arg(long)]
        depth: Option<u32>,
        /// Emit the result as one JSON line (always on; flag kept for the
        /// PLAN.md §6 command shape).
        #[arg(long)]
        json: bool,
    },
    /// Summarize a samples.jsonl file into summary.md.
    Report {
        /// Path to samples.jsonl.
        samples: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Cmd::Orchestrate {
            scenario,
            iters,
            concurrency,
            runtime_workers,
            depth,
            quick,
            sensitivity,
            out,
        } => cmd_orchestrate(OrchestrateArgs {
            scenario,
            iters,
            concurrency,
            runtime_workers,
            depth,
            quick,
            sensitivity,
            out,
        }),
        Cmd::Run {
            scenario,
            engine,
            url,
            concurrency,
            runtime_workers,
            nonce,
            depth,
            json: _,
        } => cmd_run(&scenario, engine, &url, concurrency, runtime_workers, &nonce, depth),
        Cmd::Report { samples } => cmd_report(&samples),
    }
}

// ---------------------------------------------------------------------------
// child mode
// ---------------------------------------------------------------------------

fn cmd_run(
    scenario_name: &str,
    engine: Engine,
    url: &str,
    concurrency: usize,
    runtime_workers: usize,
    nonce: &str,
    depth: Option<u32>,
) -> anyhow::Result<()> {
    // Parse args + build the scenario spec BEFORE `ready` (unmeasured setup).
    let spec = scenarios::build(scenario_name, nonce, depth)?;
    // The runtime is harness plumbing, identical for every engine; it exists
    // before `ready`. No HTTP client/crawler/Website exists yet (review A-2).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_workers)
        .enable_all()
        .build()?;

    measure::signal_ready_and_await_go()?;
    let start = Instant::now();
    // Engine construction + crawl + full drain, all inside the timed region.
    let outcome = runtime.block_on(engines::run(engine, &spec, concurrency, url));
    let wall = start.elapsed();

    // The CHILD reads RUSAGE_SELF itself, right before printing (review A-6).
    let usage = measure::self_usage()?;

    let mut validation_errors = Vec::new();
    let (pages, bytes_decoded) = match outcome {
        Ok(outcome) => {
            validation_errors.extend(outcome.errors.iter().cloned());
            validate_outcome(engine, &spec, &outcome, &mut validation_errors);
            (outcome.pages, outcome.bytes_decoded)
        }
        Err(err) => {
            validation_errors.push(format!("engine error: {err:#}"));
            (0, 0)
        }
    };

    let wall_ms = wall.as_millis() as u64;
    let sample = measure::Sample {
        scenario: scenario_name.to_owned(),
        engine: engine.as_str().to_owned(),
        pages,
        wall_ms,
        pages_per_sec: if wall.as_secs_f64() > 0.0 {
            spec.expected.pages as f64 / wall.as_secs_f64()
        } else {
            0.0
        },
        bytes_decoded,
        // Identity placeholder; the orchestrator overwrites this field with
        // the authoritative server-side bytes-on-wire for the trial window.
        bytes_on_wire: bytes_decoded,
        max_rss_bytes: usage.max_rss_bytes,
        cpu_user_ms: usage.cpu_user_ms,
        cpu_sys_ms: usage.cpu_sys_ms,
        valid: validation_errors.is_empty(),
        validation_errors,
    };
    println!("{}", serde_json::to_string(&sample)?);
    Ok(())
}

/// Engine-side validation gates (PLAN.md §8, gates 4-6).
fn validate_outcome(
    engine: Engine,
    spec: &scenario::ScenarioSpec,
    outcome: &engines::EngineOutcome,
    errors: &mut Vec<String>,
) {
    let expected = &spec.expected;
    if outcome.pages != expected.pages {
        errors.push(format!(
            "pages {} != expected {}",
            outcome.pages, expected.pages
        ));
    }
    if outcome.bytes_decoded != expected.decoded_bytes {
        errors.push(format!(
            "decoded bytes {} != expected {}",
            outcome.bytes_decoded, expected.decoded_bytes
        ));
    }
    if outcome.checksum != expected.checksum {
        errors.push(format!(
            "checksum {:#x} != expected {:#x}",
            outcome.checksum, expected.checksum
        ));
    }
    // The baseline is a fetch ceiling; extraction gates apply to crawlers only.
    if engine != Engine::Baseline {
        if let Some(records) = expected.records
            && outcome.digest.count != records
        {
            errors.push(format!(
                "records {} != expected {records}",
                outcome.digest.count
            ));
        }
        if let Some(digest) = expected.digest
            && outcome.digest.value() != digest
        {
            errors.push(format!(
                "digest {:#x} != expected {digest:#x}",
                outcome.digest.value()
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// orchestrator
// ---------------------------------------------------------------------------

struct OrchestrateArgs {
    scenario: String,
    iters: usize,
    concurrency: usize,
    runtime_workers: usize,
    depth: Option<u32>,
    quick: bool,
    sensitivity: bool,
    out: Option<PathBuf>,
}

fn cmd_orchestrate(args: OrchestrateArgs) -> anyhow::Result<()> {
    let names: Vec<&str> = if args.scenario == "all" {
        scenarios::ALL.to_vec()
    } else {
        // Fail early on unknown names.
        scenarios::build(&args.scenario, "probe", args.depth)
            .map(|_| ())
            .or_else(|err| {
                if scenarios::ALL.contains(&args.scenario.as_str()) {
                    Ok(()) // known name; probe may fail for stub/depth reasons later
                } else {
                    Err(err)
                }
            })?;
        vec![scenarios::ALL
            .iter()
            .copied()
            .find(|n| *n == args.scenario)
            .context("scenario name not in registry")?]
    };
    if args.sensitivity {
        eprintln!(
            "note: --sensitivity accepted, but the M-http-raw sensitivity row is \
not implemented in the scaffold (PLAN.md §7)."
        );
    }

    let out_dir = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/results"));
    std::fs::create_dir_all(&out_dir)?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let samples_path = out_dir.join(format!("{timestamp}-samples.jsonl"));
    let metadata_path = out_dir.join(format!("{timestamp}-metadata.json"));
    let summary_path = out_dir.join(format!("{timestamp}-summary.md"));

    let runtime = tokio::runtime::Runtime::new()?;
    let exe = std::env::current_exe()?;
    let iters = if args.quick { 1 } else { args.iters };
    let mut suite_failures: Vec<String> = Vec::new();
    let mut samples_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&samples_path)?;

    for name in &names {
        runtime.block_on(run_scenario(
            name,
            &args,
            iters,
            &exe,
            &mut samples_file,
            &mut suite_failures,
        ))?;
    }
    drop(samples_file);

    report::write_metadata(&metadata_path, args.concurrency, args.runtime_workers)?;
    let summary = report::summarize(&samples_path)?;
    std::fs::write(&summary_path, &summary)?;
    println!("samples:  {}", samples_path.display());
    println!("metadata: {}", metadata_path.display());
    println!("summary:  {}", summary_path.display());

    if !suite_failures.is_empty() {
        for failure in &suite_failures {
            eprintln!("SUITE FAILURE: {failure}");
        }
        anyhow::bail!("{} suite failure(s); see above", suite_failures.len());
    }
    Ok(())
}

async fn run_scenario(
    name: &str,
    args: &OrchestrateArgs,
    iters: usize,
    exe: &std::path::Path,
    samples_file: &mut std::fs::File,
    suite_failures: &mut Vec<String>,
) -> anyhow::Result<()> {
    use std::io::Write as _;

    let nonce = format!(
        "{:016x}",
        seahash::hash(
            format!(
                "{}-{}-{name}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos()
            )
            .as_bytes()
        )
    );
    let spec = scenarios::build(name, &nonce, args.depth)?;
    let server = server::start(&spec.site, &nonce).await?;
    let root_url = format!("{}{}", server.base_url(), spec.root_path);
    eprintln!("[{name}] server at {} ({} pages)", server.base_url(), spec.expected.pages);

    // Warm-up burst: prime listener/accept path and page map caches.
    warm_up(&root_url).await?;
    server.state.snapshot_and_reset();

    let engines_order = [Engine::Millipede, Engine::Spider, Engine::Baseline];
    let child_timeout = Duration::from_secs(600);

    // One unmeasured warm-up run per engine (skipped in --quick).
    if !args.quick {
        for engine in engines_order {
            let child_args = child_args(name, engine, &root_url, args, &nonce);
            let _ = measure::run_child_trial(exe, &child_args, child_timeout)
                .await
                .with_context(|| format!("[{name}] warm-up run for {}", engine.as_str()))?;
            server.state.snapshot_and_reset();
        }
    }

    // Measured trials, interleaved M, S, B, M, S, B, ... (thermal/cache drift).
    let mut valid_counts = std::collections::BTreeMap::<&'static str, usize>::new();
    for iteration in 0..iters {
        for engine in engines_order {
            server.state.snapshot_and_reset();
            let child_args = child_args(name, engine, &root_url, args, &nonce);
            let mut sample = measure::run_child_trial(exe, &child_args, child_timeout)
                .await
                .with_context(|| {
                    format!("[{name}] trial {iteration} for {}", engine.as_str())
                })?;
            let snap = server.state.snapshot_and_reset();
            validate_server_side(&spec, engine, &snap, &mut sample);

            if sample.valid {
                *valid_counts.entry(engine.as_str()).or_default() += 1;
            } else {
                eprintln!(
                    "[{name}] INVALID trial ({}, iter {iteration}): {:?}",
                    engine.as_str(),
                    sample.validation_errors
                );
            }
            // Authoritative wire bytes come from the server window.
            sample.bytes_on_wire = snap.bytes_on_wire;

            let mut line = serde_json::to_value(&sample)?;
            let obj = line.as_object_mut().expect("sample serializes to object");
            obj.insert("concurrency".into(), args.concurrency.into());
            obj.insert("runtime_workers".into(), args.runtime_workers.into());
            obj.insert("connections".into(), snap.connections.into());
            obj.insert("iteration".into(), iteration.into());
            obj.insert("server".into(), serde_json::to_value(&snap)?);
            writeln!(samples_file, "{line}")?;
            eprintln!(
                "[{name}] {} iter {iteration}: {} ms, {:.1} pages/s, valid={}",
                engine.as_str(),
                sample.wall_ms,
                sample.pages_per_sec,
                sample.valid
            );
        }
    }

    // Publication gate: >= 5 valid trials per engine (PLAN.md §6), unless --quick.
    if !args.quick {
        for engine in engines_order {
            let valid = valid_counts.get(engine.as_str()).copied().unwrap_or(0);
            if valid < 5 {
                suite_failures.push(format!(
                    "[{name}] engine {} produced only {valid} valid trials (< 5)",
                    engine.as_str()
                ));
            }
        }
    } else {
        for engine in engines_order {
            let valid = valid_counts.get(engine.as_str()).copied().unwrap_or(0);
            if valid < iters {
                suite_failures.push(format!(
                    "[{name}] quick validation failed for engine {}",
                    engine.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn child_args(
    name: &str,
    engine: Engine,
    root_url: &str,
    args: &OrchestrateArgs,
    nonce: &str,
) -> Vec<String> {
    let mut child = vec![
        "run".to_owned(),
        "--scenario".to_owned(),
        name.to_owned(),
        "--engine".to_owned(),
        engine.as_str().to_owned(),
        "--url".to_owned(),
        root_url.to_owned(),
        "--concurrency".to_owned(),
        args.concurrency.to_string(),
        "--runtime-workers".to_owned(),
        args.runtime_workers.to_string(),
        "--nonce".to_owned(),
        nonce.to_owned(),
        "--json".to_owned(),
    ];
    if let Some(depth) = args.depth {
        child.push("--depth".to_owned());
        child.push(depth.to_string());
    }
    child
}

/// Server-side validation gates (PLAN.md §8, gates 1-3 and 7).
fn validate_server_side(
    spec: &scenario::ScenarioSpec,
    engine: Engine,
    snap: &server::ServerSnapshot,
    sample: &mut measure::Sample,
) {
    let mut push = |msg: String| {
        sample.valid = false;
        sample.validation_errors.push(msg);
    };
    if snap.unique_pages_hit != spec.expected.pages {
        push(format!(
            "server unique pages hit {} != expected {}",
            snap.unique_pages_hit, spec.expected.pages
        ));
    }
    if snap.duplicate_page_hits != 0 {
        push(format!("server duplicate page hits {}", snap.duplicate_page_hits));
    }
    if snap.robots_hits != 0 {
        push(format!("robots.txt hits {}", snap.robots_hits));
    }
    if snap.off_host_hits != 0 {
        push(format!("off-host (Host: localhost) hits {}", snap.off_host_hits));
    }
    if snap.unknown_hits != 0 {
        push(format!("unknown-path hits {}", snap.unknown_hits));
    }
    let redirect_count = spec.site.redirects.len() as u64;
    if redirect_count > 0 {
        // Every engine (incl. the baseline, which enters via the redirect
        // sources) must hit each redirect exactly once.
        if snap.unique_redirects_hit != redirect_count {
            push(format!(
                "redirect paths hit {} != expected {redirect_count}",
                snap.unique_redirects_hit
            ));
        }
        if snap.duplicate_redirect_hits != 0 {
            push(format!(
                "duplicate redirect hits {}",
                snap.duplicate_redirect_hits
            ));
        }
    }
    let _ = engine; // gates are engine-independent by design (review A-5)
}

async fn warm_up(root_url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let client = client.clone();
        let url = root_url.to_owned();
        tasks.push(tokio::spawn(async move {
            let _ = client.get(&url).send().await.and_then(|r| r.error_for_status());
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// report
// ---------------------------------------------------------------------------

fn cmd_report(samples: &std::path::Path) -> anyhow::Result<()> {
    let summary = report::summarize(samples)?;
    let summary_path = samples.with_file_name(
        samples
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.replace("samples.jsonl", "summary.md"))
            .unwrap_or_else(|| "summary.md".to_owned()),
    );
    std::fs::write(&summary_path, &summary)?;
    println!("{summary}");
    eprintln!("wrote {}", summary_path.display());
    Ok(())
}
