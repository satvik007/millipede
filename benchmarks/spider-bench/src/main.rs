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
#[command(
    name = "spider-bench",
    version,
    about = "millipede vs spider benchmark harness"
)]
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
        } => cmd_run(
            &scenario,
            engine,
            &url,
            concurrency,
            runtime_workers,
            &nonce,
            depth,
        ),
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
    // The child must NEVER build the full site: pre-rendering every page body
    // here would raise this process's ru_maxrss high-water mark by the whole
    // site size (~256 MiB for `payload`) and charge rendering/checksum CPU to
    // the trial, contaminating the published RSS/CPU columns. Ground truth
    // arrives from the orchestrator as a TrialWire line; only the per-page
    // work fn is resolved locally (unmeasured setup, before `ready`).
    let _ = (nonce, depth); // orchestrator-consistency args; unused in child mode
    let work = scenarios::work(scenario_name)?;
    // The runtime is harness plumbing, identical for every engine; it exists
    // before `ready`. No HTTP client/crawler/Website exists yet (review A-2).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_workers)
        .enable_all()
        .build()?;

    let wire = measure::signal_ready_and_await_go()?;
    let spec = scenario::TrialSpec {
        expected: wire.expected,
        work,
        entry_urls: wire.entry_urls,
    };
    // Baseline-RSS checkpoint at `go`: no engine exists yet, so this is the
    // harness-attributable floor under the final peak (PLAN.md §6 step 5).
    let ready_usage = measure::self_usage()?;
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
            validate_outcome(engine, &spec.expected, &outcome, &mut validation_errors);
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
        ready_rss_bytes: ready_usage.max_rss_bytes,
        cpu_user_ms: usage.cpu_user_ms.saturating_sub(ready_usage.cpu_user_ms),
        cpu_sys_ms: usage.cpu_sys_ms.saturating_sub(ready_usage.cpu_sys_ms),
        valid: validation_errors.is_empty(),
        validation_errors,
    };
    println!("{}", serde_json::to_string(&sample)?);
    Ok(())
}

/// Engine-side validation gates (PLAN.md §8, gates 4-6).
fn validate_outcome(
    engine: Engine,
    expected: &scenario::Expected,
    outcome: &engines::EngineOutcome,
    errors: &mut Vec<String>,
) {
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
    let external = prepare_external_runners()?;
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
        vec![
            scenarios::ALL
                .iter()
                .copied()
                .find(|n| *n == args.scenario)
                .context("scenario name not in registry")?,
        ]
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
            &external,
            &mut samples_file,
            &mut suite_failures,
        ))?;
    }
    drop(samples_file);

    report::write_metadata(
        &metadata_path,
        &report::RunMeta {
            concurrency: args.concurrency,
            runtime_workers: args.runtime_workers,
            iters,
            quick: args.quick,
            depth: args.depth,
            scenarios: names.iter().map(|n| (*n).to_owned()).collect(),
            command: std::env::args().collect::<Vec<_>>().join(" "),
        },
    )?;
    let summary = report::summarize(&samples_path)?;
    std::fs::write(&summary_path, &summary.markdown)?;
    println!("samples:  {}", samples_path.display());
    println!("metadata: {}", metadata_path.display());
    println!("summary:  {}", summary_path.display());

    // PLAN.md §5: a server-bound row must be scaled up before publication —
    // enforce it as a suite failure, not a footnote (quick runs are never
    // publishable, so they only warn).
    for row in &summary.server_bound {
        if args.quick {
            eprintln!("warning (quick, not publishable): {row}");
        } else {
            suite_failures.push(format!(
                "server-bound row must be scaled up before publication: {row}"
            ));
        }
    }

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
    external: &ExternalRunners,
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
    eprintln!(
        "[{name}] server at {} ({} pages)",
        server.base_url(),
        spec.expected.pages
    );

    // The child never renders the site (RSS/CPU isolation): the orchestrator
    // ships the ground truth + baseline entry URLs over the handshake.
    let wire = serde_json::to_string(&scenario::TrialWire {
        expected: spec.expected.clone(),
        entry_urls: scenario::baseline_entry_urls(&spec.site, &server.base_url()),
    })?;

    // Warm-up burst: prime listener/accept path and page map caches.
    warm_up(&root_url).await?;
    server.state.snapshot_and_reset();

    let engines_order = [
        Engine::Millipede,
        Engine::Spider,
        Engine::Gocolly,
        Engine::Crawlee,
        Engine::Baseline,
    ];
    let child_timeout = Duration::from_secs(600);
    // Some scenarios are impossible for spider by construction (e.g. its SSRF
    // guard vs loopback redirects); report N/A instead of a doomed row.
    let runs_engine = |engine: Engine| -> bool {
        if engine != Engine::Spider {
            return true;
        }
        match engines::spider::unsupported_reason(name) {
            Some(reason) => {
                eprintln!("[{name}] spider N/A: {reason}");
                false
            }
            None => true,
        }
    };
    let active_engines: Vec<Engine> = engines_order
        .into_iter()
        .filter(|&engine| runs_engine(engine))
        .collect();

    // One unmeasured warm-up run per engine (skipped in --quick).
    if !args.quick {
        for &engine in &active_engines {
            let child = child_command(name, engine, &root_url, args, &nonce, exe, external);
            let _ = measure::run_child_trial(&child.program, &child.args, &wire, child_timeout)
                .await
                .with_context(|| format!("[{name}] warm-up run for {}", engine.as_str()))?;
            server.state.snapshot_and_reset();
        }
    }

    // Measured trials use a cyclically rotated engine order to spread both
    // thermal drift and fixed-position bias across engines.
    let mut valid_counts = std::collections::BTreeMap::<&'static str, usize>::new();
    for iteration in 0..iters {
        for offset in 0..active_engines.len() {
            let engine = active_engines[(iteration + offset) % active_engines.len()];
            server.state.snapshot_and_reset();
            let child = child_command(name, engine, &root_url, args, &nonce, exe, external);
            let mut sample =
                measure::run_child_trial(&child.program, &child.args, &wire, child_timeout)
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
                // PLAN.md §6 step 8: ANY invalid measured trial fails the
                // suite loudly — not only rows that fall under 5 valid.
                suite_failures.push(format!(
                    "[{name}] invalid trial (engine {}, iter {iteration}): {}",
                    engine.as_str(),
                    sample.validation_errors.join("; ")
                ));
            }
            // Authoritative wire bytes come from the server window.
            sample.bytes_on_wire = snap.bytes_on_wire;

            let mut line = serde_json::to_value(&sample)?;
            let obj = line.as_object_mut().expect("sample serializes to object");
            obj.insert("concurrency".into(), args.concurrency.into());
            let effective_workers = if engine == Engine::Crawlee {
                1 // Cheerio handlers and parsing run on Node's event-loop thread.
            } else {
                args.runtime_workers
            };
            obj.insert("runtime_workers".into(), effective_workers.into());
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

    // Publication gate: >= 5 valid trials per engine (PLAN.md §6), unless
    // --quick. Engines marked N/A for the scenario are exempt (reported as
    // N/A with the reason, never as a failure).
    if !args.quick {
        for &engine in &active_engines {
            let valid = valid_counts.get(engine.as_str()).copied().unwrap_or(0);
            if valid < 5 {
                suite_failures.push(format!(
                    "[{name}] engine {} produced only {valid} valid trials (< 5)",
                    engine.as_str()
                ));
            }
        }
    } else {
        for &engine in &active_engines {
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

struct ExternalRunners {
    gocolly: PathBuf,
    crawlee: PathBuf,
}

struct ChildCommand {
    program: PathBuf,
    args: Vec<String>,
}

fn prepare_external_runners() -> anyhow::Result<ExternalRunners> {
    use std::process::Command;

    let benchmarks = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("spider-bench must live under benchmarks/")?
        .to_owned();
    let go_dir = benchmarks.join("gocolly-bench");
    let go_target = go_dir.join("target/gocolly-bench");
    std::fs::create_dir_all(go_target.parent().expect("target has parent"))?;
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&go_target)
        .arg(".")
        .current_dir(&go_dir)
        .status()
        .context("building the gocolly benchmark runner (is Go installed?)")?;
    anyhow::ensure!(
        status.success(),
        "`go build` failed for {}",
        go_dir.display()
    );

    let crawlee_dir = benchmarks.join("crawlee-bench");
    let status = Command::new("npm")
        .arg("ci")
        .arg("--no-audit")
        .arg("--no-fund")
        .current_dir(&crawlee_dir)
        .status()
        .context("installing the Crawlee benchmark runner (are Node.js and npm installed?)")?;
    anyhow::ensure!(
        status.success(),
        "`npm ci` failed for {}",
        crawlee_dir.display()
    );

    Ok(ExternalRunners {
        gocolly: go_target,
        crawlee: crawlee_dir.join("runner.mjs"),
    })
}

fn child_command(
    name: &str,
    engine: Engine,
    root_url: &str,
    args: &OrchestrateArgs,
    nonce: &str,
    rust_exe: &std::path::Path,
    external: &ExternalRunners,
) -> ChildCommand {
    let mut common = vec![
        "--scenario".to_owned(),
        name.to_owned(),
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
        common.push("--depth".to_owned());
        common.push(depth.to_string());
    }
    match engine {
        Engine::Millipede | Engine::Spider | Engine::Baseline => {
            let mut rust_args = vec!["run".to_owned()];
            rust_args.extend(common);
            rust_args.extend(["--engine".to_owned(), engine.as_str().to_owned()]);
            ChildCommand {
                program: rust_exe.to_owned(),
                args: rust_args,
            }
        }
        Engine::Gocolly => ChildCommand {
            program: external.gocolly.clone(),
            args: common,
        },
        Engine::Crawlee => {
            let mut node_args = vec![external.crawlee.display().to_string()];
            node_args.extend(common);
            ChildCommand {
                program: PathBuf::from("node"),
                args: node_args,
            }
        }
    }
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
        push(format!(
            "server duplicate page hits {}",
            snap.duplicate_page_hits
        ));
    }
    if snap.robots_hits != 0 {
        push(format!("robots.txt hits {}", snap.robots_hits));
    }
    if snap.off_host_hits != 0 {
        push(format!(
            "off-host (Host: localhost) hits {}",
            snap.off_host_hits
        ));
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
            let _ = client
                .get(&url)
                .send()
                .await
                .and_then(|r| r.error_for_status());
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
    std::fs::write(&summary_path, &summary.markdown)?;
    println!("{}", summary.markdown);
    for row in &summary.server_bound {
        eprintln!("NOT PUBLISHABLE — server-bound row must be scaled up first: {row}");
    }
    eprintln!("wrote {}", summary_path.display());
    Ok(())
}
