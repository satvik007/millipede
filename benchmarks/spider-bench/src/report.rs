//! Reporting: `samples.jsonl` -> `summary.md`, plus `metadata.json`.
//!
//! Sample lines are the child JSON (measure::Sample) with orchestrator-added
//! fields: `concurrency`, `runtime_workers`, `connections` (server-side).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;

use anyhow::Context as _;
use serde_json::Value;

/// Claims-scope statement (PLAN.md §1). Included verbatim in every summary
/// and in the README.
pub const CLAIMS_SCOPE: &str = "Scope of claims: this suite measures success-path \
HTTP/1.1 (plus one redirect and one compression scenario) crawl throughput and \
peak RSS against a synthetic axum site on loopback, with identical page sets, \
fixed concurrency, zero client-side delays, no retries, and robots disabled on \
both engines. It does NOT characterize TLS, DNS, error/retry paths, anti-bot \
behavior, JS rendering, or politeness compliance. Live-network examples are \
never benchmarked directly; their workload shapes are replicated locally. \
Never publish a ratio without the absolute numbers and raw samples alongside.";

#[derive(Debug, Clone)]
struct Row {
    scenario: String,
    concurrency: u64,
    engine: String,
    pages: u64,
    wall_median_ms: f64,
    wall_iqr_ms: f64,
    wall_min_ms: u64,
    wall_max_ms: u64,
    pages_per_sec_median: f64,
    mib_per_sec_median: f64,
    rss_median_bytes: f64,
    cpu_median_ms: f64,
    conns_median: f64,
    valid: usize,
    total: usize,
}

fn median(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

fn quartile_span(sorted: &[f64]) -> f64 {
    if sorted.len() < 2 {
        return 0.0;
    }
    let q = |p: f64| -> f64 {
        let idx = p * (sorted.len() - 1) as f64;
        let lo = idx.floor() as usize;
        let hi = idx.ceil() as usize;
        sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo as f64)
    };
    q(0.75) - q(0.25)
}

fn f(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn u(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// Generates `summary.md` content from raw sample lines.
pub fn summarize(samples_path: &Path) -> anyhow::Result<String> {
    let raw = std::fs::read_to_string(samples_path)
        .with_context(|| format!("reading {}", samples_path.display()))?;
    let samples: Vec<Value> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).with_context(|| format!("parsing sample line: {l}")))
        .collect::<anyhow::Result<_>>()?;
    anyhow::ensure!(!samples.is_empty(), "no samples in {}", samples_path.display());

    // Group by (scenario, C, engine); engine order fixed for readability.
    let mut groups: BTreeMap<(String, u64, u8, String), Vec<&Value>> = BTreeMap::new();
    for sample in &samples {
        let scenario = sample["scenario"].as_str().unwrap_or("?").to_owned();
        let engine = sample["engine"].as_str().unwrap_or("?").to_owned();
        let c = u(sample, "concurrency");
        let order = match engine.as_str() {
            "millipede" => 0,
            "spider" => 1,
            "baseline" => 2,
            _ => 3,
        };
        groups.entry((scenario, c, order, engine)).or_default().push(sample);
    }

    let mut rows = Vec::new();
    for ((scenario, concurrency, _, engine), group) in &groups {
        let valid: Vec<&&Value> = group
            .iter()
            .filter(|s| s["valid"].as_bool().unwrap_or(false))
            .collect();
        let sorted = |key: &str| -> Vec<f64> {
            let mut v: Vec<f64> = valid.iter().map(|s| f(s, key)).collect();
            v.sort_by(f64::total_cmp);
            v
        };
        let walls = sorted("wall_ms");
        let wall_median_ms = median(&walls);
        let bytes = valid
            .first()
            .map(|s| u(s, "bytes_decoded"))
            .unwrap_or(0);
        let mib_per_sec_median = if wall_median_ms > 0.0 {
            (bytes as f64 / (1024.0 * 1024.0)) / (wall_median_ms / 1000.0)
        } else {
            0.0
        };
        rows.push(Row {
            scenario: scenario.clone(),
            concurrency: *concurrency,
            engine: engine.clone(),
            pages: group.first().map(|s| u(s, "pages")).unwrap_or(0),
            wall_median_ms,
            wall_iqr_ms: quartile_span(&walls),
            wall_min_ms: walls.first().copied().unwrap_or(0.0) as u64,
            wall_max_ms: walls.last().copied().unwrap_or(0.0) as u64,
            pages_per_sec_median: median(&sorted("pages_per_sec")),
            mib_per_sec_median,
            rss_median_bytes: median(&sorted("max_rss_bytes")),
            cpu_median_ms: {
                let mut v: Vec<f64> = valid
                    .iter()
                    .map(|s| f(s, "cpu_user_ms") + f(s, "cpu_sys_ms"))
                    .collect();
                v.sort_by(f64::total_cmp);
                median(&v)
            },
            conns_median: median(&sorted("connections")),
            valid: valid.len(),
            total: group.len(),
        });
    }

    let mut out = String::new();
    writeln!(out, "# spider-bench summary\n")?;
    writeln!(out, "{CLAIMS_SCOPE}\n")?;
    writeln!(
        out,
        "| scenario | C | engine | pages | median wall | IQR | pages/s | MiB/s | vs spider | peak RSS | CPU (u+s) | conns | validation |"
    )?;
    writeln!(
        out,
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|"
    )?;
    for row in &rows {
        let spider_pps = rows
            .iter()
            .find(|r| {
                r.scenario == row.scenario && r.concurrency == row.concurrency && r.engine == "spider"
            })
            .map(|r| r.pages_per_sec_median)
            .unwrap_or(0.0);
        let vs_spider = if row.engine == "millipede" && spider_pps > 0.0 && row.pages_per_sec_median > 0.0 {
            format!("{:.2}x", row.pages_per_sec_median / spider_pps)
        } else {
            "-".to_owned()
        };
        // Server-bound flag: fastest crawler >= 70% of baseline throughput.
        let baseline_pps = rows
            .iter()
            .find(|r| {
                r.scenario == row.scenario
                    && r.concurrency == row.concurrency
                    && r.engine == "baseline"
            })
            .map(|r| r.pages_per_sec_median)
            .unwrap_or(0.0);
        let server_bound = row.engine != "baseline"
            && baseline_pps > 0.0
            && row.pages_per_sec_median >= 0.7 * baseline_pps;
        let validation = if row.valid == row.total && row.valid > 0 {
            format!(
                "ok ({}/{}){}",
                row.valid,
                row.total,
                if server_bound { " ⚠ possibly server-bound (≥70% of baseline)" } else { "" }
            )
        } else {
            format!("INVALID ({}/{})", row.valid, row.total)
        };
        writeln!(
            out,
            "| {} | {} | {} | {} | {:.0} ms | {:.0} ms | {:.1} | {:.1} | {} | {:.1} MiB | {:.0} ms | {:.0} | {} |",
            row.scenario,
            row.concurrency,
            row.engine,
            row.pages,
            row.wall_median_ms,
            row.wall_iqr_ms,
            row.pages_per_sec_median,
            row.mib_per_sec_median,
            vs_spider,
            row.rss_median_bytes / (1024.0 * 1024.0),
            row.cpu_median_ms,
            row.conns_median,
            validation,
        )?;
    }
    writeln!(out)?;
    writeln!(
        out,
        "Wall min/max per row: {}.",
        rows.iter()
            .map(|r| format!(
                "{}/{}/{}: {}–{} ms",
                r.scenario, r.concurrency, r.engine, r.wall_min_ms, r.wall_max_ms
            ))
            .collect::<Vec<_>>()
            .join("; ")
    )?;
    writeln!(
        out,
        "\nDeltas under 5% are noise. Never compare RSS across OSes. Extraction \
rows (books, hn): spider re-parses bytes with scraper in its subscriber because \
it does not expose its internal lol_html DOM; millipede reuses its one parse. \
The CPU column makes that extra work visible (PLAN.md §7)."
    )?;
    Ok(out)
}

fn cmd_out(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Writes `metadata.json` describing the run environment (PLAN.md §8).
pub fn write_metadata(
    path: &Path,
    concurrency: usize,
    runtime_workers: usize,
) -> anyhow::Result<()> {
    let (cpu_model, ram_bytes) = if cfg!(target_os = "macos") {
        (
            cmd_out("sysctl", &["-n", "machdep.cpu.brand_string"]),
            cmd_out("sysctl", &["-n", "hw.memsize"]),
        )
    } else {
        (
            cmd_out("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"]),
            cmd_out("sh", &["-c", "grep -m1 MemTotal /proc/meminfo | awk '{print $2*1024}'"]),
        )
    };
    let metadata = serde_json::json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "kernel": cmd_out("uname", &["-sr"]),
        "cpu_model": cpu_model.trim(),
        "ram_bytes": ram_bytes,
        "rustc": cmd_out("rustc", &["-V"]),
        "git_commit": cmd_out("git", &["rev-parse", "HEAD"]),
        "spider_version": "2.52.9",
        "spider_features": if cfg!(feature = "spider-upstream-defaults") {
            "sync + upstream defaults (sensitivity build)"
        } else {
            "sync only (headline)"
        },
        "runtime_workers": runtime_workers,
        "concurrency": concurrency,
        "profile": if cfg!(debug_assertions) { "debug (NOT publishable)" } else { "release" },
    });
    std::fs::write(path, serde_json::to_string_pretty(&metadata)?)?;
    Ok(())
}
