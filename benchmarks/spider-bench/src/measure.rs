//! Ready/go handshake, wall-clock timing, and self-reported resource usage.
//!
//! Protocol (PLAN.md §6, amended): the child parses args and resolves its
//! per-page work fn, then prints `ready` on stdout BEFORE constructing any
//! HTTP client, crawler, or `Website` — and WITHOUT ever pre-rendering the
//! site (the full page-body map would contaminate the child's peak-RSS/CPU
//! self-report). The parent replies with one JSON [`crate::scenario::TrialWire`]
//! line (ground truth + baseline entry URLs) followed by `go`; the child
//! starts its `Instant` on `go`. The timer stops only after the engine has
//! fully drained; the child then calls `getrusage(RUSAGE_SELF)` itself
//! (review A-6 — never the orchestrator, never `RUSAGE_CHILDREN`) and prints
//! exactly one JSON line. The child also self-reports its RSS at `go`
//! (`ready_rss_bytes`), implementing §6 step 5's baseline-RSS checkpoint in
//! the only process that can attribute it.

use std::io::{BufRead, Write};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::scenario::TrialWire;

/// One trial result: the single JSON line a child prints (PLAN.md §6 step 7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub scenario: String,
    pub engine: String,
    pub pages: u64,
    pub wall_ms: u64,
    pub pages_per_sec: f64,
    pub bytes_decoded: u64,
    pub bytes_on_wire: u64,
    pub max_rss_bytes: u64,
    /// Child RSS at the `go` checkpoint, before any engine exists: the
    /// harness-attributable floor under `max_rss_bytes` (PLAN.md §6 step 5).
    #[serde(default)]
    pub ready_rss_bytes: u64,
    pub cpu_user_ms: u64,
    pub cpu_sys_ms: u64,
    pub valid: bool,
    pub validation_errors: Vec<String>,
}

/// Child side: print `ready`, receive the [`TrialWire`] spec line, then block
/// until the parent sends `go`.
pub fn signal_ready_and_await_go() -> anyhow::Result<TrialWire> {
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(b"ready\n")?;
    stdout.flush()?;
    drop(stdout);
    let stdin = std::io::stdin();
    let mut lines = stdin.lock();
    let mut spec_line = String::new();
    lines
        .read_line(&mut spec_line)
        .context("reading TrialWire spec line from stdin")?;
    let wire: TrialWire = serde_json::from_str(spec_line.trim())
        .context("parsing TrialWire spec line from orchestrator")?;
    let mut line = String::new();
    lines
        .read_line(&mut line)
        .context("reading go signal from stdin")?;
    anyhow::ensure!(
        line.trim() == "go",
        "expected `go` from orchestrator, got {line:?}"
    );
    Ok(wire)
}

/// Resource usage self-reported by the child process.
#[derive(Debug, Clone, Copy)]
pub struct SelfUsage {
    /// Peak resident set size, normalized to bytes on every OS.
    pub max_rss_bytes: u64,
    pub cpu_user_ms: u64,
    pub cpu_sys_ms: u64,
}

/// Reads `getrusage(RUSAGE_SELF)` for the CURRENT process.
///
/// `ru_maxrss` units differ: macOS reports bytes, Linux reports KiB. This is
/// the only `unsafe` in the package — legal here because spider-bench is a
/// standalone package outside the workspace `unsafe_code = "deny"` lint wall.
pub fn self_usage() -> anyhow::Result<SelfUsage> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: RUSAGE_SELF with a properly sized, writable rusage out-pointer.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    anyhow::ensure!(
        rc == 0,
        "getrusage failed: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: getrusage returned 0, so the struct is fully initialized.
    let usage = unsafe { usage.assume_init() };
    let max_rss_raw = u64::try_from(usage.ru_maxrss).unwrap_or(0);
    let max_rss_bytes = if cfg!(target_os = "macos") {
        max_rss_raw
    } else {
        max_rss_raw * 1024
    };
    let tv_ms = |tv: libc::timeval| -> u64 {
        u64::try_from(tv.tv_sec).unwrap_or(0) * 1000 + u64::try_from(tv.tv_usec).unwrap_or(0) / 1000
    };
    Ok(SelfUsage {
        max_rss_bytes,
        cpu_user_ms: tv_ms(usage.ru_utime),
        cpu_sys_ms: tv_ms(usage.ru_stime),
    })
}

/// Proxy environment variables that must never leak into a trial child.
const PROXY_ENV_VARS: &[&str] = &[
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
];

/// Parent side: spawn one fresh child trial, drive the ready/spec/go
/// handshake, and parse the child's single JSON result line.
///
/// `wire_spec_json` is the serialized [`TrialWire`] the parent computed from
/// the fully rendered site; it is written to the child right after `ready`.
///
/// The child environment is proxy-neutral: millipede's client hard-disables
/// proxying (`.no_proxy()`), but spider's reqwest 0.13 dependency enables the
/// `system-proxy` feature and its client builder never opts out, so env vars
/// or platform proxy settings could give spider a different network path.
/// Removing `*_PROXY` and pinning `NO_PROXY=127.0.0.1,localhost` (which
/// hyper-util's system matcher honors even when macOS/Windows system proxy
/// settings are present) guarantees identical direct-loopback transport for
/// every engine.
pub async fn run_child_trial(
    program: &std::path::Path,
    args: &[String],
    wire_spec_json: &str,
    timeout: Duration,
) -> anyhow::Result<Sample> {
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    for var in PROXY_ENV_VARS {
        command.env_remove(var);
    }
    command.env("NO_PROXY", "127.0.0.1,localhost");
    command.env("no_proxy", "127.0.0.1,localhost");
    let mut child = command.spawn().context("spawning bench child process")?;
    let result = tokio::time::timeout(timeout, drive_child(&mut child, wire_spec_json)).await;
    match result {
        Ok(sample) => {
            let status = child.wait().await?;
            let sample = sample?;
            anyhow::ensure!(status.success(), "child exited with {status}");
            Ok(sample)
        }
        Err(_) => {
            let _ = child.kill().await;
            anyhow::bail!("child trial timed out after {timeout:?}")
        }
    }
}

async fn drive_child(
    child: &mut tokio::process::Child,
    wire_spec_json: &str,
) -> anyhow::Result<Sample> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    let stdout = child.stdout.take().context("child stdout not piped")?;
    let mut stdin = child.stdin.take().context("child stdin not piped")?;
    let mut lines = tokio::io::BufReader::new(stdout).lines();

    // Wait for `ready` (the child must not have built any engine yet).
    loop {
        let line = lines
            .next_line()
            .await?
            .context("child exited before printing `ready`")?;
        if line.trim() == "ready" {
            break;
        }
    }
    stdin.write_all(wire_spec_json.as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.write_all(b"go\n").await?;
    stdin.flush().await?;

    // The next JSON object line is the result.
    loop {
        let line = lines
            .next_line()
            .await?
            .context("child exited before printing its result line")?;
        let trimmed = line.trim();
        if trimmed.starts_with('{') {
            return serde_json::from_str(trimmed)
                .with_context(|| format!("parsing child result line: {trimmed}"));
        }
    }
}
