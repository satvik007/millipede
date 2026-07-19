//! Ready/go handshake, wall-clock timing, and self-reported resource usage.
//!
//! Protocol (PLAN.md §6): the child parses args and builds its scenario spec,
//! then prints `ready` on stdout BEFORE constructing any HTTP client, crawler,
//! or `Website`. The parent replies `go` on the child's stdin; the child
//! starts its `Instant` on `go`. The timer stops only after the engine has
//! fully drained; the child then calls `getrusage(RUSAGE_SELF)` itself
//! (review A-6 — never the orchestrator, never `RUSAGE_CHILDREN`) and prints
//! exactly one JSON line.

use std::io::{BufRead, Write};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

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
    pub cpu_user_ms: u64,
    pub cpu_sys_ms: u64,
    pub valid: bool,
    pub validation_errors: Vec<String>,
}

/// Child side: print `ready`, then block until the parent sends `go`.
pub fn signal_ready_and_await_go() -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(b"ready\n")?;
    stdout.flush()?;
    drop(stdout);
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .context("reading go signal from stdin")?;
    anyhow::ensure!(
        line.trim() == "go",
        "expected `go` from orchestrator, got {line:?}"
    );
    Ok(())
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
    anyhow::ensure!(rc == 0, "getrusage failed: {}", std::io::Error::last_os_error());
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

/// Parent side: spawn one fresh child trial, drive the ready/go handshake, and
/// parse the child's single JSON result line.
pub async fn run_child_trial(
    exe: &std::path::Path,
    args: &[String],
    timeout: Duration,
) -> anyhow::Result<Sample> {
    let mut child = tokio::process::Command::new(exe)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning bench child process")?;
    let result = tokio::time::timeout(timeout, drive_child(&mut child)).await;
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

async fn drive_child(child: &mut tokio::process::Child) -> anyhow::Result<Sample> {
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
