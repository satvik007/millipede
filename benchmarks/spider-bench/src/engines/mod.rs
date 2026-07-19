//! Generic engine drivers. Each takes a `&TrialSpec`, a fixed concurrency
//! `C`, and the root URL, and returns an [`EngineOutcome`] once fully drained.
//!
//! ALL drivers are called AFTER the `go` signal: the timed region includes
//! engine/client construction (review A-2). The tokio runtime is built by the
//! child before `ready`, identically for all engines (review A-3).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::scenario::{Digest, PageWork, TrialSpec, body_checksum};

pub mod baseline;
pub mod millipede;
pub mod spider;

/// Engine selector for the child `run` subcommand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Engine {
    Millipede,
    Spider,
    Baseline,
}

impl Engine {
    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Millipede => "millipede",
            Engine::Spider => "spider",
            Engine::Baseline => "baseline",
        }
    }
}

/// What an engine driver observed for one trial (engine-side view; the
/// server-side hit set remains the authoritative count).
#[derive(Debug, Default)]
pub struct EngineOutcome {
    /// Pages the engine processed.
    pub pages: u64,
    /// Sum of decoded body lengths.
    pub bytes_decoded: u64,
    /// Wrapping sum of per-body seahash checksums.
    pub checksum: u64,
    /// Extraction digest (zero for raw rows and the baseline).
    pub digest: Digest,
    /// Engine-side validation failures (non-empty invalidates the trial).
    pub errors: Vec<String>,
}

/// Thread-safe accumulator shared by handler/drain tasks. Checksum and byte
/// sums use wrapping atomic adds, so accumulation is scheduling-order
/// independent by construction.
#[derive(Debug, Default)]
pub struct Accum {
    pub count: AtomicU64,
    pub bytes: AtomicU64,
    pub checksum: AtomicU64,
    pub digest: Mutex<Digest>,
}

impl Accum {
    /// Performs the shared per-page work on one decoded body.
    ///
    /// `parse` lazily supplies a parsed DOM for extraction rows; raw rows
    /// never invoke it (no re-parse anywhere on raw rows — review A-4).
    pub fn record_body(&self, work: &PageWork, body: &[u8], parse: impl FnOnce(&mut Digest)) {
        self.count.fetch_add(1, Ordering::AcqRel);
        self.bytes.fetch_add(body.len() as u64, Ordering::AcqRel);
        self.checksum
            .fetch_add(body_checksum(body), Ordering::AcqRel);
        if matches!(work, PageWork::Extract(_)) {
            let mut local = Digest::default();
            parse(&mut local);
            self.digest
                .lock()
                .expect("digest mutex poisoned")
                .merge(&local);
        }
    }

    /// Reads the accumulator into an outcome.
    ///
    /// Takes `&self` deliberately: millipede's crawler permanently owns its
    /// request-handler closure (`Arc<dyn RequestHandler>`), so the handler's
    /// `Arc<Accum>` clone outlives `run()` and `Arc::into_inner` can never
    /// succeed there. All fields are atomics (plus a mutex-guarded `Copy`
    /// digest), so reading through the `Arc` is exact once the engine reports
    /// that every request has finished.
    pub fn snapshot(&self, errors: Vec<String>) -> EngineOutcome {
        EngineOutcome {
            pages: self.count.load(Ordering::Acquire),
            bytes_decoded: self.bytes.load(Ordering::Acquire),
            checksum: self.checksum.load(Ordering::Acquire),
            digest: *self.digest.lock().expect("digest mutex poisoned"),
            errors,
        }
    }
}

/// Dispatches to the requested driver.
pub async fn run(
    engine: Engine,
    spec: &TrialSpec,
    concurrency: usize,
    root_url: &str,
) -> anyhow::Result<EngineOutcome> {
    match engine {
        Engine::Millipede => millipede::run(spec, concurrency, root_url).await,
        Engine::Spider => spider::run(spec, concurrency, root_url).await,
        Engine::Baseline => baseline::run(spec, concurrency, root_url).await,
    }
}
