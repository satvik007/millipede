//! `hn` scenario — scaffold stub. See PLAN.md §4 for the site shape.
//!
//! Owned by its scenario task; the scaffold never edits this file again.

use crate::scenario::ScenarioSpec;

/// Builds the `hn` scenario for the given run-nonce (and optional depth
/// override where the scenario supports one).
pub fn build(_nonce: &str, _depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    anyhow::bail!("scenario `hn` is not implemented yet (scaffold stub)")
}
