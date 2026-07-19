//! Fixed scenario registry. This file is owned by the scaffold and is NEVER
//! edited after scaffolding; scenario tasks implement only their own
//! `scenarios/<name>.rs` file against the [`crate::scenario::ScenarioSpec`]
//! contract.
//!
//! Constructor contract: `fn build(nonce: &str, depth: Option<u32>) ->
//! anyhow::Result<ScenarioSpec>`. All site paths must embed the run-nonce
//! prefix (e.g. `/<nonce>/p/{i}`) with relative links preserving it; `depth`
//! is the optional CLI override for depth-scalable scenarios (others must
//! reject or ignore it per their spec in PLAN.md §4).

use crate::scenario::ScenarioSpec;

pub mod books;
pub mod compressed;
pub mod hn;
pub mod latency;
pub mod mesh;
pub mod payload;
pub mod redirects;
pub mod tree;
pub mod wide;

/// All scenario names, in PLAN.md §4 matrix order.
pub const ALL: &[&str] = &[
    "tree",
    "wide",
    "mesh",
    "latency",
    "payload",
    "redirects",
    "compressed",
    "books",
    "hn",
];

/// Builds a scenario by registry name.
pub fn build(name: &str, nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    match name {
        "tree" => tree::build(nonce, depth),
        "wide" => wide::build(nonce, depth),
        "mesh" => mesh::build(nonce, depth),
        "latency" => latency::build(nonce, depth),
        "payload" => payload::build(nonce, depth),
        "redirects" => redirects::build(nonce, depth),
        "compressed" => compressed::build(nonce, depth),
        "books" => books::build(nonce, depth),
        "hn" => hn::build(nonce, depth),
        other => anyhow::bail!("unknown scenario `{other}`; known: {}", ALL.join(", ")),
    }
}
