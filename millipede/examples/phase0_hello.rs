//! Phase 0 placeholder example.
//!
//! Proves the workspace skeleton builds and the umbrella crate's prelude
//! resolves. Replaced by real crawling examples from Phase 1 onward.

fn main() {
    // Prove the umbrella crate and its (currently empty) prelude resolve.
    #[allow(unused_imports)]
    use millipede::prelude;

    println!(
        "millipede {} — workspace skeleton is alive (Phase 0)",
        env!("CARGO_PKG_VERSION")
    );
}
