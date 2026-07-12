//! Smoke test: proves `millipede-html` compiles, links, and exposes its prelude module.

#[test]
fn crate_links_and_prelude_exists() {
    // Path-resolves the (currently empty) prelude module.
    #[allow(unused_imports)]
    use millipede_html::prelude;
}
