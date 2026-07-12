//! Smoke test: proves `millipede-http` compiles, links, and exposes its prelude module.

#[test]
fn crate_links_and_prelude_exists() {
    // Path-resolves the (currently empty) prelude module.
    #[allow(unused_imports)]
    use millipede_http::prelude;
}
