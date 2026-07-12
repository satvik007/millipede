//! Smoke test for the `millipede` umbrella crate.

#[test]
fn umbrella_links_and_prelude_exists() {
    // Path-resolves the umbrella prelude module.
    #[allow(unused_imports)]
    use millipede::prelude;
}

#[test]
fn core_reexport_is_reachable() {
    #[allow(unused_imports)]
    use millipede::core::prelude;
}
