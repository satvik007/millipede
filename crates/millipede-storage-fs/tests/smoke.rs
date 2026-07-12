//! Smoke test: proves `millipede-storage-fs` compiles, links, and exposes its prelude module.

#[test]
fn crate_links_and_prelude_exists() {
    // Path-resolves the (currently empty) prelude module.
    #[allow(unused_imports)]
    use millipede_storage_fs::prelude;
}
