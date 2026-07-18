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

#[test]
fn expanded_core_surface_is_reachable() {
    #[allow(unused_imports)]
    use millipede::{
        AddRequestsBatchedResult, AntiBotDetector, AntiBotSignals, AntiBotTech, BatchAddHandle,
        ConfigError, DatasetInfo, DefaultAntiBotDetector, ErrorSnapshot, ErrorSnapshotter, KeyInfo,
        KeyList, KvEntry, ListKeysOptions, Page, RequestBuildError,
    };
}

#[cfg(feature = "browser")]
#[test]
fn browser_surface_is_reachable() {
    #[allow(unused_imports)]
    use millipede::{PageClosedHook, PageHook, PageOptions, PagePrepHook, PreLaunchHook};
}

#[cfg(feature = "fingerprint")]
#[test]
fn fingerprint_surface_is_reachable() {
    #[allow(unused_imports)]
    use millipede::{BrowserFingerprintGenerator, HeaderGenerator, HeaderProfile};
}

#[cfg(feature = "html")]
#[test]
fn html_surface_is_reachable() {
    #[allow(unused_imports)]
    use millipede::SynchronizedHtml;
}

#[cfg(feature = "storage-fs")]
#[test]
fn filesystem_storage_surface_is_reachable() {
    #[allow(unused_imports)]
    use millipede::{FsDataset, FsKeyValueStore};
}

#[cfg(feature = "storage-memory")]
#[test]
fn memory_storage_surface_is_reachable() {
    #[allow(unused_imports)]
    use millipede::{DomainRoundRobin, MemoryDataset, MemoryKeyValueStore};
}
