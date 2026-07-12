#![doc = include_str!("../README.md")]

pub use millipede_core as core;

#[cfg(feature = "browser")]
pub use millipede_browser as browser;
#[cfg(feature = "browser-chromiumoxide")]
pub use millipede_browser_chromiumoxide as browser_chromiumoxide;
#[cfg(feature = "fingerprint")]
pub use millipede_fingerprint as fingerprint;
#[cfg(feature = "html")]
pub use millipede_html as html;
#[cfg(feature = "http")]
pub use millipede_http as http;
#[cfg(feature = "storage-fs")]
pub use millipede_storage_fs as storage_fs;
#[cfg(feature = "storage-memory")]
pub use millipede_storage_memory as storage_memory;

/// Curated public API for ergonomic `millipede::Type` imports.
pub use millipede_core::config::{Configuration, ConfigurationBuilder, LogLevel};
pub use millipede_core::errors::{AntiBotTech, CrawlError};
pub use millipede_core::events::{
    CrawlerEvent, EventBus, EventStream, HandledRequest, RequestFinalState,
};
pub use millipede_core::request::{
    HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuilder, RequestId, RequestState,
    UserData,
};
pub use millipede_core::storage::{
    AddOptions, AutoSaved, Dataset, DatasetExt, KeyValueStore, KeyValueStoreExt, Lease, LeaseId,
    ListOptions, ProcessedRequest, QueueOpInfo, ReclaimOptions, RequestQueue, RequestSource,
    StorageClient, StorageError, StorageResult,
};
#[cfg(feature = "storage-memory")]
pub use millipede_storage_memory::{MemoryQueuePolicy, MemoryRequestQueue, MemoryStorageClient};

/// Commonly used items across all enabled Millipede crates.
///
/// Empty until the first real types land (see `docs/ROADMAP.md`).
pub mod prelude {
    // The sub-crate preludes are empty until Phase 1, so their glob
    // re-exports would otherwise trip `unused_imports` under -D warnings.
    // Remove this allow once the preludes gain real items.
    #![allow(unused_imports)]

    pub use millipede_core::prelude::*;

    #[cfg(feature = "browser")]
    pub use millipede_browser::prelude::*;
    #[cfg(feature = "browser-chromiumoxide")]
    pub use millipede_browser_chromiumoxide::prelude::*;
    #[cfg(feature = "fingerprint")]
    pub use millipede_fingerprint::prelude::*;
    #[cfg(feature = "html")]
    pub use millipede_html::prelude::*;
    #[cfg(feature = "http")]
    pub use millipede_http::prelude::*;
    #[cfg(feature = "storage-fs")]
    pub use millipede_storage_fs::prelude::*;
    #[cfg(feature = "storage-memory")]
    pub use millipede_storage_memory::prelude::*;
}
