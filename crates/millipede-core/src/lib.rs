#![doc = include_str!("../README.md")]

#[cfg(test)]
extern crate self as millipede_core;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/dataset.rs"]
mod dataset;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/kvs.rs"]
mod kvs;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/client.rs"]
mod memory_client;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/policy.rs"]
mod policy;
#[cfg(test)]
#[allow(dead_code, unreachable_patterns)]
#[path = "../../millipede-storage-memory/src/queue.rs"]
mod queue;
#[cfg(test)]
use dataset::MemoryDataset;
#[cfg(test)]
use kvs::MemoryKeyValueStore;
#[cfg(test)]
use queue::MemoryRequestQueue;

/// Crawler configuration and environment resolution.
pub mod config;
/// Crawler lifecycle kinds, handles, and shared state.
pub mod crawler;
/// Crawl error taxonomy and retry classification.
pub mod errors;
/// Crawler lifecycle events and broadcast support.
pub mod events;
/// Request handler and middleware contracts.
pub mod handler;
/// Request data types and construction helpers.
pub mod request;
/// Label- and method-based request routing.
pub mod router;
/// Crawl statistics, rates, and persistence.
pub mod statistics;
/// Object-safe storage abstractions and typed convenience wrappers.
pub mod storage;

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::config::{Configuration, ConfigurationBuilder, LogLevel};
    pub use crate::crawler::{
        BasicContext, BasicKind, CrawlerEnv, CrawlerHandle, CrawlerKind, RequestEnv,
        RequestOutcome, RequestPrep,
    };
    pub use crate::errors::{AntiBotTech, CrawlError};
    pub use crate::events::{
        CrawlerEvent, EventBus, EventStream, HandledRequest, RequestFinalState, ResultStream,
    };
    pub use crate::handler::{
        FailedRequestContext, FailedRequestHandler, Middleware, RequestHandler,
    };
    pub use crate::request::{
        HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuildError, RequestBuilder,
        RequestId, RequestState, UserData,
    };
    pub use crate::router::{HasRequest, MethodFilter, Router};
    pub use crate::statistics::{
        FinalStatistics, STATISTICS_PERSIST_KEY, StatisticsHandle, StatisticsSnapshot,
    };
    pub use crate::storage::{
        AddOptions, AddRequestsBatchedResult, AutoSaved, BatchAddHandle, Dataset, DatasetExt,
        DatasetInfo, KeyInfo, KeyList, KeyValueStore, KeyValueStoreExt, KvEntry, Lease, LeaseId,
        ListKeysOptions, ListOptions, Page, ProcessedRequest, QueueOpInfo, ReclaimOptions,
        RequestQueue, RequestSource, StorageClient, StorageError, StorageResult,
    };
}
