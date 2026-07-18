//! Lease-based request queue contracts.

use super::{StorageError, StorageResult};
use crate::request::{Request, RequestId};
use std::{fmt, time::Duration};

/// Identifier for one active request lease.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseId(u64);

impl LeaseId {
    /// Creates a lease identifier from its raw value.
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw identifier value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for LeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Temporary, linear ownership of a queued request.
///
/// This type deliberately does not implement `Clone`: exactly one of
/// [`RequestQueue::mark_handled`], [`RequestQueue::reclaim`], or
/// [`RequestQueue::abandon`] consumes it.
#[derive(Debug)]
pub struct Lease {
    /// Leased request.
    pub request: Request,
    /// Identifier used to renew this lease.
    pub lease_id: LeaseId,
    /// Current lease deadline.
    pub expires_at: std::time::Instant,
}

/// Options controlling insertion of requests.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[must_use = "add options do nothing unless passed to RequestQueue::add"]
pub struct AddOptions {
    /// Whether to insert at the front of the queue.
    pub forefront: bool,
}

/// Options controlling return of a leased request to the queue.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use = "reclaim options do nothing unless passed to RequestQueue::reclaim"]
pub struct ReclaimOptions {
    /// Whether to reinsert at the front of the queue.
    pub forefront: bool,
    /// Whether to increment the request retry count.
    pub increment_retry: bool,
}

impl Default for ReclaimOptions {
    fn default() -> Self {
        Self {
            forefront: false,
            increment_retry: true,
        }
    }
}

/// Result metadata for adding one request.
#[derive(Debug, Clone)]
#[must_use = "queue insertion results report deduplication state"]
pub struct ProcessedRequest {
    /// Stable request identifier.
    pub request_id: RequestId,
    /// Queue deduplication key.
    pub unique_key: String,
    /// Whether the request was already known to the queue.
    pub was_already_present: bool,
    /// Whether the known request had already been handled.
    pub was_already_handled: bool,
}

/// Alternate interface spelling for [`ProcessedRequest`]; both names describe the same payload.
pub type QueueOpInfo = ProcessedRequest;

/// A source from which requests can be added.
///
/// Sitemap ingestion ships as [`crate::sitemap::RequestQueueWithSitemap`], a queue wrapper rather
/// than a `RequestSource` variant, which keeps queue backends decoupled from fetching. This enum
/// remains non-exhaustive for future request-source variants.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RequestSource {
    /// One fully built request.
    Request(Request),
}

impl From<Request> for RequestSource {
    fn from(request: Request) -> Self {
        Self::Request(request)
    }
}

/// Completion result for a batched request addition.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use = "batched insertion results report processed requests"]
pub struct AddRequestsBatchedResult {
    /// All processed requests.
    pub processed: Vec<ProcessedRequest>,
}

/// Handle for observing completion of a batched request addition.
#[must_use = "batch handles must be awaited to observe completion"]
pub struct BatchAddHandle {
    /// Requests added synchronously.
    pub added: Vec<ProcessedRequest>,
    completion: Completion,
}

enum Completion {
    Ready(AddRequestsBatchedResult),
    Task(tokio::task::JoinHandle<StorageResult<AddRequestsBatchedResult>>),
}

impl fmt::Debug for BatchAddHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchAddHandle")
            .field("added", &self.added)
            .field("completion", &"<completion>")
            .finish()
    }
}

impl BatchAddHandle {
    /// Creates an already-completed batch handle.
    pub fn ready(added: Vec<ProcessedRequest>) -> Self {
        Self {
            completion: Completion::Ready(AddRequestsBatchedResult {
                processed: added.clone(),
            }),
            added,
        }
    }

    /// Creates a batch handle backed by a spawned completion task.
    pub fn deferred(
        added: Vec<ProcessedRequest>,
        task: tokio::task::JoinHandle<StorageResult<AddRequestsBatchedResult>>,
    ) -> Self {
        Self {
            added,
            completion: Completion::Task(task),
        }
    }

    /// Runs `notify` after this batch completes, even if the public handle is never awaited.
    pub(crate) fn notify_on_completion<F>(self, notify: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let Self { added, completion } = self;
        match completion {
            Completion::Ready(result) => {
                notify();
                Self {
                    added,
                    completion: Completion::Ready(result),
                }
            }
            Completion::Task(task) => Self {
                added,
                completion: Completion::Task(tokio::spawn(async move {
                    let result = task.await.map_err(|error| {
                        StorageError::Backend(anyhow::anyhow!("batch add task failed: {error}"))
                    });
                    notify();
                    result?
                })),
            },
        }
    }

    /// Waits until all requests in the batch have been processed.
    pub async fn wait(self) -> StorageResult<AddRequestsBatchedResult> {
        match self.completion {
            Completion::Ready(result) => Ok(result),
            Completion::Task(task) => task.await.map_err(|error| {
                StorageError::Backend(anyhow::anyhow!("batch add task failed: {error}"))
            })?,
        }
    }
}

/// Object-safe queue with temporary lease ownership.
///
/// Single-process backends may document lease expiry as a no-op, but must retain this complete
/// lease API so callers and distributed backends share one contract.
#[async_trait::async_trait]
pub trait RequestQueue: Send + Sync {
    /// Adds a request and returns its deduplication status.
    async fn add(&self, req: Request, opts: AddOptions) -> StorageResult<QueueOpInfo>;
    /// Adds multiple request sources and returns a handle for deferred completion.
    async fn add_batch(
        &self,
        reqs: Vec<RequestSource>,
        opts: AddOptions,
    ) -> StorageResult<BatchAddHandle>;
    /// Hands temporary ownership of the next request to the caller as a lease.
    async fn fetch_next(&self) -> StorageResult<Option<Lease>>;
    /// Marks a leased request as successful and consumes its lease.
    async fn mark_handled(&self, lease: Lease) -> StorageResult<()>;
    /// Re-queues a lease, incrementing retry count unless `opts` disables it.
    ///
    /// Backends must persist the request state carried in the lease (e.g. mutated `error_messages`
    /// or `session_rotation_count`), not a previously stored copy.
    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> StorageResult<()>;
    /// Extends an active lease deadline, returning `LeaseNotFound` if it is unknown or completed.
    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()>;
    /// Re-queues and consumes a lease without incrementing its request retry count.
    ///
    /// Backends must persist the request state carried in the lease (e.g. mutated `error_messages`
    /// or `session_rotation_count`), not a previously stored copy.
    async fn abandon(&self, lease: Lease) -> StorageResult<()>;
    /// Returns whether no requests are currently pending.
    async fn is_empty(&self) -> StorageResult<bool>;
    /// Returns whether no requests are pending or leased.
    async fn is_finished(&self) -> StorageResult<bool>;
    /// Returns the number of successfully handled requests.
    async fn handled_count(&self) -> StorageResult<u64>;
    /// Returns the number of pending requests.
    async fn pending_count(&self) -> StorageResult<u64>;
}
