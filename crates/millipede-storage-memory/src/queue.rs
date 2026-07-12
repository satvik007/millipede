//! In-process request queue implementation.
//!
//! Lease expiry is a documented no-op in this single-process backend: an expired lease is never
//! reassigned. Marking handled, reclaiming, renewing, and abandoning still enforce the full lease
//! contract so distributed backends can implement real expiry without an API break.

use crate::policy::{Frontier, MemoryQueuePolicy};
use millipede_core::{
    request::{Request, RequestId},
    storage::{
        AddOptions, BatchAddHandle, Lease, LeaseId, ProcessedRequest, QueueOpInfo, ReclaimOptions,
        RequestQueue, RequestSource, StorageError, StorageResult,
    },
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Mutex,
    time::{Duration, Instant},
};

const LEASE_TTL: Duration = Duration::from_secs(180);

/// A thread-safe, lease-based request queue stored entirely in memory.
pub struct MemoryRequestQueue {
    name: String,
    state: Mutex<QueueState>,
}

struct QueueState {
    dedup: HashMap<String, RequestId>,
    handled: HashSet<String>,
    pending: Frontier,
    in_flight: HashMap<u64, LeasedRequest>,
    handled_count: u64,
    next_lease_id: u64,
}

struct LeasedRequest {
    request: Request,
    expires_at: Instant,
}

impl fmt::Debug for MemoryRequestQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryRequestQueue")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl MemoryRequestQueue {
    /// Creates an empty FIFO queue with the given name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_policy(name, MemoryQueuePolicy::Fifo)
    }

    /// Creates an empty queue using the selected frontier policy.
    #[must_use]
    pub fn with_policy(name: impl Into<String>, policy: MemoryQueuePolicy) -> Self {
        Self {
            name: name.into(),
            state: Mutex::new(QueueState {
                dedup: HashMap::new(),
                handled: HashSet::new(),
                pending: Frontier::new(policy),
                in_flight: HashMap::new(),
                handled_count: 0,
                next_lease_id: 0,
            }),
        }
    }

    fn add_locked(state: &mut QueueState, request: Request, opts: &AddOptions) -> QueueOpInfo {
        let unique_key = request.unique_key.clone();
        if let Some(request_id) = state.dedup.get(&unique_key) {
            return ProcessedRequest {
                request_id: request_id.clone(),
                unique_key: unique_key.clone(),
                was_already_present: true,
                was_already_handled: state.handled.contains(&unique_key),
            };
        }

        let request_id = request.id.clone();
        state.dedup.insert(unique_key.clone(), request_id.clone());
        if opts.forefront {
            state.pending.push_front(request);
        } else {
            state.pending.push_back(request);
        }
        ProcessedRequest {
            request_id,
            unique_key,
            was_already_present: false,
            was_already_handled: false,
        }
    }

    fn remove_lease(state: &mut QueueState, lease_id: &LeaseId) -> StorageResult<LeasedRequest> {
        state
            .in_flight
            .remove(&lease_id.as_u64())
            .ok_or_else(|| StorageError::LeaseNotFound {
                lease_id: lease_id.clone(),
            })
    }
}

#[async_trait::async_trait]
impl RequestQueue for MemoryRequestQueue {
    async fn add(&self, request: Request, opts: AddOptions) -> StorageResult<QueueOpInfo> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        Ok(Self::add_locked(&mut state, request, &opts))
    }

    async fn add_batch(
        &self,
        requests: Vec<RequestSource>,
        opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        let mut infos = Vec::with_capacity(requests.len());
        for source in requests {
            let request = match source {
                RequestSource::Request(request) => request,
                _ => return Err(StorageError::Unsupported("memory request source")),
            };
            infos.push(Self::add_locked(&mut state, request, &opts));
        }
        Ok(BatchAddHandle::ready(infos))
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        let Some(request) = state.pending.pop_front() else {
            return Ok(None);
        };
        let raw_lease_id = state.next_lease_id;
        state.next_lease_id += 1;
        let lease_id = LeaseId::new(raw_lease_id);
        let expires_at = Instant::now() + LEASE_TTL;
        state.in_flight.insert(
            raw_lease_id,
            LeasedRequest {
                request: request.clone(),
                expires_at,
            },
        );
        Ok(Some(Lease {
            request,
            lease_id,
            expires_at,
        }))
    }

    async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        let leased = Self::remove_lease(&mut state, &lease.lease_id)?;
        state.handled_count += 1;
        state.handled.insert(leased.request.unique_key);
        Ok(())
    }

    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> StorageResult<()> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        let mut request = Self::remove_lease(&mut state, &lease.lease_id)?.request;
        if opts.increment_retry {
            request.retry_count += 1;
        }
        if opts.forefront {
            state.pending.push_front(request);
        } else {
            state.pending.push_back(request);
        }
        Ok(())
    }

    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        let leased = state.in_flight.get_mut(&lease_id.as_u64()).ok_or_else(|| {
            StorageError::LeaseNotFound {
                lease_id: lease_id.clone(),
            }
        })?;
        leased.expires_at += extend_by;
        Ok(())
    }

    async fn abandon(&self, lease: Lease) -> StorageResult<()> {
        let mut state = self.state.lock().expect("request queue mutex poisoned");
        let request = Self::remove_lease(&mut state, &lease.lease_id)?.request;
        state.pending.push_front(request);
        Ok(())
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        let state = self.state.lock().expect("request queue mutex poisoned");
        Ok(state.pending.is_empty())
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        let state = self.state.lock().expect("request queue mutex poisoned");
        Ok(state.pending.is_empty() && state.in_flight.is_empty())
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        let state = self.state.lock().expect("request queue mutex poisoned");
        Ok(state.handled_count)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let state = self.state.lock().expect("request queue mutex poisoned");
        Ok(state.pending.len() as u64)
    }
}
