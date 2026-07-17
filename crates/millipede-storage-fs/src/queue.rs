//! Durable file-system request queue.

use crate::layout::{is_temporary_file, temporary_suffix};
use millipede_core::{
    request::{Request, RequestId},
    storage::{
        AddOptions, BatchAddHandle, Lease, LeaseId, ProcessedRequest, QueueOpInfo, ReclaimOptions,
        RequestQueue, RequestSource, StorageError, StorageResult,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

const LEASE_TTL: Duration = Duration::from_secs(180);
const STATE_VERSION: u8 = 1;

/// A durable FIFO request queue backed by atomic file replacements.
///
/// Request files, rather than `state.json`, are the source of truth. Leases are
/// deliberately process-local, so requests leased by a process that exits are
/// pending again when a new client rescans the queue. Writes use a temporary
/// file and same-directory rename, but do not call `fsync`; a machine-level
/// power loss can therefore lose writes that the operating system had not yet
/// flushed.
pub struct FsRequestQueue {
    name: String,
    path: PathBuf,
    requests_path: PathBuf,
    operations: Arc<RwLock<()>>,
    state: Mutex<QueueState>,
}

struct QueueState {
    pending: BTreeMap<i64, RequestId>,
    dedup: HashMap<String, (RequestId, bool)>,
    leases: HashMap<LeaseId, (RequestId, Instant)>,
    requests: HashMap<RequestId, StoredRequest>,
    handled_count: u64,
    next_order: i64,
    next_forefront_order: i64,
    next_lease_id: u64,
}

struct StoredRequest {
    request: Request,
    order_no: Option<i64>,
}

#[derive(Serialize)]
struct RequestEnvelope {
    id: String,
    url: String,
    #[serde(rename = "uniqueKey")]
    unique_key: String,
    method: String,
    #[serde(rename = "retryCount")]
    retry_count: u32,
    #[serde(rename = "orderNo")]
    order_no: Option<i64>,
    json: Request,
}

#[derive(Deserialize)]
struct ReadEnvelope {
    #[serde(rename = "orderNo")]
    order_no: Option<i64>,
    json: Request,
}

#[derive(Serialize, Deserialize)]
struct QueueCache {
    version: u8,
    #[serde(rename = "handledRequestCount")]
    handled_request_count: u64,
    #[serde(rename = "pendingRequestCount")]
    pending_request_count: u64,
    next_order: i64,
    next_forefront_order: i64,
}

impl fmt::Debug for FsRequestQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FsRequestQueue")
            .field("name", &self.name)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl FsRequestQueue {
    pub(crate) async fn open(
        name: String,
        path: PathBuf,
        operations: Arc<RwLock<()>>,
    ) -> StorageResult<Self> {
        let requests_path = path.join("requests");
        tokio::fs::create_dir_all(&requests_path).await?;
        let state = scan_queue(&name, &path, &requests_path).await?;
        let queue = Self {
            name,
            path,
            requests_path,
            operations,
            state: Mutex::new(state),
        };
        {
            let state = queue.state.lock().await;
            queue.write_cache_best_effort(&state).await;
        }
        Ok(queue)
    }

    pub(crate) async fn ensure_layout(&self) -> StorageResult<()> {
        tokio::fs::create_dir_all(&self.requests_path).await?;
        Ok(())
    }

    pub(crate) async fn reset(&self) {
        let mut state = self.state.lock().await;
        *state = QueueState::empty();
    }

    async fn write_request(&self, request: &Request, order_no: Option<i64>) -> StorageResult<()> {
        tokio::fs::create_dir_all(&self.requests_path).await?;
        let envelope = RequestEnvelope {
            id: request.id.to_string(),
            url: request.url.to_string(),
            unique_key: request.unique_key.clone(),
            method: request.method.as_str().to_owned(),
            retry_count: request.retry_count,
            order_no,
            json: request.clone(),
        };
        let destination = self
            .requests_path
            .join(format!("{}.json", request.id.as_str()));
        atomic_write(&destination, &serde_json::to_vec_pretty(&envelope)?).await
    }

    async fn write_cache_best_effort(&self, state: &QueueState) {
        let cache = QueueCache {
            version: STATE_VERSION,
            handled_request_count: state.handled_count,
            pending_request_count: state
                .requests
                .values()
                .filter(|request| request.order_no.is_some())
                .count() as u64,
            next_order: state.next_order,
            next_forefront_order: state.next_forefront_order,
        };
        let result = match serde_json::to_vec_pretty(&cache) {
            Ok(bytes) => atomic_write(&self.path.join("state.json"), &bytes).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            tracing::warn!(
                queue = %self.name,
                %error,
                "failed to refresh request queue state cache"
            );
        }
    }

    fn lease_request_id(state: &QueueState, lease_id: &LeaseId) -> StorageResult<RequestId> {
        state
            .leases
            .get(lease_id)
            .map(|(request_id, _)| request_id.clone())
            .ok_or_else(|| StorageError::LeaseNotFound {
                lease_id: lease_id.clone(),
            })
    }

    fn validate_unique_key(
        state: &QueueState,
        request_id: &RequestId,
        request: &Request,
    ) -> StorageResult<()> {
        if let Some((known_id, _)) = state.dedup.get(&request.unique_key) {
            if known_id != request_id {
                return Err(StorageError::Backend(anyhow::anyhow!(
                    "request lease changed unique_key to an existing queue key"
                )));
            }
        }
        Ok(())
    }

    fn replace_stored_request(
        state: &mut QueueState,
        request_id: &RequestId,
        request: Request,
        order_no: Option<i64>,
        handled: bool,
    ) {
        if let Some(previous) = state.requests.get(request_id) {
            if previous.request.unique_key != request.unique_key {
                state.dedup.remove(&previous.request.unique_key);
            }
        }
        state
            .dedup
            .insert(request.unique_key.clone(), (request_id.clone(), handled));
        state
            .requests
            .insert(request_id.clone(), StoredRequest { request, order_no });
    }

    fn reclaim_expired(state: &mut QueueState) {
        let now = Instant::now();
        let expired: Vec<_> = state
            .leases
            .iter()
            .filter(|(_, (_, expires_at))| *expires_at <= now)
            .map(|(lease_id, _)| lease_id.clone())
            .collect();
        for lease_id in expired {
            if let Some((request_id, _)) = state.leases.remove(&lease_id) {
                if let Some(order_no) = state
                    .requests
                    .get(&request_id)
                    .and_then(|request| request.order_no)
                {
                    state.pending.insert(order_no, request_id);
                }
            }
        }
    }

    async fn add_locked(
        &self,
        state: &mut QueueState,
        request: Request,
        opts: &AddOptions,
    ) -> StorageResult<QueueOpInfo> {
        let unique_key = request.unique_key.clone();
        if let Some((request_id, handled)) = state.dedup.get(&unique_key) {
            return Ok(ProcessedRequest {
                request_id: request_id.clone(),
                unique_key,
                was_already_present: true,
                was_already_handled: *handled,
            });
        }

        let request_id = request.id.clone();
        let order_no = state.take_order(opts.forefront)?;
        self.write_request(&request, Some(order_no)).await?;
        state.pending.insert(order_no, request_id.clone());
        state
            .dedup
            .insert(unique_key.clone(), (request_id.clone(), false));
        state.requests.insert(
            request_id.clone(),
            StoredRequest {
                request,
                order_no: Some(order_no),
            },
        );
        self.write_cache_best_effort(state).await;
        Ok(ProcessedRequest {
            request_id,
            unique_key,
            was_already_present: false,
            was_already_handled: false,
        })
    }

    async fn requeue(
        &self,
        lease: Lease,
        forefront: bool,
        increment_retry: bool,
    ) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let mut state = self.state.lock().await;
        let request_id = Self::lease_request_id(&state, &lease.lease_id)?;
        let mut request = lease.request;
        Self::validate_unique_key(&state, &request_id, &request)?;
        if increment_retry {
            request.retry_count += 1;
        }
        let order_no = state.take_order(forefront)?;
        self.write_request(&request, Some(order_no)).await?;
        state.leases.remove(&lease.lease_id);
        state.pending.insert(order_no, request_id.clone());
        Self::replace_stored_request(&mut state, &request_id, request, Some(order_no), false);
        self.write_cache_best_effort(&state).await;
        Ok(())
    }
}

impl QueueState {
    fn empty() -> Self {
        Self {
            pending: BTreeMap::new(),
            dedup: HashMap::new(),
            leases: HashMap::new(),
            requests: HashMap::new(),
            handled_count: 0,
            next_order: 1,
            next_forefront_order: -1,
            next_lease_id: 0,
        }
    }

    fn take_order(&mut self, forefront: bool) -> StorageResult<i64> {
        if forefront {
            let order_no = self.next_forefront_order;
            self.next_forefront_order = order_no.checked_sub(1).ok_or_else(|| {
                StorageError::Backend(anyhow::anyhow!("request queue forefront order exhausted"))
            })?;
            Ok(order_no)
        } else {
            let order_no = self.next_order;
            self.next_order = order_no.checked_add(1).ok_or_else(|| {
                StorageError::Backend(anyhow::anyhow!("request queue order exhausted"))
            })?;
            Ok(order_no)
        }
    }
}

#[async_trait::async_trait]
impl RequestQueue for FsRequestQueue {
    async fn add(&self, request: Request, opts: AddOptions) -> StorageResult<QueueOpInfo> {
        let _operation = self.operations.read().await;
        let mut state = self.state.lock().await;
        self.add_locked(&mut state, request, &opts).await
    }

    async fn add_batch(
        &self,
        requests: Vec<RequestSource>,
        opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        let _operation = self.operations.read().await;
        let mut state = self.state.lock().await;
        let mut processed = Vec::with_capacity(requests.len());
        for source in requests {
            let request = match source {
                RequestSource::Request(request) => request,
                _ => {
                    return Err(StorageError::Backend(anyhow::anyhow!(
                        "unsupported request source"
                    )));
                }
            };
            processed.push(self.add_locked(&mut state, request, &opts).await?);
        }
        Ok(BatchAddHandle::ready(processed))
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        let _operation = self.operations.read().await;
        let mut state = self.state.lock().await;
        Self::reclaim_expired(&mut state);
        let Some((_, request_id)) = state.pending.pop_first() else {
            return Ok(None);
        };
        let request = state
            .requests
            .get(&request_id)
            .expect("pending request has a stored request")
            .request
            .clone();
        let raw_lease_id = state.next_lease_id;
        state.next_lease_id = raw_lease_id.checked_add(1).ok_or_else(|| {
            StorageError::Backend(anyhow::anyhow!("request queue lease identifiers exhausted"))
        })?;
        let lease_id = LeaseId::new(raw_lease_id);
        let expires_at = Instant::now() + LEASE_TTL;
        state
            .leases
            .insert(lease_id.clone(), (request_id, expires_at));
        Ok(Some(Lease {
            request,
            lease_id,
            expires_at,
        }))
    }

    async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let mut state = self.state.lock().await;
        let request_id = Self::lease_request_id(&state, &lease.lease_id)?;
        Self::validate_unique_key(&state, &request_id, &lease.request)?;
        self.write_request(&lease.request, None).await?;
        state.leases.remove(&lease.lease_id);
        state.handled_count = state.handled_count.saturating_add(1);
        Self::replace_stored_request(&mut state, &request_id, lease.request, None, true);
        self.write_cache_best_effort(&state).await;
        Ok(())
    }

    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> StorageResult<()> {
        self.requeue(lease, opts.forefront, opts.increment_retry)
            .await
    }

    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let mut state = self.state.lock().await;
        let expired = state
            .leases
            .get(lease_id)
            .is_some_and(|(_, expires_at)| *expires_at <= Instant::now());
        if expired {
            if let Some((request_id, _)) = state.leases.remove(lease_id) {
                if let Some(order_no) = state
                    .requests
                    .get(&request_id)
                    .and_then(|request| request.order_no)
                {
                    state.pending.insert(order_no, request_id);
                }
            }
            return Err(StorageError::LeaseNotFound {
                lease_id: lease_id.clone(),
            });
        }
        let (_, expires_at) =
            state
                .leases
                .get_mut(lease_id)
                .ok_or_else(|| StorageError::LeaseNotFound {
                    lease_id: lease_id.clone(),
                })?;
        *expires_at += extend_by;
        Ok(())
    }

    async fn abandon(&self, lease: Lease) -> StorageResult<()> {
        self.requeue(lease, true, false).await
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        let _operation = self.operations.read().await;
        Ok(self.state.lock().await.pending.is_empty())
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        let _operation = self.operations.read().await;
        let state = self.state.lock().await;
        Ok(state.pending.is_empty() && state.leases.is_empty())
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        let _operation = self.operations.read().await;
        Ok(self.state.lock().await.handled_count)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let _operation = self.operations.read().await;
        Ok(self.state.lock().await.pending.len() as u64)
    }
}

async fn scan_queue(name: &str, path: &Path, requests_path: &Path) -> StorageResult<QueueState> {
    let mut entries = tokio::fs::read_dir(requests_path).await?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if is_temporary_file(file_name) || !file_name.ends_with(".json") {
            continue;
        }
        files.push(entry.path());
    }
    files.sort_unstable();

    let mut decoded = Vec::with_capacity(files.len());
    for file in files {
        let bytes = tokio::fs::read(&file).await?;
        match decode_request(&bytes) {
            Ok(request) => decoded.push(request),
            Err(error) => {
                tracing::warn!(
                    queue = name,
                    path = %file.display(),
                    %error,
                    "skipping unreadable request queue item"
                );
            }
        }
    }

    let max_normal = decoded
        .iter()
        .filter_map(|(_, order_no, _)| order_no.filter(|order_no| *order_no > 0))
        .max()
        .unwrap_or(0);
    let min_forefront = decoded
        .iter()
        .filter_map(|(_, order_no, _)| order_no.filter(|order_no| *order_no < 0))
        .min()
        .unwrap_or(0);
    let mut state = QueueState::empty();
    state.next_order = max_normal.saturating_add(1);
    state.next_forefront_order = if min_forefront < 0 {
        min_forefront.saturating_sub(1)
    } else {
        -1
    };

    for (request, disk_order_no, bare) in decoded {
        let request_id = request.id.clone();
        let order_no = if bare {
            Some(state.take_order(false)?)
        } else if let Some(order_no) = disk_order_no {
            if state.pending.contains_key(&order_no) {
                Some(state.take_order(false)?)
            } else {
                Some(order_no)
            }
        } else {
            None
        };
        let handled = order_no.is_none();
        if let Some(order_no) = order_no {
            state.pending.insert(order_no, request_id.clone());
        } else {
            state.handled_count = state.handled_count.saturating_add(1);
        }
        state
            .dedup
            .insert(request.unique_key.clone(), (request_id.clone(), handled));
        state
            .requests
            .insert(request_id, StoredRequest { request, order_no });
    }

    compare_cache(name, path, &state).await;
    Ok(state)
}

fn decode_request(bytes: &[u8]) -> Result<(Request, Option<i64>, bool), serde_json::Error> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if value.get("json").is_some() {
        let envelope: ReadEnvelope = serde_json::from_value(value)?;
        Ok((envelope.json, envelope.order_no, false))
    } else {
        let request = serde_json::from_value(value)?;
        Ok((request, None, true))
    }
}

async fn compare_cache(name: &str, path: &Path, state: &QueueState) {
    let bytes = match tokio::fs::read(path.join("state.json")).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(queue = name, %error, "failed to read request queue state cache");
            return;
        }
    };
    let cache: QueueCache = match serde_json::from_slice::<QueueCache>(&bytes) {
        Ok(cache) if cache.version == STATE_VERSION => cache,
        Ok(_) => {
            tracing::warn!(
                queue = name,
                "ignoring unsupported request queue state cache"
            );
            return;
        }
        Err(error) => {
            tracing::warn!(queue = name, %error, "ignoring unreadable request queue state cache");
            return;
        }
    };
    let pending_count = state
        .requests
        .values()
        .filter(|request| request.order_no.is_some())
        .count() as u64;
    if cache.handled_request_count != state.handled_count
        || cache.pending_request_count != pending_count
    {
        tracing::warn!(
            queue = name,
            cached_handled = cache.handled_request_count,
            scanned_handled = state.handled_count,
            cached_pending = cache.pending_request_count,
            scanned_pending = pending_count,
            "request queue state cache disagrees with request files"
        );
    }
}

async fn atomic_write(destination: &Path, bytes: &[u8]) -> StorageResult<()> {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| StorageError::Backend(anyhow::anyhow!("invalid queue file path")))?;
    let temporary = destination.with_file_name(format!("{file_name}.{}", temporary_suffix()));
    tokio::fs::write(&temporary, bytes).await?;
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(())
}
