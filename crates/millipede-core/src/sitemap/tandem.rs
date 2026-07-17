use std::{sync::Arc, time::Duration};

use crate::{
    request::Request,
    storage::{
        AddOptions, BatchAddHandle, Lease, LeaseId, QueueOpInfo, ReclaimOptions, RequestQueue,
        RequestSource, StorageError, StorageResult,
    },
};

use super::SitemapRequestList;

/// A request queue that lazily feeds sitemap entries into another queue.
///
/// The wrapper lets the crawler consume already-queued work first, then drains the sitemap in
/// bounded batches whenever the inner queue becomes empty.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use millipede_core::{
///     sitemap::{RequestQueueWithSitemap, SitemapRequestList},
///     storage::RequestQueue,
/// };
///
/// # fn wrap(inner: Arc<dyn RequestQueue>, list: SitemapRequestList) {
/// let queue: Arc<dyn RequestQueue> =
///     Arc::new(RequestQueueWithSitemap::new(inner, list).batch_size(64));
/// # drop(queue);
/// # }
/// ```
pub struct RequestQueueWithSitemap {
    inner: Arc<dyn RequestQueue>,
    list: SitemapRequestList,
    batch: usize,
    drain: tokio::sync::Mutex<DrainState>,
}

#[derive(Default)]
struct DrainState {
    pending_add: Option<Request>,
}

impl RequestQueueWithSitemap {
    /// Wraps `inner` with a sitemap source, draining at most 32 entries per pass.
    pub fn new(inner: Arc<dyn RequestQueue>, list: SitemapRequestList) -> Self {
        Self {
            inner,
            list,
            batch: 32,
            drain: tokio::sync::Mutex::new(DrainState::default()),
        }
    }

    /// Sets the maximum number of sitemap entries drained per pass.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch = n.max(1);
        self
    }

    async fn persist_after_batch(&self) {
        if let Err(error) = self.list.persist().await {
            tracing::warn!(%error, "sitemap tandem checkpoint failed");
        }
    }
}

#[async_trait::async_trait]
impl RequestQueue for RequestQueueWithSitemap {
    async fn add(&self, req: Request, opts: AddOptions) -> StorageResult<QueueOpInfo> {
        self.inner.add(req, opts).await
    }

    async fn add_batch(
        &self,
        reqs: Vec<RequestSource>,
        opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        self.inner.add_batch(reqs, opts).await
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        let mut drain = self.drain.lock().await;
        loop {
            if !self.inner.is_empty().await? {
                return self.inner.fetch_next().await;
            }

            if drain.pending_add.is_none() && self.list.is_finished().await {
                return self.inner.fetch_next().await;
            }

            for _ in 0..self.batch {
                if drain.pending_add.is_none() {
                    drain.pending_add = match self.list.fetch_next_for_tandem().await {
                        Ok(request) => request,
                        Err(error) => {
                            self.persist_after_batch().await;
                            return Err(StorageError::Backend(anyhow::Error::new(error)));
                        }
                    };
                    if drain.pending_add.is_none() {
                        break;
                    }
                }

                let request = drain
                    .pending_add
                    .as_ref()
                    .expect("pending sitemap request exists")
                    .clone();
                self.inner.add(request, AddOptions::default()).await?;
                drain.pending_add = None;
            }
            self.persist_after_batch().await;

            if drain.pending_add.is_none() && self.list.is_finished().await {
                return self.inner.fetch_next().await;
            }
        }
    }

    async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
        self.inner.mark_handled(lease).await
    }

    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> StorageResult<()> {
        self.inner.reclaim(lease, opts).await
    }

    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
        self.inner.renew(lease_id, extend_by).await
    }

    async fn abandon(&self, lease: Lease) -> StorageResult<()> {
        self.inner.abandon(lease).await
    }

    /// Returns `false` while sitemap entries remain undrained, preventing premature engine
    /// termination; once the sitemap is drained, delegates to the inner queue.
    async fn is_empty(&self) -> StorageResult<bool> {
        let drain = self.drain.lock().await;
        if drain.pending_add.is_some() {
            return Ok(false);
        }
        if !self.list.is_finished().await {
            return Ok(false);
        }
        self.inner.is_empty().await
    }

    /// Returns `false` while sitemap entries remain undrained, preventing premature engine
    /// termination; once the sitemap is drained, delegates to the inner queue.
    async fn is_finished(&self) -> StorageResult<bool> {
        let drain = self.drain.lock().await;
        if drain.pending_add.is_some() {
            return Ok(false);
        }
        if !self.list.is_finished().await {
            return Ok(false);
        }
        self.inner.is_finished().await
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        self.inner.handled_count().await
    }

    /// Returns only the inner queue's pending count. Undrained sitemap entries are not included
    /// because their count is unknown until the sitemap is streamed.
    async fn pending_count(&self) -> StorageResult<u64> {
        self.inner.pending_count().await
    }
}
