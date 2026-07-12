//! Crawler lifecycle events and broadcast delivery.

use crate::request::Request;
use std::{sync::Arc, time::Duration};

/// The terminal result of processing one request.
#[derive(Debug, Clone)]
pub struct HandledRequest {
    /// The request that was processed.
    pub request: Arc<Request>,
    /// The final URL after redirects, when navigation occurred.
    pub loaded_url: Option<url::Url>,
    /// The request's terminal state.
    pub outcome: RequestFinalState,
    /// The response status, when a response was received.
    pub response_status: Option<http::StatusCode>,
    /// The number of retry attempts made.
    pub retry_count: u32,
    /// The total processing duration.
    pub duration: Duration,
}

/// A request's terminal processing state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFinalState {
    /// The request completed successfully.
    Succeeded,
    /// The request permanently failed.
    Failed,
    /// The request was deliberately skipped.
    Skipped,
}

/// A snapshot of host resource usage.
///
/// This is a placeholder data carrier; system-information collection is wired in Phase 4.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SystemSnapshot {
    /// When the snapshot was collected, if known.
    pub created_at: Option<time::OffsetDateTime>,
    /// The fraction of CPU capacity in use, if known.
    pub cpu_used_ratio: Option<f32>,
    /// The number of memory bytes in use, if known.
    pub memory_used_bytes: Option<u64>,
}

/// A control-plane event emitted during a crawler run.
#[derive(Debug, Clone)]
pub enum CrawlerEvent {
    /// Requests that subscribers persist crawler state.
    PersistState {
        /// Whether persistence is occurring before migration.
        is_migrating: bool,
    },
    /// Reports a terminal request snapshot.
    RequestFinished(HandledRequest),
    /// Reports a request processing failure.
    RequestFailed {
        /// The request that failed.
        request: Arc<Request>,
        /// A displayable error description.
        error: String,
    },
    /// Reports current host resource usage.
    SystemInfo(SystemSnapshot),
    /// Indicates that immediate cancellation has begun.
    Aborting,
    /// Indicates that the crawler is exiting.
    Exiting,
}

/// A receiver for crawler events.
pub type EventStream = tokio::sync::broadcast::Receiver<CrawlerEvent>;

/// A broadcast channel for crawler control-plane events.
#[derive(Debug, Clone)]
pub struct EventBus {
    tx: tokio::sync::broadcast::Sender<CrawlerEvent>,
}

impl EventBus {
    /// Creates an event bus with the specified retained-message capacity.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "event bus capacity must be greater than zero");
        let (tx, _) = tokio::sync::broadcast::channel(capacity);
        Self { tx }
    }

    /// Creates a subscriber that receives events emitted after subscription.
    pub fn subscribe(&self) -> EventStream {
        self.tx.subscribe()
    }

    /// Broadcasts an event to current subscribers.
    ///
    /// The send error produced when no subscribers exist is intentionally ignored.
    pub fn emit(&self, event: CrawlerEvent) {
        let _ = self.tx.send(event);
    }

    /// Returns the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emit_without_subscribers_does_not_panic() {
        EventBus::default().emit(CrawlerEvent::Exiting);
    }

    #[tokio::test]
    async fn subscriber_receives_emitted_event() {
        let bus = EventBus::default();
        let mut subscriber = bus.subscribe();
        bus.emit(CrawlerEvent::Aborting);
        assert!(matches!(
            subscriber.recv().await,
            Ok(CrawlerEvent::Aborting)
        ));
    }

    #[tokio::test]
    async fn all_subscribers_receive_the_same_event() {
        let bus = EventBus::default();
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();
        bus.emit(CrawlerEvent::PersistState { is_migrating: true });
        assert!(matches!(
            first.recv().await,
            Ok(CrawlerEvent::PersistState { is_migrating: true })
        ));
        assert!(matches!(
            second.recv().await,
            Ok(CrawlerEvent::PersistState { is_migrating: true })
        ));
    }

    #[tokio::test]
    async fn late_subscriber_does_not_receive_earlier_event() {
        let bus = EventBus::default();
        let mut existing = bus.subscribe();
        bus.emit(CrawlerEvent::Exiting);
        let mut late = bus.subscribe();
        assert!(matches!(existing.recv().await, Ok(CrawlerEvent::Exiting)));
        assert!(matches!(
            late.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }
}
