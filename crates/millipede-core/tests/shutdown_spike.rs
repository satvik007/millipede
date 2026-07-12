//! Executable spike backing ADR-0004: shutdown semantics of JoinSet vs FuturesUnordered.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

type IdentifiedJoin =
    Pin<Box<dyn Future<Output = (usize, Result<usize, JoinError>)> + Send + 'static>>;

#[tokio::test]
async fn joinset_shutdown_aborts_all_inflight() {
    let completed = Arc::new((0..4).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
    let mut set = JoinSet::new();

    for id in 0..4 {
        let completed = Arc::clone(&completed);
        set.spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            completed[id].store(true, Ordering::SeqCst);
        });
    }

    let started = Instant::now();
    set.shutdown().await;

    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(completed.iter().all(|flag| !flag.load(Ordering::SeqCst)));
}

#[tokio::test]
async fn futures_unordered_supports_drain_and_abort() {
    let drain = CancellationToken::new();
    let mut draining = FuturesUnordered::<IdentifiedJoin>::new();
    for id in 0..4 {
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            id
        });
        draining.push(Box::pin(async move { (id, handle.await) }));
    }

    drain.cancel();
    assert!(drain.is_cancelled());
    let mut completed = Vec::new();
    while let Some((id, result)) = draining.next().await {
        assert_eq!(result.expect("drained task should complete normally"), id);
        completed.push(id);
    }
    completed.sort_unstable();
    assert_eq!(completed, vec![0, 1, 2, 3]);

    let cancel = CancellationToken::new();
    let mut aborting = FuturesUnordered::<IdentifiedJoin>::new();
    let mut abort_handles = Vec::new();
    for id in 0..4 {
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            id
        });
        abort_handles.push(handle.abort_handle());
        aborting.push(Box::pin(async move { (id, handle.await) }));
    }

    cancel.cancel();
    assert!(cancel.is_cancelled());
    for handle in abort_handles {
        handle.abort();
    }
    let mut cancelled = 0;
    while let Some((_id, result)) = aborting.next().await {
        let error = result.expect_err("aborted task should return a JoinError");
        assert!(error.is_cancelled());
        cancelled += 1;
    }
    assert_eq!(cancelled, 4);
}

#[tokio::test]
async fn notify_wakeup_is_not_lost_when_registered_before_check() {
    let notify = Arc::new(Notify::new());
    let condition = Arc::new(AtomicBool::new(false));
    let notified = notify.notified();

    let notifier = {
        let notify = Arc::clone(&notify);
        let condition = Arc::clone(&condition);
        tokio::spawn(async move {
            condition.store(true, Ordering::SeqCst);
            notify.notify_waiters();
        })
    };
    notifier.await.expect("notifier should not panic");

    assert!(condition.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_millis(100), notified)
        .await
        .expect("a waiter created before notify_waiters should wake");

    let missed_notify = Notify::new();
    missed_notify.notify_waiters();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), missed_notify.notified())
            .await
            .is_err(),
        "notify_waiters should not wake a future created later"
    );
}

#[tokio::test]
async fn cancelled_token_arm_is_hot_after_cancellation() {
    let drain = CancellationToken::new();
    drain.cancel();

    let observations = AtomicUsize::new(0);
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_millis(25), drain.cancelled())
            .await
            .expect("cancelled() should remain immediately ready");
        observations.fetch_add(1, Ordering::SeqCst);
    }

    assert_eq!(observations.load(Ordering::SeqCst), 2);
}
