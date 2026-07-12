//! Statistics persistence integration coverage.

use millipede_core::{
    statistics::{STATISTICS_PERSIST_KEY, StatisticsHandle},
    storage::KeyValueStore,
};
use millipede_storage_memory::MemoryKeyValueStore;
use std::time::Duration;

#[tokio::test]
async fn statistics_persistence_round_trips_losslessly() {
    let kvs = MemoryKeyValueStore::new("statistics");
    let original = StatisticsHandle::new();
    original.record_finished(Duration::new(1, 234_567_891), Some(201), 1);
    original.record_failed(Duration::from_nanos(999_999_999), "non-retryable: exact", 2);
    original.record_retry("retryable: exact");
    original.mark_run_started();
    original.mark_run_stopped();
    original.persist(&kvs).await.unwrap();

    let entry = kvs
        .get_bytes(STATISTICS_PERSIST_KEY)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.content_type, "application/json");

    let expected = original.snapshot();
    let restored = StatisticsHandle::new();
    assert!(restored.restore(&kvs).await.unwrap());
    let actual = restored.snapshot();

    assert_eq!(actual.requests_finished, expected.requests_finished);
    assert_eq!(actual.requests_failed, expected.requests_failed);
    assert_eq!(actual.requests_retries, expected.requests_retries);
    assert_eq!(actual.requests_finished_per_minute, 0.0);
    assert_eq!(actual.requests_failed_per_minute, 0.0);
    assert_eq!(actual.request_avg_duration, expected.request_avg_duration);
    assert_eq!(actual.request_min_duration, expected.request_min_duration);
    assert_eq!(actual.request_max_duration, expected.request_max_duration);
    assert_eq!(actual.status_codes, expected.status_codes);
    assert_eq!(actual.crawler_runtime, expected.crawler_runtime);
    assert_eq!(actual.retry_histogram, expected.retry_histogram);
    assert_eq!(actual.errors, expected.errors);
    assert_eq!(actual.retry_errors, expected.retry_errors);
}
