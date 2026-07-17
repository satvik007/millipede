use millipede_core::storage::{StorageError, StorageResult};
use std::{
    collections::hash_map::RandomState,
    hash::BuildHasher,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) const DATASETS: &str = "datasets";
pub(crate) const KEY_VALUE_STORES: &str = "key_value_stores";
pub(crate) const REQUEST_QUEUES: &str = "request_queues";

pub(crate) fn store_name(name: Option<&str>) -> StorageResult<String> {
    let name = name.unwrap_or("default");
    validate_component(name, "storage name")?;
    Ok(name.to_owned())
}

pub(crate) fn validate_key(key: &str) -> StorageResult<()> {
    validate_component(key, "key")
}

pub(crate) fn store_path(root: &Path, category: &str, name: &str) -> PathBuf {
    root.join(category).join(name)
}

pub(crate) fn temporary_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let random = RandomState::new().hash_one((
        std::process::id(),
        std::thread::current().id(),
        nanos,
        sequence,
    ));
    format!("tmp-{random:016x}")
}

pub(crate) fn is_temporary_file(name: &str) -> bool {
    let Some((destination, suffix)) = name.rsplit_once(".tmp-") else {
        return false;
    };
    let Some((key, extension)) = destination.rsplit_once('.') else {
        return false;
    };
    if key.is_empty() || extension.is_empty() {
        return false;
    }
    suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn validate_component(value: &str, kind: &str) -> StorageResult<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || value.contains('\0')
    {
        return Err(StorageError::Backend(anyhow::anyhow!(
            "invalid {kind} {value:?}: must be non-empty and contain no '/', '\\', '..', or NUL"
        )));
    }
    Ok(())
}
