//! Dynamic concurrency, load-signal evaluation, and politeness limits.

mod aimd;
mod pool;
mod rate_limit;
mod signal;
mod signals;
mod snapshotter;
mod system_status;

pub use aimd::AimdController;
pub(crate) use pool::AttemptOutcomeKind;
pub use pool::{AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions};
pub use signal::{LoadSignal, LoadSnapshot};
pub use signals::{
    ClientLoadSignal, ClientLoadSignalHandle, CpuLoadSignal, CpuLoadSignalOptions,
    MemoryLoadSignal, MemoryLoadSignalOptions, TokioRuntimeLoadSignal,
    TokioRuntimeLoadSignalOptions,
};
pub use snapshotter::{Snapshotter, SnapshotterOptions};
pub use system_status::{ScaleDecision, SystemStatus, SystemStatusOptions};
