//! Dynamic concurrency, load-signal evaluation, and politeness limits.

mod aimd;
mod pool;
mod rate_limit;
mod signal;
mod snapshotter;
mod system_status;

pub use aimd::AimdController;
#[allow(unused_imports)]
pub(crate) use pool::{AttemptOutcomeKind, apply_scale_decision};
pub use pool::{AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions};
pub use signal::{LoadSignal, LoadSnapshot};
pub use snapshotter::{Snapshotter, SnapshotterOptions};
pub use system_status::{ScaleDecision, SystemStatus, SystemStatusOptions};
