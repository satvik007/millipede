#![doc = include_str!("../README.md")]

/// Browser-specific errors and crawl error classification.
pub mod error;
/// Browser lifecycle hooks.
pub mod hooks;
/// Provider-erased browser page operations and page options.
pub mod page;
/// Browser pool and provider-erased RAII page handles.
pub mod pool;
/// Concrete browser provider integration points.
pub mod provider;

pub use error::BrowserError;
pub use hooks::{BrowserHooks, PageClosedHook, PageHook, PagePrepHook, PreLaunchHook};
pub use page::{
    BrowserPage, BrowserResponse, GotoOptions, PageId, PageOpts, ScreenshotOptions, WaitUntil,
};
pub use pool::{BrowserPool, BrowserPoolOptions, PageHandle};
pub use provider::{BrowserProvider, LaunchContext};

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::{
        BrowserError, BrowserHooks, BrowserPage, BrowserPool, BrowserPoolOptions, BrowserProvider,
        BrowserResponse, GotoOptions, LaunchContext, PageClosedHook, PageHandle, PageHook, PageId,
        PageOpts, PagePrepHook, PreLaunchHook, ScreenshotOptions, WaitUntil,
    };
}
