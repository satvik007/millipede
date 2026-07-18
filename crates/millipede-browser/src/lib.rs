#![doc = include_str!("../README.md")]

/// HTTP-attempt detector interfaces for smart browser promotion.
pub mod detect;
/// Browser-specific errors and crawl error classification.
pub mod error;
/// Browser lifecycle hooks.
pub mod hooks;
/// Browser-backed crawler kind and handler context.
pub mod kind;
/// Browser pre- and post-navigation hook contexts.
pub mod nav;
/// Provider-erased browser page operations and page options.
pub mod page;
/// Browser pool and provider-erased RAII page handles.
pub mod pool;
/// Concrete browser provider integration points.
pub mod provider;
/// HTTP-first crawler kind with selective browser promotion.
pub mod smart;

pub use detect::{
    BrowserPromotionDetector, DefaultPromotionDetector, HttpAttemptSnapshot, PromotionReason,
};
pub use error::BrowserError;
pub use hooks::{BrowserHooks, PageClosedHook, PageHook, PagePrepHook, PreLaunchHook};
pub use kind::{BrowserContext, BrowserCrawler, BrowserKind, BrowserKindBuilder};
pub use nav::{
    BrowserPostHookCtx, BrowserPostNavigationHook, BrowserPreHookCtx, BrowserPreNavigationHook,
};
pub use page::{
    BrowserPage, BrowserResponse, GotoOptions, PageId, PageOptions, ScreenshotOptions, WaitUntil,
};
pub use pool::{BrowserPool, BrowserPoolOptions, PageHandle};
pub use provider::{BrowserProvider, LaunchContext};
pub use smart::{SmartContext, SmartCrawler, SmartKind, SmartKindBuilder};

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::{
        BrowserContext, BrowserCrawler, BrowserError, BrowserHooks, BrowserKind,
        BrowserKindBuilder, BrowserPage, BrowserPool, BrowserPoolOptions, BrowserPostHookCtx,
        BrowserPostNavigationHook, BrowserPreHookCtx, BrowserPreNavigationHook,
        BrowserPromotionDetector, BrowserProvider, BrowserResponse, DefaultPromotionDetector,
        GotoOptions, HttpAttemptSnapshot, LaunchContext, PageClosedHook, PageHandle, PageHook,
        PageId, PageOptions, PagePrepHook, PreLaunchHook, PromotionReason, ScreenshotOptions,
        SmartContext, SmartCrawler, SmartKind, SmartKindBuilder, WaitUntil,
    };
}
