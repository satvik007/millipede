#![doc = include_str!("../README.md")]

mod cookie;
pub mod discovery;
pub mod launch;
pub mod page;
pub mod provider;

pub use discovery::find_browser;
pub use launch::ChromiumLaunchOptions;
pub use page::ChromiumPage;
pub use provider::{ChromiumBrowser, ChromiumoxideProvider};

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::{
        ChromiumBrowser, ChromiumLaunchOptions, ChromiumPage, ChromiumoxideProvider, find_browser,
    };
}
