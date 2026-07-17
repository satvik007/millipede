#![doc = include_str!("../README.md")]

mod browser_fingerprint;
mod header_generator;

pub use browser_fingerprint::BrowserFingerprintGenerator;
pub use header_generator::{HeaderGenerator, HeaderProfile};

/// Commonly used fingerprint generation types.
pub mod prelude {
    pub use crate::{BrowserFingerprintGenerator, HeaderGenerator, HeaderProfile};
}
