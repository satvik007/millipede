# millipede-browser

BrowserCrawler core, BrowserProvider trait, and BrowserPool for the Millipede web crawler.

[![crates.io](https://img.shields.io/crates/v/millipede-browser.svg)](https://crates.io/crates/millipede-browser) [![docs.rs](https://docs.rs/millipede-browser/badge.svg)](https://docs.rs/millipede-browser) [![license](https://img.shields.io/crates/l/millipede-browser.svg)](https://github.com/satvik007/millipede#license)

This crate defines [Millipede](https://github.com/satvik007/millipede)'s provider-neutral browser pages, lifecycle hooks, crawler kind, and browser pool. The concrete Chromium provider comes from [`millipede-browser-chromiumoxide`](https://crates.io/crates/millipede-browser-chromiumoxide).

## Installation

```toml
[dependencies]
millipede-browser = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate instead.

## Example

Pool policy and hooks can be prepared without choosing a provider:

```rust,no_run
use millipede_browser::{BrowserHooks, BrowserPoolOptions};

let hooks = BrowserHooks::defaults()
    .with_launch_args(vec!["--disable-background-networking".to_owned()]);
let options = BrowserPoolOptions::<()>::default()
    .with_max_open_pages_per_browser(4)
    .with_retire_browser_after_page_count(100)
    .with_hooks(hooks);

assert_eq!(options.max_open_pages_per_browser, 4);
assert_eq!(options.retire_browser_after_page_count, 100);
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for browser pools, hooks, smart crawling, and fingerprinting guidance.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
