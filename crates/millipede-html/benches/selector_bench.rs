//! Selector/link-extraction benchmark (INTERFACE.md §22 Q2). Populated later in Phase 5.
//! Compile-time guards for the DOM storage needed by `HtmlContext`.

use std::sync::{Arc, Mutex};

fn assert_send<T: Send>() {}
fn assert_context_bounds<T: Send + Clone + 'static>() {}

fn main() {
    // The `atomic` feature makes `Html` movable between threads, but not shareable between them:
    // its element cache contains a non-`Sync` `OnceCell`.
    assert_send::<scraper::Html>();

    // `CrawlerKind::Context` requires `Send + Clone + 'static`. A bare `Arc<Html>` is not `Send`
    // because that would also require `Html: Sync`, so shared context storage must synchronize it.
    assert_context_bounds::<Arc<Mutex<scraper::Html>>>();
}
