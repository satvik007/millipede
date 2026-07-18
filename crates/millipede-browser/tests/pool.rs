#![allow(missing_docs)]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use millipede_browser::{
    BrowserError, BrowserHooks, BrowserPage, BrowserPool, BrowserPoolOptions, BrowserProvider,
    BrowserResponse, GotoOptions, LaunchContext, PageHandle, PageOptions, ScreenshotOptions,
};
use millipede_core::{
    cookies::Cookie,
    proxy::ProxyConfiguration,
    session::{Session, SessionConfig},
};

#[derive(Default)]
struct FakeStats {
    launches: usize,
    page_create_attempts: usize,
    page_close_attempts: usize,
    pages_created: usize,
    pages_closed: Vec<u64>,
    browsers_closed: usize,
    open_pages: i64,
    launch_contexts: Vec<(Option<String>, Vec<String>)>,
}

#[derive(Clone)]
struct FakeProvider {
    stats: Arc<Mutex<FakeStats>>,
    hang_goto: bool,
    initial_cookies: Arc<Vec<Cookie>>,
    set_cookie_calls: Arc<Mutex<Vec<Vec<Cookie>>>>,
    block_next_launch: Arc<AtomicBool>,
    block_next_page: Arc<AtomicBool>,
    block_next_close: Arc<AtomicBool>,
    launch_release: Arc<tokio::sync::Notify>,
    page_release: Arc<tokio::sync::Notify>,
    close_release: Arc<tokio::sync::Notify>,
}

impl FakeProvider {
    fn new() -> Self {
        Self {
            stats: Arc::new(Mutex::new(FakeStats::default())),
            hang_goto: false,
            initial_cookies: Arc::new(Vec::new()),
            set_cookie_calls: Arc::new(Mutex::new(Vec::new())),
            block_next_launch: Arc::new(AtomicBool::new(false)),
            block_next_page: Arc::new(AtomicBool::new(false)),
            block_next_close: Arc::new(AtomicBool::new(false)),
            launch_release: Arc::new(tokio::sync::Notify::new()),
            page_release: Arc::new(tokio::sync::Notify::new()),
            close_release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn hanging() -> Self {
        Self {
            hang_goto: true,
            ..Self::new()
        }
    }

    fn with_cookies(cookies: Vec<Cookie>) -> Self {
        Self {
            initial_cookies: Arc::new(cookies),
            ..Self::new()
        }
    }

    fn block_next_launch(&self) {
        self.block_next_launch.store(true, Ordering::SeqCst);
    }

    fn block_next_page(&self) {
        self.block_next_page.store(true, Ordering::SeqCst);
    }

    fn block_next_close(&self) {
        self.block_next_close.store(true, Ordering::SeqCst);
    }
}

struct FakeBrowser {
    id: u64,
}

#[derive(Clone)]
struct FakePage {
    serial: u64,
    stats: Arc<Mutex<FakeStats>>,
    hang_goto: bool,
    cookies: Arc<Mutex<Vec<Cookie>>>,
    set_cookie_calls: Arc<Mutex<Vec<Vec<Cookie>>>>,
}

#[async_trait::async_trait]
impl BrowserPage for FakePage {
    async fn goto(
        &self,
        _url: &url::Url,
        _opts: GotoOptions,
    ) -> Result<Option<BrowserResponse>, BrowserError> {
        if self.hang_goto {
            futures_util::future::pending::<()>().await;
        }
        Ok(Some(BrowserResponse::default()))
    }

    async fn content(&self) -> Result<String, BrowserError> {
        Ok("<html></html>".to_owned())
    }

    async fn evaluate_js(&self, _script: &str) -> Result<serde_json::Value, BrowserError> {
        Ok(serde_json::Value::Null)
    }

    async fn evaluate_anchors(
        &self,
        _selector: Option<&str>,
    ) -> Result<Vec<url::Url>, BrowserError> {
        Ok(Vec::new())
    }

    async fn cookies(&self) -> Result<Vec<Cookie>, BrowserError> {
        Ok(self
            .cookies
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone())
    }

    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<(), BrowserError> {
        self.set_cookie_calls
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(cookies.to_vec());
        let mut stored = self
            .cookies
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for cookie in cookies {
            stored.retain(|current| {
                current.name != cookie.name
                    || current.domain != cookie.domain
                    || current.path != cookie.path
            });
            stored.push(cookie.clone());
        }
        Ok(())
    }

    async fn set_extra_headers(&self, _headers: &http::HeaderMap) -> Result<(), BrowserError> {
        Ok(())
    }

    async fn wait_for_selector(
        &self,
        _selector: &str,
        _timeout: Duration,
    ) -> Result<(), BrowserError> {
        Ok(())
    }

    async fn click(&self, _selector: &str) -> Result<(), BrowserError> {
        Ok(())
    }

    async fn screenshot(&self, _opts: ScreenshotOptions) -> Result<bytes::Bytes, BrowserError> {
        Ok(bytes::Bytes::new())
    }
}

#[async_trait::async_trait]
impl BrowserProvider for FakeProvider {
    type Browser = FakeBrowser;
    type Page = FakePage;
    type LaunchOptions = ();

    async fn launch(
        &self,
        _opts: Self::LaunchOptions,
        ctx: &LaunchContext,
    ) -> Result<Self::Browser, BrowserError> {
        let id = {
            let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
            stats.launches += 1;
            stats.launch_contexts.push((
                ctx.proxy.as_ref().map(|proxy| proxy.url.to_string()),
                ctx.extra_args.clone(),
            ));
            stats.launches as u64
        };
        if self.block_next_launch.swap(false, Ordering::SeqCst) {
            self.launch_release.notified().await;
        }
        Ok(FakeBrowser { id })
    }

    async fn new_page(&self, browser: &Self::Browser) -> Result<Self::Page, BrowserError> {
        let _browser_id = browser.id;
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .page_create_attempts += 1;
        if self.block_next_page.swap(false, Ordering::SeqCst) {
            self.page_release.notified().await;
        }
        let serial = {
            let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
            stats.pages_created += 1;
            stats.open_pages += 1;
            stats.pages_created as u64
        };
        Ok(FakePage {
            serial,
            stats: Arc::clone(&self.stats),
            hang_goto: self.hang_goto,
            cookies: Arc::new(Mutex::new((*self.initial_cookies).clone())),
            set_cookie_calls: Arc::clone(&self.set_cookie_calls),
        })
    }

    async fn close_page(&self, page: Self::Page) -> Result<(), BrowserError> {
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .page_close_attempts += 1;
        if self.block_next_close.swap(false, Ordering::SeqCst) {
            self.close_release.notified().await;
        }
        let mut stats = page.stats.lock().unwrap_or_else(|error| error.into_inner());
        stats.pages_closed.push(page.serial);
        stats.open_pages -= 1;
        Ok(())
    }

    async fn close_browser(&self, _browser: Self::Browser) -> Result<(), BrowserError> {
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .browsers_closed += 1;
        Ok(())
    }
}

fn stats(provider: &FakeProvider) -> std::sync::MutexGuard<'_, FakeStats> {
    provider
        .stats
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

#[tokio::test]
async fn max_open_pages_per_browser_spills_to_second_browser() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new().with_max_open_pages_per_browser(2);
        let pool = BrowserPool::new(provider.clone(), options);
        let _p1 = pool.new_page(PageOptions::new()).await.unwrap();
        let _p2 = pool.new_page(PageOptions::new()).await.unwrap();
        let _p3 = pool.new_page(PageOptions::new()).await.unwrap();
        assert_eq!(stats(&provider).launches, 2);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retire_browser_after_page_count_launches_replacement_and_closes_retiree() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new()
            .with_retire_browser_after_page_count(2)
            .with_max_open_pages_per_browser(10);
        let pool = BrowserPool::new(provider.clone(), options);
        let p1 = pool.new_page(PageOptions::new()).await.unwrap();
        let p2 = pool.new_page(PageOptions::new()).await.unwrap();
        p1.close().await.unwrap();
        p2.close().await.unwrap();
        let p3 = pool.new_page(PageOptions::new()).await.unwrap();
        {
            let stats = stats(&provider);
            assert_eq!(stats.launches, 2);
            assert_eq!(stats.browsers_closed, 1);
        }
        p3.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn max_browsers_waits_then_wakes_on_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new()
            .with_max_browsers(Some(1))
            .with_max_open_pages_per_browser(1);
        let pool = BrowserPool::new(provider, options);
        let p1 = pool.new_page(PageOptions::new()).await.unwrap();
        let waiting_pool = pool.clone();
        let mut waiting =
            tokio::spawn(async move { waiting_pool.new_page(PageOptions::new()).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut waiting)
                .await
                .is_err()
        );
        p1.close().await.unwrap();
        let p2 = waiting.await.unwrap().unwrap();
        p2.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn page_acquire_timeout_errors_when_capacity_never_frees() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new()
            .with_max_browsers(Some(1))
            .with_max_open_pages_per_browser(1)
            .with_page_acquire_timeout(Duration::from_millis(200));
        let pool = BrowserPool::new(provider, options);
        let p1 = pool.new_page(PageOptions::new()).await.unwrap();
        let error = pool.new_page(PageOptions::new()).await.unwrap_err();
        assert!(matches!(&error, BrowserError::PageCreate(_)));
        assert!(error.classify().is_retryable());
        p1.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn launch_timeout_rolls_back_placeholder_capacity() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        provider.block_next_launch();
        let options = BrowserPoolOptions::new()
            .with_max_browsers(Some(1))
            .with_max_open_pages_per_browser(1)
            .with_page_acquire_timeout(Duration::from_millis(100));
        let pool = BrowserPool::new(provider.clone(), options);

        assert!(matches!(
            pool.new_page(PageOptions::new()).await,
            Err(BrowserError::PageCreate(_))
        ));
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        assert_eq!(stats(&provider).launches, 2);
        page.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn abort_during_page_creation_rolls_back_page_reservation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new()
            .with_max_browsers(Some(1))
            .with_max_open_pages_per_browser(1);
        let pool = BrowserPool::new(provider.clone(), options);
        let first = pool.new_page(PageOptions::new()).await.unwrap();
        first.close().await.unwrap();

        provider.block_next_page();
        let task_pool = pool.clone();
        let task = tokio::spawn(async move { task_pool.new_page(PageOptions::new()).await });
        for _ in 0..100 {
            if stats(&provider).page_create_attempts == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&provider).page_create_attempts, 2);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let replacement = pool.new_page(PageOptions::new()).await.unwrap();
        assert_eq!(stats(&provider).launches, 1);
        replacement.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_in_flight_reservation_unretires_reusable_browser() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new()
            .with_max_browsers(Some(1))
            .with_max_open_pages_per_browser(2)
            .with_retire_browser_after_page_count(2);
        let pool = BrowserPool::new(provider.clone(), options);
        let first = pool.new_page(PageOptions::new()).await.unwrap();

        provider.block_next_page();
        let task_pool = pool.clone();
        let task = tokio::spawn(async move { task_pool.new_page(PageOptions::new()).await });
        for _ in 0..100 {
            if stats(&provider).page_create_attempts == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        first.close().await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let replacement = pool.new_page(PageOptions::new()).await.unwrap();
        assert_eq!(stats(&provider).launches, 1);
        replacement.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_rejects_page_created_by_in_flight_acquisition() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let first = pool.new_page(PageOptions::new()).await.unwrap();
        first.close().await.unwrap();

        provider.block_next_page();
        let task_pool = pool.clone();
        let task = tokio::spawn(async move { task_pool.new_page(PageOptions::new()).await });
        for _ in 0..100 {
            if stats(&provider).page_create_attempts == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let shutdown_pool = pool.clone();
        let mut shutdown = tokio::spawn(async move { shutdown_pool.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err(),
            "shutdown returned while a page acquisition was still in flight"
        );
        provider.page_release.notify_one();
        assert!(matches!(task.await.unwrap(), Err(BrowserError::Shutdown)));
        shutdown.await.unwrap().unwrap();
        let stats = stats(&provider);
        assert_eq!(stats.pages_closed.len(), 2);
        assert_eq!(stats.browsers_closed, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_launch_and_closes_its_browser() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        provider.block_next_launch();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());

        let task_pool = pool.clone();
        let acquisition = tokio::spawn(async move { task_pool.new_page(PageOptions::new()).await });
        for _ in 0..100 {
            if stats(&provider).launches == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&provider).launches, 1);

        let shutdown_pool = pool.clone();
        let mut shutdown = tokio::spawn(async move { shutdown_pool.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err(),
            "shutdown returned while a browser launch was still in flight"
        );

        provider.launch_release.notify_one();
        assert!(matches!(
            acquisition.await.unwrap(),
            Err(BrowserError::Shutdown)
        ));
        shutdown.await.unwrap().unwrap();
        assert_eq!(stats(&provider).browsers_closed, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_acquisition_keeps_capacity_until_created_page_is_closed() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let stall_next_hook = Arc::new(AtomicBool::new(false));
        let hook_flag = Arc::clone(&stall_next_hook);
        let hooks = BrowserHooks::default().push_post_page_create(move |_, _| {
            let stall = hook_flag.swap(false, Ordering::SeqCst);
            Box::pin(async move {
                if stall {
                    futures_util::future::pending::<()>().await;
                }
                Ok(())
            })
        });
        let provider = FakeProvider::new();
        let options = BrowserPoolOptions::new()
            .with_hooks(hooks)
            .with_max_browsers(Some(1))
            .with_max_open_pages_per_browser(1)
            .with_page_acquire_timeout(Duration::from_millis(300));
        let pool = BrowserPool::new(provider.clone(), options);
        let warmup = pool.new_page(PageOptions::new()).await.unwrap();
        warmup.close().await.unwrap();

        stall_next_hook.store(true, Ordering::SeqCst);
        provider.block_next_close();
        assert!(matches!(
            pool.new_page(PageOptions::new()).await,
            Err(BrowserError::PageCreate(_))
        ));
        for _ in 0..100 {
            if stats(&provider).page_close_attempts == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&provider).open_pages, 1);

        let task_pool = pool.clone();
        let mut replacement =
            tokio::spawn(async move { task_pool.new_page(PageOptions::new()).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut replacement)
                .await
                .is_err()
        );
        assert_eq!(stats(&provider).page_create_attempts, 2);
        assert_eq!(stats(&provider).open_pages, 1);

        provider.close_release.notify_one();
        let replacement = replacement.await.unwrap().unwrap();
        assert_eq!(stats(&provider).open_pages, 1);
        replacement.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_close_is_not_serialized_behind_a_blocked_drop_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let dropped = pool.new_page(PageOptions::new()).await.unwrap();
        let explicit = pool.new_page(PageOptions::new()).await.unwrap();

        provider.block_next_close();
        drop(dropped);
        for _ in 0..100 {
            if stats(&provider).page_close_attempts == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::timeout(Duration::from_secs(1), explicit.close())
            .await
            .expect("explicit close was serialized behind the background worker")
            .unwrap();
        assert_eq!(stats(&provider).pages_closed.len(), 1);

        provider.close_release.notify_one();
        pool.shutdown().await.unwrap();
        assert_eq!(stats(&provider).pages_closed.len(), 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn close_hook_can_cascade_to_another_explicit_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let cascade = Arc::new(Mutex::new(None::<PageHandle>));
        let hook_cascade = Arc::clone(&cascade);
        let hooks = BrowserHooks::default().push_pre_page_close(move |_, _| {
            let page = hook_cascade
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            Box::pin(async move {
                if let Some(page) = page {
                    page.close().await?;
                }
                Ok(())
            })
        });
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(
            provider.clone(),
            BrowserPoolOptions::new().with_hooks(hooks),
        );
        let first = pool.new_page(PageOptions::new()).await.unwrap();
        let second = pool.new_page(PageOptions::new()).await.unwrap();
        *cascade.lock().unwrap_or_else(|error| error.into_inner()) = Some(second);

        tokio::time::timeout(Duration::from_secs(1), first.close())
            .await
            .expect("cascading explicit close deadlocked")
            .unwrap();
        assert_eq!(stats(&provider).pages_closed.len(), 2);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_shutdown_does_not_poison_page_close_state() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        provider.block_next_close();

        let shutdown_pool = pool.clone();
        let shutdown = tokio::spawn(async move { shutdown_pool.shutdown().await });
        for _ in 0..100 {
            if stats(&provider).page_close_attempts == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        shutdown.abort();
        assert!(shutdown.await.unwrap_err().is_cancelled());

        let closing_page = page.clone();
        let mut close = tokio::spawn(async move { closing_page.close().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut close)
                .await
                .is_err()
        );
        provider.close_release.notify_one();
        close.await.unwrap().unwrap();
        assert_eq!(stats(&provider).pages_closed.len(), 1);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn background_worker_continues_after_close_hook_panic() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let panic_once = Arc::new(AtomicBool::new(true));
        let hook_flag = Arc::clone(&panic_once);
        let hooks = BrowserHooks::default().push_pre_page_close(move |_, _| {
            let should_panic = hook_flag.swap(false, Ordering::SeqCst);
            Box::pin(async move {
                assert!(!should_panic, "intentional background close panic");
                Ok(())
            })
        });
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(
            provider.clone(),
            BrowserPoolOptions::new().with_hooks(hooks),
        );
        let dropped = pool.new_page(PageOptions::new()).await.unwrap();
        let second = pool.new_page(PageOptions::new()).await.unwrap();

        drop(dropped);
        for _ in 0..100 {
            if !panic_once.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(second);
        for _ in 0..100 {
            if stats(&provider).pages_closed == vec![2] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&provider).pages_closed, vec![2]);
        pool.shutdown().await.unwrap();
        assert_eq!(stats(&provider).pages_closed.len(), 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_close_reports_task_panic_and_can_be_retried() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let panic_once = Arc::new(AtomicBool::new(true));
        let hook_flag = Arc::clone(&panic_once);
        let hooks = BrowserHooks::default().push_pre_page_close(move |_, _| {
            let should_panic = hook_flag.swap(false, Ordering::SeqCst);
            Box::pin(async move {
                assert!(!should_panic, "intentional explicit close panic");
                Ok(())
            })
        });
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(
            provider.clone(),
            BrowserPoolOptions::new().with_hooks(hooks),
        );
        let page = pool.new_page(PageOptions::new()).await.unwrap();

        assert!(matches!(
            page.close().await,
            Err(BrowserError::PageCreate(_))
        ));
        assert_eq!(stats(&provider).page_close_attempts, 0);
        page.close().await.unwrap();
        assert_eq!(stats(&provider).pages_closed.len(), 1);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_aborts_a_worker_stuck_in_drop_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        provider.block_next_close();
        drop(page);
        for _ in 0..100 {
            if stats(&provider).page_close_attempts == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let started = tokio::time::Instant::now();
        assert!(pool.shutdown().await.is_err());
        assert!(started.elapsed() < Duration::from_secs(7));
        for _ in 0..100 {
            if stats(&provider).pages_closed.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&provider).pages_closed.len(), 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_page_acquisition_can_drop_outside_a_runtime() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let thread_provider = provider.clone();
        let hooks = BrowserHooks::default().push_post_page_create(|_, _| {
            Box::pin(async move {
                futures_util::future::pending::<()>().await;
                Ok(())
            })
        });

        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let pool = BrowserPool::new(
                thread_provider.clone(),
                BrowserPoolOptions::new().with_hooks(hooks),
            );
            runtime.spawn(async move {
                let _ = pool.new_page(PageOptions::new()).await;
            });
            runtime.block_on(async {
                for _ in 0..100 {
                    if stats(&thread_provider).pages_created == 1 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert_eq!(stats(&thread_provider).pages_created, 1);
            });
            drop(runtime);
        });

        assert!(
            thread.join().is_ok(),
            "dropping the suspended acquisition outside its runtime panicked"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn aborting_explicit_close_still_closes_page_once() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        provider.block_next_close();
        let closing_page = page.clone();
        let task = tokio::spawn(async move { closing_page.close().await });
        for _ in 0..100 {
            if stats(&provider).page_close_attempts == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        provider.close_release.notify_one();
        for _ in 0..100 {
            if stats(&provider).pages_closed.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(page);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(stats(&provider).pages_closed.len(), 1);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_pool_before_last_handle_closes_page_and_browser() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        drop(pool);
        drop(page);
        for _ in 0..100 {
            let cleanup_finished = {
                let stats = stats(&provider);
                stats.pages_closed.len() == 1 && stats.browsers_closed == 1
            };
            if cleanup_finished {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stats = stats(&provider);
        assert_eq!(stats.pages_closed.len(), 1);
        assert_eq!(stats.browsers_closed, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropped_handle_closes_via_background_worker() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::hanging();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let task_pool = pool.clone();
        let task = tokio::spawn(async move {
            let page = task_pool.new_page(PageOptions::new()).await.unwrap();
            page.goto(
                &url::Url::parse("https://example.com").unwrap(),
                GotoOptions::default(),
            )
            .await
            .unwrap();
        });
        for _ in 0..100 {
            if stats(&provider).open_pages == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&provider).open_pages, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        for _ in 0..100 {
            if stats(&provider).pages_closed.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(stats(&provider).pages_closed.len(), 1);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_close_prevents_background_double_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        let clone = page.clone();
        page.close().await.unwrap();
        drop(page);
        drop(clone);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(stats(&provider).pages_closed.len(), 1);
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hooks_fire_on_create_and_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let imported = Cookie::new("imported", "one", "example.com");
        let exported = Cookie::new("exported", "two", "example.com");
        let provider = FakeProvider::with_cookies(vec![exported.clone()]);
        let options = BrowserPoolOptions::new()
            .with_hooks(BrowserHooks::default().with_session_cookie_sync());
        let pool = BrowserPool::new(provider.clone(), options);
        let session = Arc::new(Session::new(SessionConfig::default()));
        session
            .cookie_jar()
            .import_cookies(std::slice::from_ref(&imported));
        let page = pool
            .new_page(PageOptions::new().with_session(Arc::clone(&session)))
            .await
            .unwrap();
        assert_eq!(
            *provider
                .set_cookie_calls
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            vec![vec![imported.clone()]]
        );
        page.close().await.unwrap();
        let cookies = session.cookie_jar().export_cookies();
        assert!(cookies.iter().any(|cookie| cookie.name == imported.name));
        assert!(cookies.iter().any(|cookie| cookie.name == exported.name));
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pre_launch_proxy_and_args_reach_the_provider() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let proxy_url = url::Url::parse("http://proxy.example:8080").unwrap();
        let options = BrowserPoolOptions::new()
            .with_proxy(Some(ProxyConfiguration::round_robin([proxy_url.clone()])))
            .with_hooks(BrowserHooks::default().with_launch_args(vec!["--lang=en-US".into()]));
        let pool = BrowserPool::new(provider.clone(), options);
        let page = pool.new_page(PageOptions::new()).await.unwrap();
        {
            let stats = stats(&provider);
            assert_eq!(
                stats.launch_contexts[0].0.as_deref(),
                Some(proxy_url.as_str())
            );
            assert!(
                stats.launch_contexts[0]
                    .1
                    .iter()
                    .any(|arg| arg == "--lang=en-US")
            );
        }
        page.close().await.unwrap();
        pool.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_closes_everything_and_rejects_new_pages() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new();
        let pool = BrowserPool::new(provider.clone(), BrowserPoolOptions::new());
        let _p1 = pool.new_page(PageOptions::new()).await.unwrap();
        let _p2 = pool.new_page(PageOptions::new()).await.unwrap();
        pool.shutdown().await.unwrap();
        {
            let stats = stats(&provider);
            assert_eq!(stats.open_pages, 0);
            assert!(stats.browsers_closed >= 1);
        }
        assert!(matches!(
            pool.new_page(PageOptions::new()).await,
            Err(BrowserError::Shutdown)
        ));
    })
    .await
    .unwrap();
}
