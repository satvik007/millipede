//! Concurrent browser and page lifecycle management.

use std::{
    collections::HashMap,
    fmt,
    ops::Deref,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use futures_util::FutureExt;
use millipede_core::proxy::{ProxyConfiguration, ProxyInfo, ProxyResolveContext};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};

use crate::{
    BrowserError, BrowserHooks, BrowserPage, BrowserProvider, LaunchContext, PageId, PageOpts,
};

/// Configuration for a [`BrowserPool`].
#[non_exhaustive]
pub struct BrowserPoolOptions<L> {
    /// Maximum number of simultaneously open pages in one browser.
    pub max_open_pages_per_browser: usize,
    /// Number of created pages after which a browser is retired.
    pub retire_browser_after_page_count: u64,
    /// Maximum number of live or launching browsers, or `None` for no limit.
    pub max_browsers: Option<usize>,
    /// Maximum time to wait for a page, including browser launch and hook work.
    pub page_acquire_timeout: Duration,
    /// Provider-specific options cloned for every browser launch.
    pub launch_options: L,
    /// Optional proxy configuration resolved once per browser launch.
    pub proxy: Option<ProxyConfiguration>,
    /// Browser lifecycle hooks.
    pub hooks: BrowserHooks,
}

impl<L: Default> Default for BrowserPoolOptions<L> {
    fn default() -> Self {
        Self {
            max_open_pages_per_browser: 20,
            retire_browser_after_page_count: 100,
            max_browsers: None,
            page_acquire_timeout: Duration::from_secs(60),
            launch_options: L::default(),
            proxy: None,
            hooks: BrowserHooks::default(),
        }
    }
}

impl<L: Default> BrowserPoolOptions<L> {
    /// Creates options with the standard pool limits and default provider launch options.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<L> BrowserPoolOptions<L> {
    /// Sets the maximum number of simultaneously open pages in one browser.
    pub fn max_open_pages_per_browser(mut self, value: usize) -> Self {
        self.max_open_pages_per_browser = value;
        self
    }

    /// Sets the page count after which a browser is retired.
    pub fn retire_browser_after_page_count(mut self, value: u64) -> Self {
        self.retire_browser_after_page_count = value;
        self
    }

    /// Sets the maximum number of live or launching browsers.
    pub fn max_browsers(mut self, value: Option<usize>) -> Self {
        self.max_browsers = value;
        self
    }

    /// Sets the maximum duration allowed for page acquisition.
    pub fn page_acquire_timeout(mut self, value: Duration) -> Self {
        self.page_acquire_timeout = value;
        self
    }

    /// Replaces the provider-specific browser launch options.
    pub fn launch_options(mut self, value: L) -> Self {
        self.launch_options = value;
        self
    }

    /// Sets the proxy configuration used for browser launches.
    pub fn proxy(mut self, value: Option<ProxyConfiguration>) -> Self {
        self.proxy = value;
        self
    }

    /// Replaces the browser lifecycle hooks.
    pub fn hooks(mut self, value: BrowserHooks) -> Self {
        self.hooks = value;
        self
    }
}

impl<L> fmt::Debug for BrowserPoolOptions<L> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrowserPoolOptions")
            .field(
                "max_open_pages_per_browser",
                &self.max_open_pages_per_browser,
            )
            .field(
                "retire_browser_after_page_count",
                &self.retire_browser_after_page_count,
            )
            .field("max_browsers", &self.max_browsers)
            .field("page_acquire_timeout", &self.page_acquire_timeout)
            .field("launch_options", &"<provider-specific>")
            .field("proxy", &self.proxy)
            .field("hooks", &self.hooks)
            .finish()
    }
}

/// A lazily launched pool of provider browsers and their pages.
pub struct BrowserPool<P: BrowserProvider> {
    inner: Arc<PoolInner<P>>,
}

impl<P: BrowserProvider> Clone for BrowserPool<P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct PoolInner<P: BrowserProvider> {
    provider: P,
    options: BrowserPoolOptions<P::LaunchOptions>,
    state: Mutex<PoolState<P>>,
    close_tx: mpsc::UnboundedSender<CloseCommand<P>>,
    close_rx: StdMutex<Option<mpsc::UnboundedReceiver<CloseCommand<P>>>>,
    capacity: Notify,
    worker: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

struct PoolState<P: BrowserProvider> {
    browsers: Vec<BrowserSlot<P>>,
    pages: HashMap<PageId, PageEntry<P>>,
    next_browser_id: u64,
    shut_down: bool,
}

struct BrowserSlot<P: BrowserProvider> {
    id: u64,
    browser: Option<Arc<P::Browser>>,
    open_pages: usize,
    in_flight_pages: usize,
    pages_created: u64,
    launching: bool,
    retired: bool,
    proxy: Option<ProxyInfo>,
}

struct PageEntry<P: BrowserProvider> {
    page: P::Page,
    browser_id: u64,
    opts: PageOpts,
    closing: bool,
    close_notify: Arc<Notify>,
}

enum CloseCommand<P: BrowserProvider> {
    Page {
        inner: Arc<PoolInner<P>>,
        id: PageId,
    },
    Finalize {
        inner: Arc<PoolInner<P>>,
    },
    Barrier(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
}

struct LaunchGuard<P: BrowserProvider> {
    inner: Arc<PoolInner<P>>,
    browser_id: u64,
    armed: bool,
}

impl<P: BrowserProvider> LaunchGuard<P> {
    fn new(inner: Arc<PoolInner<P>>, browser_id: u64) -> Self {
        Self {
            inner,
            browser_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<P: BrowserProvider> Drop for LaunchGuard<P> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = Arc::clone(&self.inner);
        let browser_id = self.browser_id;
        if let Ok(mut state) = inner.state.try_lock() {
            state.browsers.retain(|slot| slot.id != browser_id);
            drop(state);
            inner.capacity.notify_waiters();
        } else if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                inner
                    .state
                    .lock()
                    .await
                    .browsers
                    .retain(|slot| slot.id != browser_id);
                inner.capacity.notify_waiters();
            });
        } else {
            tracing::warn!(
                browser_id,
                "cancelled browser launch dropped outside a Tokio runtime; launch placeholder cleanup could not be scheduled"
            );
        }
    }
}

struct PageReservationGuard<P: BrowserProvider> {
    inner: Arc<PoolInner<P>>,
    browser_id: u64,
    page: Option<P::Page>,
    armed: bool,
}

impl<P: BrowserProvider> PageReservationGuard<P> {
    fn new(inner: Arc<PoolInner<P>>, browser_id: u64) -> Self {
        Self {
            inner,
            browser_id,
            page: None,
            armed: true,
        }
    }

    fn set_page(&mut self, page: P::Page) {
        self.page = Some(page);
    }

    fn clear_page(&mut self) {
        self.page = None;
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.page = None;
    }
}

impl<P: BrowserProvider> Drop for PageReservationGuard<P> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let page = self.page.take();
        let inner = Arc::clone(&self.inner);
        let browser_id = self.browser_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(page) = page {
                    if let Err(error) = inner.provider.close_page(page).await {
                        tracing::warn!(%error, "failed to close page after cancelled acquisition");
                    }
                }
                let browser = rollback_page_reservation(&inner, browser_id).await;
                if let Some(browser) = browser {
                    close_browser_arc(inner.as_ref(), browser).await;
                }
            });
        } else {
            tracing::warn!(
                "cancelled page acquisition dropped outside a Tokio runtime; provider cleanup was limited to dropping its handles"
            );
        }
    }
}

async fn rollback_page_reservation<P: BrowserProvider>(
    inner: &Arc<PoolInner<P>>,
    browser_id: u64,
) -> Option<Arc<P::Browser>> {
    let mut state = inner.state.lock().await;
    let shut_down = state.shut_down;
    let mut browser = None;
    if let Some(slot) = state.browsers.iter_mut().find(|slot| slot.id == browser_id) {
        slot.open_pages = slot.open_pages.saturating_sub(1);
        slot.in_flight_pages = slot.in_flight_pages.saturating_sub(1);
        slot.pages_created = slot.pages_created.saturating_sub(1);
        slot.retired = slot.pages_created >= inner.options.retire_browser_after_page_count;
        if slot.open_pages == 0 && (slot.retired || shut_down) {
            browser = slot.browser.take();
        }
    }
    drop(state);
    inner.capacity.notify_waiters();
    browser
}

impl<P: BrowserProvider> BrowserPool<P> {
    /// Creates an empty pool without requiring a Tokio runtime.
    ///
    /// Browser launch and the fallback close worker are both lazy: the first call to
    /// [`Self::new_page`] starts them from within the caller's runtime. This intentionally differs
    /// from the asynchronous constructor sketched in INTERFACE §12.
    pub fn new(provider: P, options: BrowserPoolOptions<P::LaunchOptions>) -> Self {
        let (close_tx, close_rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(PoolInner {
                provider,
                options,
                state: Mutex::new(PoolState {
                    browsers: Vec::new(),
                    pages: HashMap::new(),
                    next_browser_id: 1,
                    shut_down: false,
                }),
                close_tx,
                close_rx: StdMutex::new(Some(close_rx)),
                capacity: Notify::new(),
                worker: StdMutex::new(None),
            }),
        }
    }

    fn start_close_worker(&self) {
        let mut worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.is_some() {
            return;
        }
        let receiver = self
            .inner
            .close_rx
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let Some(mut receiver) = receiver else {
            return;
        };
        *worker = Some(tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    CloseCommand::Page { inner, id } => {
                        match AssertUnwindSafe(close_page(inner, id)).catch_unwind().await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                tracing::warn!(page_id = %id, %error, "background page close failed");
                            }
                            Err(_) => {
                                tracing::warn!(page_id = %id, "background page close panicked");
                            }
                        }
                    }
                    CloseCommand::Finalize { inner } => {
                        finalize_orphaned_pool(inner).await;
                    }
                    CloseCommand::Barrier(completion) => {
                        let _ = completion.send(());
                    }
                    CloseCommand::Stop(completion) => {
                        let _ = completion.send(());
                        break;
                    }
                }
            }
        }));
    }

    /// Acquires a page, launching or waiting for browser capacity as needed.
    pub async fn new_page(&self, opts: PageOpts) -> Result<PageHandle, BrowserError> {
        self.start_close_worker();
        let timeout = self.inner.options.page_acquire_timeout;
        match tokio::time::timeout(timeout, self.acquire_page(opts)).await {
            Ok(result) => result,
            Err(_) => Err(BrowserError::PageCreate(anyhow::anyhow!(
                "page acquisition timed out after {:?}",
                timeout
            ))),
        }
    }

    async fn acquire_page(&self, opts: PageOpts) -> Result<PageHandle, BrowserError> {
        loop {
            let notified = self.inner.capacity.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            enum Choice<B> {
                Existing {
                    id: u64,
                    browser: Arc<B>,
                    proxy: Option<ProxyInfo>,
                },
                Launch(u64),
                Wait,
            }

            let choice = {
                let mut state = self.inner.state.lock().await;
                if state.shut_down {
                    return Err(BrowserError::Shutdown);
                }
                let available = state.browsers.iter().position(|slot| {
                    !slot.retired
                        && !slot.launching
                        && slot.browser.is_some()
                        && slot.open_pages < self.inner.options.max_open_pages_per_browser
                        && slot.pages_created < self.inner.options.retire_browser_after_page_count
                });
                if let Some(index) = available {
                    let slot = &mut state.browsers[index];
                    slot.open_pages += 1;
                    slot.in_flight_pages += 1;
                    slot.pages_created += 1;
                    Choice::Existing {
                        id: slot.id,
                        browser: Arc::clone(slot.browser.as_ref().expect("browser checked above")),
                        proxy: slot.proxy.clone(),
                    }
                } else {
                    let live_count = state
                        .browsers
                        .iter()
                        .filter(|slot| slot.launching || slot.browser.is_some())
                        .count();
                    #[allow(clippy::unnecessary_map_or)]
                    let launch_allowed = self
                        .inner
                        .options
                        .max_browsers
                        .map_or(true, |maximum| live_count < maximum);
                    if launch_allowed {
                        let id = state.next_browser_id;
                        state.next_browser_id = state.next_browser_id.wrapping_add(1);
                        state.browsers.push(BrowserSlot {
                            id,
                            browser: None,
                            open_pages: 0,
                            in_flight_pages: 0,
                            pages_created: 0,
                            launching: true,
                            retired: false,
                            proxy: None,
                        });
                        Choice::Launch(id)
                    } else {
                        Choice::Wait
                    }
                }
            };

            match choice {
                Choice::Wait => notified.await,
                Choice::Launch(id) => {
                    let mut guard = LaunchGuard::new(Arc::clone(&self.inner), id);
                    let proxy = if let Some(configuration) = &self.inner.options.proxy {
                        configuration
                            .new_proxy_info(ProxyResolveContext::new())
                            .await
                            .map_err(|error| BrowserError::Launch(anyhow::Error::new(error)))?
                    } else {
                        None
                    };
                    let mut context = LaunchContext::new();
                    context.proxy = proxy;
                    for hook in &self.inner.options.hooks.pre_launch {
                        hook(&mut context);
                    }
                    let launch_options = self.inner.options.launch_options.clone();
                    match self.inner.provider.launch(launch_options, &context).await {
                        Ok(browser) => {
                            let mut browser = Some(browser);
                            let install = {
                                let mut state = self.inner.state.lock().await;
                                if state.shut_down {
                                    false
                                } else if let Some(slot) =
                                    state.browsers.iter_mut().find(|slot| slot.id == id)
                                {
                                    slot.browser = Some(Arc::new(
                                        browser.take().expect("launched browser is available"),
                                    ));
                                    slot.launching = false;
                                    slot.proxy = context.proxy;
                                    true
                                } else {
                                    false
                                }
                            };
                            if !install {
                                if let Err(error) = self
                                    .inner
                                    .provider
                                    .close_browser(
                                        browser.take().expect("uninstalled browser is available"),
                                    )
                                    .await
                                {
                                    tracing::warn!(%error, "failed to close browser launched during shutdown");
                                }
                                self.inner
                                    .state
                                    .lock()
                                    .await
                                    .browsers
                                    .retain(|slot| slot.id != id);
                                guard.disarm();
                                self.inner.capacity.notify_waiters();
                                return Err(BrowserError::Shutdown);
                            }
                            guard.disarm();
                            self.inner.capacity.notify_waiters();
                        }
                        Err(error) => return Err(error),
                    }
                }
                Choice::Existing { id, browser, proxy } => {
                    let mut guard = PageReservationGuard::new(Arc::clone(&self.inner), id);
                    let mut prepared_opts = opts.clone();
                    for hook in &self.inner.options.hooks.pre_page_create {
                        hook(&mut prepared_opts);
                    }
                    let page_result = self.inner.provider.new_page(browser.as_ref()).await;
                    drop(browser);
                    let page = match page_result {
                        Ok(page) => page,
                        Err(error) => return Err(error),
                    };
                    guard.set_page(page.clone());
                    for hook in &self.inner.options.hooks.post_page_create {
                        if let Err(error) = hook(&page, &prepared_opts).await {
                            if let Err(close_error) =
                                self.inner.provider.close_page(page.clone()).await
                            {
                                tracing::warn!(%close_error, "failed to close page after hook error");
                            }
                            guard.clear_page();
                            return Err(error);
                        }
                    }
                    let page_id = PageId::next();
                    let registered = {
                        let mut state = self.inner.state.lock().await;
                        if state.shut_down {
                            false
                        } else {
                            if let Some(slot) = state.browsers.iter_mut().find(|slot| slot.id == id)
                            {
                                slot.in_flight_pages = slot.in_flight_pages.saturating_sub(1);
                            }
                            state.pages.insert(
                                page_id,
                                PageEntry {
                                    page: page.clone(),
                                    browser_id: id,
                                    opts: prepared_opts,
                                    closing: false,
                                    close_notify: Arc::new(Notify::new()),
                                },
                            );
                            true
                        }
                    };
                    if !registered {
                        if let Err(error) = self.inner.provider.close_page(page.clone()).await {
                            tracing::warn!(%error, "failed to close page created during shutdown");
                        }
                        guard.clear_page();
                        return Err(BrowserError::Shutdown);
                    }
                    guard.disarm();
                    let closer: Arc<dyn PoolCloser> = self.inner.clone();
                    return Ok(PageHandle {
                        page: Arc::new(page),
                        proxy,
                        shared: Arc::new(HandleShared {
                            id: page_id,
                            close_state: AtomicU8::new(HANDLE_OPEN),
                            closer: Some(closer),
                        }),
                    });
                }
            }
        }
    }

    /// Closes all pages and browsers and rejects future acquisitions.
    ///
    /// Calling shutdown more than once is safe.
    pub async fn shutdown(&self) -> Result<(), BrowserError> {
        const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

        self.start_close_worker();
        {
            let mut state = self.inner.state.lock().await;
            state.shut_down = true;
        }
        self.inner.capacity.notify_waiters();

        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let mut first_error = None;

        let (barrier_tx, barrier_rx) = oneshot::channel();
        if self
            .inner
            .close_tx
            .send(CloseCommand::Barrier(barrier_tx))
            .is_ok()
            && tokio::time::timeout_at(deadline, barrier_rx).await.is_err()
        {
            first_error = Some(shutdown_timeout_error());
            abort_close_worker(&self.inner).await;
        }

        loop {
            let notified = self.inner.capacity.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let has_in_flight = {
                let state = self.inner.state.lock().await;
                state
                    .browsers
                    .iter()
                    .any(|slot| slot.launching || slot.in_flight_pages != 0)
            };
            if !has_in_flight {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                if first_error.is_none() {
                    first_error = Some(shutdown_timeout_error());
                }
                break;
            }
        }

        let page_ids = {
            let state = self.inner.state.lock().await;
            state.pages.keys().copied().collect::<Vec<_>>()
        };

        let mut close_tasks = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            let inner = Arc::clone(&self.inner);
            close_tasks.push(tokio::spawn(async move {
                close_page_by_id(&inner, page_id).await
            }));
        }
        for mut task in close_tasks {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(close_task_error(error));
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(shutdown_timeout_error());
                    }
                }
            }
        }

        let browsers = {
            let mut state = self.inner.state.lock().await;
            state
                .browsers
                .iter_mut()
                .filter(|slot| slot.open_pages == 0 && slot.in_flight_pages == 0)
                .filter_map(|slot| slot.browser.take())
                .collect::<Vec<_>>()
        };
        let mut browser_tasks = Vec::with_capacity(browsers.len());
        for browser in browsers {
            let inner = Arc::clone(&self.inner);
            browser_tasks.push(tokio::spawn(async move {
                close_browser_arc(inner.as_ref(), browser).await;
            }));
        }
        for mut task in browser_tasks {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() && first_error.is_none()
            {
                first_error = Some(shutdown_timeout_error());
            }
        }

        let (stop_tx, stop_rx) = oneshot::channel();
        let stop_sent = self
            .inner
            .close_tx
            .send(CloseCommand::Stop(stop_tx))
            .is_ok();
        if stop_sent
            && tokio::time::timeout_at(deadline, stop_rx).await.is_err()
            && first_error.is_none()
        {
            first_error = Some(shutdown_timeout_error());
        }
        let worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(mut worker) = worker {
            if tokio::time::timeout_at(deadline, &mut worker)
                .await
                .is_err()
            {
                worker.abort();
                let _ = worker.await;
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }
}

async fn close_page_by_id<P: BrowserProvider>(
    inner: &Arc<PoolInner<P>>,
    id: PageId,
) -> Result<(), BrowserError> {
    close_page(Arc::clone(inner), id).await
}

async fn close_page<P: BrowserProvider>(
    inner: Arc<PoolInner<P>>,
    id: PageId,
) -> Result<(), BrowserError> {
    let entry: PageEntry<P> = loop {
        let wait = {
            let mut state = inner.state.lock().await;
            let Some(entry) = state.pages.get_mut(&id) else {
                return Ok(());
            };
            if entry.closing {
                let mut notified = Box::pin(Arc::clone(&entry.close_notify).notified_owned());
                notified.as_mut().enable();
                Some(notified)
            } else {
                entry.closing = true;
                break PageEntry {
                    page: entry.page.clone(),
                    browser_id: entry.browser_id,
                    opts: entry.opts.clone(),
                    closing: true,
                    close_notify: Arc::clone(&entry.close_notify),
                };
            }
        };
        if let Some(wait) = wait {
            wait.await;
        }
    };
    let mut close_guard = PageCloseGuard {
        inner: Arc::clone(&inner),
        id,
        notify: Arc::clone(&entry.close_notify),
        armed: true,
    };

    for hook in &inner.options.hooks.pre_page_close {
        if let Err(error) = hook(&entry.page, &entry.opts).await {
            tracing::warn!(page_id = %id, %error, "pre-page-close hook failed");
        }
    }
    let close_result = inner.provider.close_page(entry.page).await;

    let browser = {
        let mut state = inner.state.lock().await;
        state.pages.remove(&id);
        let shut_down = state.shut_down;
        let mut browser = None;
        if let Some(slot) = state
            .browsers
            .iter_mut()
            .find(|slot| slot.id == entry.browser_id)
        {
            slot.open_pages = slot.open_pages.saturating_sub(1);
            if slot.pages_created >= inner.options.retire_browser_after_page_count {
                slot.retired = true;
            }
            if (slot.retired || shut_down) && slot.open_pages == 0 {
                browser = slot.browser.take();
            }
        }
        browser
    };
    inner.capacity.notify_waiters();
    close_guard.armed = false;
    entry.close_notify.notify_waiters();
    for hook in &inner.options.hooks.post_page_close {
        hook(id);
    }
    if let Some(browser) = browser {
        close_browser_arc(inner.as_ref(), browser).await;
    }
    close_result
}

struct PageCloseGuard<P: BrowserProvider> {
    inner: Arc<PoolInner<P>>,
    id: PageId,
    notify: Arc<Notify>,
    armed: bool,
}

impl<P: BrowserProvider> Drop for PageCloseGuard<P> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = Arc::clone(&self.inner);
        let id = self.id;
        let notify = Arc::clone(&self.notify);
        if let Ok(mut state) = inner.state.try_lock() {
            if let Some(entry) = state.pages.get_mut(&id) {
                entry.closing = false;
            }
            drop(state);
            notify.notify_waiters();
        } else if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(entry) = inner.state.lock().await.pages.get_mut(&id) {
                    entry.closing = false;
                }
                notify.notify_waiters();
            });
        } else {
            tracing::warn!(page_id = %id, "cancelled page close dropped outside a Tokio runtime");
        }
    }
}

fn shutdown_timeout_error() -> BrowserError {
    BrowserError::PageCreate(anyhow::anyhow!(
        "browser pool shutdown timed out after 5 seconds"
    ))
}

fn close_task_error(error: tokio::task::JoinError) -> BrowserError {
    BrowserError::PageCreate(anyhow::anyhow!("page close task failed: {error}"))
}

async fn abort_close_worker<P: BrowserProvider>(inner: &Arc<PoolInner<P>>) {
    let worker = inner
        .worker
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    if let Some(worker) = worker {
        worker.abort();
        let _ = worker.await;
    }
}

async fn finalize_orphaned_pool<P: BrowserProvider>(inner: Arc<PoolInner<P>>) {
    if Arc::strong_count(&inner) != 1 {
        return;
    }
    let browsers = {
        let mut state = inner.state.lock().await;
        if !state.pages.is_empty() {
            return;
        }
        state
            .browsers
            .iter_mut()
            .filter_map(|slot| slot.browser.take())
            .collect::<Vec<_>>()
    };
    for browser in browsers {
        close_browser_arc(inner.as_ref(), browser).await;
    }
}

async fn close_browser_arc<P: BrowserProvider>(inner: &PoolInner<P>, mut browser: Arc<P::Browser>) {
    for _ in 0..16 {
        match Arc::try_unwrap(browser) {
            Ok(browser) => {
                if let Err(error) = inner.provider.close_browser(browser).await {
                    tracing::warn!(%error, "failed to close retired browser");
                }
                return;
            }
            Err(still_shared) => {
                browser = still_shared;
                tokio::task::yield_now().await;
            }
        }
    }
    tracing::warn!(
        outstanding_clones = Arc::strong_count(&browser) - 1,
        "browser could not be explicitly closed because handles are still outstanding"
    );
}

#[async_trait::async_trait]
trait PoolCloser: Send + Sync {
    async fn close_now(self: Arc<Self>, id: PageId) -> Result<(), BrowserError>;
    fn enqueue_close(self: Arc<Self>, id: PageId);
    fn enqueue_finalize(self: Arc<Self>);
}

#[async_trait::async_trait]
impl<P: BrowserProvider> PoolCloser for PoolInner<P> {
    async fn close_now(self: Arc<Self>, id: PageId) -> Result<(), BrowserError> {
        match tokio::spawn(close_page(self, id)).await {
            Ok(result) => result,
            Err(error) => Err(close_task_error(error)),
        }
    }

    fn enqueue_close(self: Arc<Self>, id: PageId) {
        let _ = self.close_tx.send(CloseCommand::Page {
            inner: Arc::clone(&self),
            id,
        });
    }

    fn enqueue_finalize(self: Arc<Self>) {
        let _ = self.close_tx.send(CloseCommand::Finalize {
            inner: Arc::clone(&self),
        });
    }
}

const HANDLE_OPEN: u8 = 0;
const HANDLE_CLOSING: u8 = 1;
const HANDLE_CLOSED: u8 = 2;

struct HandleShared {
    id: PageId,
    close_state: AtomicU8,
    /// A strong closer is cloned into every queued command, so the worker can finish cleanup even
    /// after the owning `BrowserPool` and the last page handle have both been dropped.
    closer: Option<Arc<dyn PoolCloser>>,
}

impl Drop for HandleShared {
    fn drop(&mut self) {
        let closer = self
            .closer
            .take()
            .expect("handle closer is present until HandleShared::drop");
        if self.close_state.load(Ordering::SeqCst) != HANDLE_CLOSED {
            tracing::warn!(
                page_id = %self.id,
                "PageHandle dropped without close(); scheduling background close — prefer page.close().await"
            );
            Arc::clone(&closer).enqueue_close(self.id);
        }
        closer.enqueue_finalize();
    }
}

/// Provider-erased RAII handle for a page checked out from a [`BrowserPool`].
///
/// `Clone` is required by the engine's `Context: Clone` contract. All clones share an atomic
/// close guard, guaranteeing at-most-once cleanup: an explicit [`Self::close`] wins, while dropping
/// the last clone schedules background cleanup only when explicit close was never called.
#[derive(Clone)]
pub struct PageHandle {
    page: Arc<dyn BrowserPage>,
    proxy: Option<ProxyInfo>,
    shared: Arc<HandleShared>,
}

impl PageHandle {
    /// Returns this page's stable process-local identifier.
    pub fn id(&self) -> PageId {
        self.shared.id
    }

    /// Returns the proxy resolved for the browser that owns this page.
    pub fn proxy_info(&self) -> Option<&ProxyInfo> {
        self.proxy.as_ref()
    }

    /// Explicitly closes the page.
    ///
    /// This is idempotent across every clone of the handle.
    pub async fn close(&self) -> Result<(), BrowserError> {
        if self
            .shared
            .close_state
            .compare_exchange(
                HANDLE_OPEN,
                HANDLE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Ok(());
        }
        let mut guard = HandleCloseGuard {
            shared: &self.shared,
            armed: true,
        };
        let result = Arc::clone(
            self.shared
                .closer
                .as_ref()
                .expect("handle closer is present while PageHandle exists"),
        )
        .close_now(self.shared.id)
        .await;
        if result.is_ok() {
            self.shared
                .close_state
                .store(HANDLE_CLOSED, Ordering::SeqCst);
            guard.armed = false;
        }
        result
    }
}

impl Deref for PageHandle {
    type Target = dyn BrowserPage;

    fn deref(&self) -> &Self::Target {
        self.page.as_ref()
    }
}

impl fmt::Debug for PageHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageHandle")
            .field("id", &self.id())
            .field("proxy", &self.proxy)
            .field(
                "closed",
                &(self.shared.close_state.load(Ordering::SeqCst) == HANDLE_CLOSED),
            )
            .finish_non_exhaustive()
    }
}

struct HandleCloseGuard<'a> {
    shared: &'a HandleShared,
    armed: bool,
}

impl Drop for HandleCloseGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.shared.close_state.store(HANDLE_OPEN, Ordering::SeqCst);
        }
    }
}
