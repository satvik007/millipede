use millipede::{
    BrowserContext, BrowserCrawler, BrowserKind, ChromiumLaunchOptions, ChromiumoxideProvider,
    find_browser,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Chrome or Chromium must be installed at runtime; MILLIPEDE_CHROME overrides its path.
    let Some(executable) = find_browser() else {
        anyhow::bail!("Chrome/Chromium not found; set MILLIPEDE_CHROME to its binary path");
    };

    let kind = BrowserKind::builder(ChromiumoxideProvider)
        .launch_options(ChromiumLaunchOptions::default().with_executable(executable))
        .build()?;
    let crawler = BrowserCrawler::builder(kind)
        .storage_client(std::sync::Arc::new(
            millipede::MemoryStorageClient::new(),
        ))
        .request_handler(|ctx: BrowserContext| async move {
            let title = ctx.page.evaluate_js("document.title").await?;
            println!("{} -> {title}", ctx.request.url);
            Ok(())
        })
        .build()
        .await?;

    let _ = crawler.run("https://example.com/").await?;
    Ok(())
}
