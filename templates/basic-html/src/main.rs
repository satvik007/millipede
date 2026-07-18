use millipede::{
    CrawlPolicy, DatasetExt, EnqueueStrategy, HtmlContext, HtmlCrawler, HtmlKind,
    html::scraper::Selector,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let title_selector = Selector::parse("title")
        .map_err(|error| anyhow::anyhow!("invalid title selector: {error}"))?;

    let crawler = HtmlCrawler::builder(HtmlKind::new()?)
        .storage_client(std::sync::Arc::new(
            millipede::MemoryStorageClient::new(),
        ))
        .crawl_policy(
            CrawlPolicy::new().max_requests_per_crawl(10),
        )
        .request_handler(move |ctx: HtmlContext| {
            let title_selector = title_selector.clone();
            async move {
                let title = ctx
                    .html
                    .select_first(&title_selector, |element| element.text().collect::<String>())
                    .unwrap_or_else(|| "<untitled>".to_owned());
                println!("{} -> {title}", ctx.request.url);
                ctx.storage.dataset().push(&title).await?;
                let _ = ctx
                    .enqueue
                    .options()
                    .strategy(EnqueueStrategy::SameDomain)
                    .send()
                    .await?;
                Ok(())
            }
        })
        .build()
        .await?;

    let _ = crawler.run("https://books.toscrape.com/").await?;
    Ok(())
}
