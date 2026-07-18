use millipede::{HttpContext, HttpCrawler, HttpKind};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let crawler = HttpCrawler::builder(HttpKind::new()?)
        .storage_client(std::sync::Arc::new(
            millipede::MemoryStorageClient::new(),
        ))
        .max_request_retries(2)
        .request_handler(|ctx: HttpContext| async move {
            println!(
                "{} -> {} ({} bytes)",
                ctx.request.url,
                ctx.response.status,
                ctx.response.body.len()
            );
            Ok(())
        })
        .build()
        .await?;

    let _ = crawler.run("https://example.com/").await?;
    Ok(())
}
