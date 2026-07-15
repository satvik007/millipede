# millipede-http

`millipede-http` provides the reqwest-based HTTP backend for the Millipede web-crawling
library. It supports manual redirects, per-request cookie jars, proxies, response streaming,
and optional in-flight request coalescing.

```rust
use millipede_core::http_client::{HttpClient, HttpRequest};
use millipede_http::ReqwestClient;
use url::Url;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = ReqwestClient::new()?;
let response = client
    .send(HttpRequest::new(Url::parse("https://example.com/")?))
    .await?;
println!("{}", response.status);
# Ok(())
# }
```

`HttpCrawler`, which builds the crawler lifecycle on top of this backend, arrives later in
Phase 3.
