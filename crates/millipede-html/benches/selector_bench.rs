//! Selector parsing and link-extraction benchmarks for ADR-0005.
#![allow(missing_docs)]

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use ::scraper::{Html as RawHtml, Selector};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures_util::stream;
use http::{HeaderMap, StatusCode};
use lol_html::{ElementContentHandlers, HtmlRewriter, Settings};
use millipede_core::{
    crawler::Crawler,
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, StreamingResponse},
};
use millipede_html::{HtmlContext, HtmlKind, SynchronizedHtml};
use millipede_storage_memory::MemoryStorageClient;
use url::Url;

const DETAIL_SELECTOR: &str = "a.detail[href]";
const LINK_COUNT: usize = 200;

millipede_html::selectors! {
    detail_selector = "a.detail[href]";
}

fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}
fn assert_context_bounds<T: Send + Clone + 'static>() {}

// `RawHtml` (upstream `scraper::Html`) is deliberately guarded as `Send` only: it is `!Sync`
// (ADR-0005), which is why `HtmlContext` stores the `Send + Sync` `SynchronizedHtml` facade.
fn compile_time_guards() {
    assert_send::<RawHtml>();
    assert_send_sync::<millipede_html::SynchronizedHtml>();
    assert_context_bounds::<Arc<millipede_html::SynchronizedHtml>>();
}

#[derive(Clone, Copy)]
enum PageKind {
    Product,
    Category,
}

impl PageKind {
    fn name(self) -> &'static str {
        match self {
            Self::Product => "product",
            Self::Category => "category",
        }
    }

    fn seed(self) -> u64 {
        match self {
            Self::Product => 0x5eed_cafe_d00d_f00d,
            Self::Category => 0xcafe_f00d_5eed_d00d,
        }
    }
}

struct Document {
    html: String,
}

struct Corpus {
    name: &'static str,
    documents: Vec<Document>,
    synchronized: Vec<Arc<SynchronizedHtml>>,
    total_bytes: usize,
}

struct CorpusClient {
    responses: HashMap<String, HttpResponse>,
}

#[async_trait::async_trait]
impl HttpClient for CorpusClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        self.responses
            .get(request.url.path())
            .cloned()
            .ok_or_else(|| HttpClientError::other(anyhow::anyhow!("missing benchmark response")))
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        Ok(StreamingResponse::new(
            request.url,
            StatusCode::OK,
            HeaderMap::new(),
            Box::pin(stream::empty()),
        ))
    }
}

fn synchronize_documents(documents: &[Document]) -> Vec<Arc<SynchronizedHtml>> {
    let mut responses = HashMap::new();
    let mut urls = Vec::with_capacity(documents.len());
    for (index, document) in documents.iter().enumerate() {
        let url = Url::parse(&format!("https://benchmark.test/{index}")).unwrap();
        responses.insert(
            url.path().to_owned(),
            HttpResponse::new(
                url.clone(),
                StatusCode::OK,
                HeaderMap::new(),
                document.html.clone().into(),
            ),
        );
        urls.push(url);
    }

    let synchronized = Arc::new(Mutex::new(Vec::with_capacity(documents.len())));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let kind = HtmlKind::builder()
            .http_client(Arc::new(CorpusClient { responses }))
            .build()
            .unwrap();
        let crawler = Crawler::builder(kind)
            .request_handler({
                let synchronized = Arc::clone(&synchronized);
                move |context: HtmlContext| {
                    let synchronized = Arc::clone(&synchronized);
                    async move {
                        synchronized.lock().unwrap().push(context.html);
                        Ok(())
                    }
                }
            })
            .storage_client(Arc::new(MemoryStorageClient::new()))
            .build()
            .await
            .unwrap();
        crawler.run(urls).await.unwrap();
    });
    Arc::into_inner(synchronized).unwrap().into_inner().unwrap()
}

fn deterministic_text(seed: &mut u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    let mut text = String::with_capacity(len);
    for _ in 0..len {
        *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        text.push(ALPHABET[(*seed as usize) % ALPHABET.len()] as char);
    }
    text
}

fn href(index: usize) -> String {
    match index % 4 {
        0 => format!("/p/{index}"),
        1 => format!("p/{index}"),
        2 => format!("https://shop.example/p/{index}"),
        _ => format!("https://external.example/p/{index}"),
    }
}

fn nested_soup_block(seed: &mut u64, index: usize, kind: PageKind) -> String {
    *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let depth = 3 + (*seed as usize % 5);
    let mut block = String::with_capacity(256);
    for level in 0..depth {
        block.push_str("<div class=\"soup layer-");
        block.push_str(&(level % 4).to_string());
        block.push_str("\">");
    }
    block.push_str("<section data-kind=\"");
    block.push_str(kind.name());
    block.push_str("\"><span>");
    block.push_str(&deterministic_text(seed, 32 + index % 17));
    block.push_str("</span></section>");
    for _ in 0..depth {
        block.push_str("</div>");
    }
    block
}

fn generate_page(target_bytes: usize, kind: PageKind, with_base: bool) -> String {
    let mut html = String::with_capacity(target_bytes + 1024);
    html.push_str("<!doctype html><html><head><meta charset=\"utf-8\">");
    if with_base {
        html.push_str("<base href=\"https://shop.example/catalog/\">");
    }
    html.push_str("<title>Selector benchmark</title></head><body><main>");

    for group in 0..20 {
        match kind {
            PageKind::Product => {
                html.push_str("<div class=\"layout\"><div><article class=\"product\">")
            }
            PageKind::Category => {
                html.push_str("<div class=\"category\"><div class=\"grid\"><section>")
            }
        }
        for offset in 0..10 {
            let index = group * 10 + offset;
            html.push_str("<a class=\"detail\" href=\"");
            html.push_str(&href(index));
            html.push_str("\">item ");
            html.push_str(&index.to_string());
            html.push_str("</a>");
        }
        match kind {
            PageKind::Product => html.push_str("</article></div></div>"),
            PageKind::Category => html.push_str("</section></div></div>"),
        }
    }

    let closing_len = "</main></body></html>".len();
    let mut seed = kind.seed() ^ target_bytes as u64 ^ u64::from(with_base);
    let mut soup_index = 0;
    loop {
        let block = nested_soup_block(&mut seed, soup_index, kind);
        if html.len() + block.len() + closing_len > target_bytes {
            break;
        }
        html.push_str(&block);
        soup_index += 1;
    }

    // Only the sub-block remainder is filled with text; growth to 100 KB and 1 MB comes from the
    // increasing number of nested soup nodes above, while every document retains exactly 200 links.
    let remaining = target_bytes.saturating_sub(html.len() + closing_len);
    if remaining >= 7 {
        html.push_str("<!--");
        html.push_str(&deterministic_text(&mut seed, remaining - 7));
        html.push_str("-->");
    }
    html.push_str("</main></body></html>");
    html
}

fn corpora() -> Vec<Corpus> {
    [
        ("10kb", 10 * 1024),
        ("100kb", 100 * 1024),
        ("1mb", 1024 * 1024),
    ]
    .into_iter()
    .map(|(name, target_bytes)| {
        let documents = [PageKind::Product, PageKind::Category]
            .into_iter()
            .flat_map(|kind| {
                [true, false]
                    .into_iter()
                    .map(move |with_base| (kind, with_base))
            })
            .map(|(kind, with_base)| {
                let html = generate_page(target_bytes, kind, with_base);
                let parsed = RawHtml::parse_document(&html);
                debug_assert_eq!(
                    parsed.select(&Selector::parse("a[href]").unwrap()).count(),
                    LINK_COUNT
                );
                Document { html }
            })
            .collect::<Vec<_>>();
        let total_bytes = documents.iter().map(|document| document.html.len()).sum();
        let synchronized = synchronize_documents(&documents);
        Corpus {
            name,
            documents,
            synchronized,
            total_bytes,
        }
    })
    .collect()
}

fn registry_lookup_or_parse<'a>(
    registry: &'a mut HashMap<String, Selector>,
    css: &str,
) -> &'a Selector {
    if !registry.contains_key(css) {
        registry.insert(css.to_owned(), Selector::parse(css).unwrap());
    }
    registry.get(css).unwrap()
}

fn extract_scraper_full(corpus: &Corpus, selector: &Selector) -> Vec<String> {
    corpus
        .documents
        .iter()
        .flat_map(|document| {
            let parsed = RawHtml::parse_document(&document.html);
            extract_scraper_preparsed(&parsed, selector)
        })
        .collect()
}

fn extract_scraper_preparsed(html: &RawHtml, selector: &Selector) -> Vec<String> {
    html.select(selector)
        .filter_map(|element| element.value().attr("href").map(str::to_owned))
        .collect()
}

fn extract_scraper_preparsed_corpus(corpus: &Corpus, selector: &Selector) -> Vec<String> {
    corpus
        .synchronized
        .iter()
        .flat_map(|html| {
            html.select(selector, |element| {
                element.value().attr("href").map(str::to_owned)
            })
            .into_iter()
            .flatten()
        })
        .collect()
}

fn lol_link_selector() -> &'static lol_html::Selector {
    static SELECTOR: OnceLock<lol_html::Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| "a[href]".parse().unwrap())
}

fn extract_lol_html(html: &[u8]) -> Vec<String> {
    let mut hrefs = Vec::with_capacity(LINK_COUNT);
    {
        let handler = ElementContentHandlers::default().element(
            |element: &mut lol_html::html_content::Element<'_, '_, _>| {
                if let Some(href) = element.get_attribute("href") {
                    hrefs.push(href);
                }
                Ok(())
            },
        );
        let settings = Settings {
            element_content_handlers: vec![(Cow::Borrowed(lol_link_selector()), handler)],
            ..Settings::new()
        };
        let mut rewriter = HtmlRewriter::new(settings, |_: &[u8]| {});
        rewriter.write(html).unwrap();
        rewriter.end().unwrap();
    }
    hrefs
}

fn extract_lol_html_corpus(corpus: &Corpus) -> Vec<String> {
    corpus
        .documents
        .iter()
        .flat_map(|document| extract_lol_html(document.html.as_bytes()))
        .collect()
}

fn selector_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("selector_parse");

    group.bench_function("inline_parse", |b| {
        b.iter(|| black_box(Selector::parse(black_box(DETAIL_SELECTOR)).unwrap()));
    });
    group.bench_function("selectors_macro_once_lock", |b| {
        b.iter(|| black_box(detail_selector()));
    });

    let mut registry = HashMap::new();
    registry_lookup_or_parse(&mut registry, DETAIL_SELECTOR);
    group.bench_function("hash_map_registry", |b| {
        b.iter(|| {
            let _ = black_box(registry_lookup_or_parse(
                &mut registry,
                black_box(DETAIL_SELECTOR),
            ));
        });
    });
    group.finish();
}

fn extract_links(c: &mut Criterion) {
    let link_selector = Selector::parse("a[href]").unwrap();

    for corpus in corpora() {
        let mut group = c.benchmark_group(format!("extract_links/{}", corpus.name));
        group.throughput(Throughput::Bytes(corpus.total_bytes as u64));
        if corpus.name == "1mb" {
            group.sample_size(20);
            group.measurement_time(Duration::from_secs(5));
        }

        group.bench_with_input(
            BenchmarkId::new("scraper_full_parse_select", corpus.total_bytes),
            &corpus,
            |b, corpus| {
                b.iter(|| {
                    black_box(extract_scraper_full(
                        black_box(corpus),
                        black_box(&link_selector),
                    ))
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scraper_preparsed_select", corpus.total_bytes),
            &corpus,
            |b, corpus| {
                b.iter(|| {
                    black_box(extract_scraper_preparsed_corpus(
                        black_box(corpus),
                        black_box(&link_selector),
                    ))
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("lol_html_streaming", corpus.total_bytes),
            &corpus,
            |b, corpus| {
                b.iter(|| black_box(extract_lol_html_corpus(black_box(corpus))));
            },
        );
        group.finish();
    }
}

fn benchmarks(c: &mut Criterion) {
    compile_time_guards();
    selector_parse(c);
    extract_links(c);
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
