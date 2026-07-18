//! HTML link extraction micro-benchmarks.
#![allow(missing_docs)]

use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use millipede_core::link_extraction::LinkExtractor;
use millipede_html::HtmlLinkExtractor;
use url::Url;

fn href(index: usize) -> String {
    match index % 3 {
        0 => format!("/relative/{index}"),
        1 => format!("https://example.com/same-domain/{index}"),
        _ => format!("https://off-domain.test/item/{index}"),
    }
}

fn document(link_count: usize) -> Arc<scraper::Html> {
    let mut html = String::from("<!doctype html><html><head></head><body>");
    for index in 0..link_count {
        html.push_str("<a href=\"");
        html.push_str(&href(index));
        html.push_str("\">link</a>");
    }
    html.push_str("</body></html>");
    #[allow(clippy::arc_with_non_send_sync)]
    Arc::new(scraper::Html::parse_document(&html))
}

fn link_extraction(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let document_url = Url::parse("https://example.com/catalog/page").unwrap();
    let full_document = document(1_000);
    let subset_document = document(20);
    let mut group = c.benchmark_group("link_extraction");

    group.throughput(Throughput::Elements(1_000));
    group.bench_function("full_1000_links", |b| {
        b.to_async(&runtime).iter_batched(
            || (Arc::clone(&full_document), document_url.clone()),
            |(document, document_url)| async move {
                let extractor = HtmlLinkExtractor::new(document, black_box(document_url));
                let links = black_box(extractor.extract(None).await.unwrap());
                (extractor, links)
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(20));
    group.bench_function("subset_20_links", |b| {
        b.to_async(&runtime).iter_batched(
            || (Arc::clone(&subset_document), document_url.clone()),
            |(document, document_url)| async move {
                let extractor = HtmlLinkExtractor::new(document, black_box(document_url));
                let links = black_box(extractor.extract(None).await.unwrap());
                (extractor, links)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, link_extraction);
criterion_main!(benches);
