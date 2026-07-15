//! Proxy rotation, tiering, and routing integration tests.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use millipede_core::{
    errors::CrawlError,
    proxy::{
        ProxyBuckets, ProxyConfiguration, ProxyInfo, ProxyKind, ProxyResolveContext, ProxyResolver,
        ProxyRouteContext, ProxyStrategy, RotationStrategy,
    },
    request::Request,
    session::SessionId,
};
use url::Url;

fn request(url: &str) -> Request {
    Request::get(url).build().unwrap()
}

#[tokio::test]
async fn round_robin_distribution_is_stable() {
    let urls = (0..3)
        .map(|n| Url::parse(&format!("http://proxy{n}.example")).unwrap())
        .collect::<Vec<_>>();
    let config = ProxyConfiguration::round_robin(urls.clone());
    let mut selected = Vec::new();
    for _ in 0..10 {
        selected.push(
            config
                .new_url(ProxyResolveContext::new())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(&selected[..3], &urls);
    let counts = selected
        .into_iter()
        .fold(HashMap::new(), |mut counts, url| {
            *counts.entry(url).or_insert(0) += 1;
            counts
        });
    assert_eq!(counts[&urls[0]], 4);
    assert_eq!(counts[&urls[1]], 3);
    assert_eq!(counts[&urls[2]], 3);
}

#[tokio::test]
async fn random_rotation_reaches_every_proxy() {
    let urls = (0..3)
        .map(|n| Url::parse(&format!("http://proxy{n}.example")).unwrap())
        .collect::<Vec<_>>();
    let config = ProxyConfiguration::rotating(urls.clone(), RotationStrategy::Random);
    let mut seen = Vec::new();
    for _ in 0..100 {
        let url = config
            .new_url(ProxyResolveContext::new())
            .await
            .unwrap()
            .unwrap();
        if !seen.contains(&url) {
            seen.push(url);
        }
    }
    assert_eq!(seen.len(), urls.len());
}

struct RecordingResolver {
    seen: Arc<Mutex<Vec<(String, String, u32)>>>,
    url: Url,
}

#[async_trait::async_trait]
impl ProxyResolver for RecordingResolver {
    async fn resolve(&self, ctx: ProxyResolveContext<'_>) -> Result<Option<Url>, CrawlError> {
        self.seen.lock().unwrap().push((
            ctx.request.unwrap().url.to_string(),
            ctx.session_id.unwrap().to_string(),
            ctx.attempt,
        ));
        Ok(Some(self.url.clone()))
    }
}

struct FailingResolver;
#[async_trait::async_trait]
impl ProxyResolver for FailingResolver {
    async fn resolve(&self, _: ProxyResolveContext<'_>) -> Result<Option<Url>, CrawlError> {
        Err(CrawlError::retry(anyhow::anyhow!("resolver failed")))
    }
}

#[tokio::test]
async fn custom_resolver_receives_context_and_result_flows() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let proxy = Url::parse("http://proxy.example:8080").unwrap();
    let config = ProxyConfiguration::custom(RecordingResolver {
        seen: Arc::clone(&seen),
        url: proxy.clone(),
    });
    let req = request("https://target.example/path");
    let session_id = SessionId::generate();
    let info = config
        .new_proxy_info(
            ProxyResolveContext::new()
                .request(&req)
                .session_id(&session_id)
                .attempt(3),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.url, proxy);
    assert_eq!(info.session_id.as_ref(), Some(&session_id));
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, req.url.as_str());
        assert_eq!(seen[0].1, session_id.as_str());
        assert_eq!(seen[0].2, 3);
    }
    assert!(
        ProxyConfiguration::custom(FailingResolver)
            .new_url(ProxyResolveContext::new())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn tiered_escalation_probing_and_domains_are_independent() {
    let t0 = Url::parse("http://tier0.example").unwrap();
    let t1 = Url::parse("http://tier1.example").unwrap();
    let config = ProxyConfiguration::tiered_with_probe_interval(
        vec![vec![Some(t0.clone())], vec![Some(t1.clone())]],
        2,
    );
    let first = request("https://one.example/a");
    let other = request("https://two.example/a");
    assert_eq!(
        config
            .new_url(ProxyResolveContext::new().request(&first))
            .await
            .unwrap(),
        Some(t0.clone())
    );
    config.report_blocked(&first.url);
    assert_eq!(
        config
            .new_url(ProxyResolveContext::new().request(&first))
            .await
            .unwrap(),
        Some(t1.clone())
    );
    assert_eq!(
        config
            .new_url(ProxyResolveContext::new().request(&first))
            .await
            .unwrap(),
        Some(t0.clone())
    );
    config.report_success(&first.url);
    assert_eq!(
        config
            .new_url(ProxyResolveContext::new().request(&first))
            .await
            .unwrap(),
        Some(t0.clone())
    );
    assert_eq!(
        config
            .new_url(ProxyResolveContext::new().request(&other))
            .await
            .unwrap(),
        Some(t0)
    );
}

#[tokio::test]
async fn none_tier_slot_selects_direct_connection() {
    let config = ProxyConfiguration::tiered(vec![vec![None]]);
    assert_eq!(
        config.new_url(ProxyResolveContext::new()).await.unwrap(),
        None
    );
}

#[test]
fn proxy_info_parses_credentials_and_endpoint() {
    let info = ProxyInfo::from_url(Url::parse("http://user:secret@proxy.example:8080").unwrap());
    assert_eq!(info.hostname, "proxy.example");
    assert_eq!(info.port, 8080);
    assert_eq!(info.username.as_deref(), Some("user"));
    assert_eq!(info.password.as_deref(), Some("secret"));
}

#[test]
fn buckets_use_documented_fallbacks() {
    let default_url = Url::parse("http://default.example").unwrap();
    let buckets = ProxyBuckets::new().with_default(ProxyConfiguration::round_robin([default_url]));
    let default = buckets.for_kind(&ProxyKind::Default).unwrap() as *const _;
    assert_eq!(
        buckets.for_kind(&ProxyKind::MediaAsset).unwrap() as *const _,
        default
    );
    assert_eq!(
        buckets
            .for_kind(&ProxyKind::Custom("missing".into()))
            .unwrap() as *const _,
        default
    );
    assert!(ProxyBuckets::new().for_kind(&ProxyKind::Default).is_none());
}

struct MediaStrategy;
impl ProxyStrategy for MediaStrategy {
    fn route(&self, ctx: &ProxyRouteContext<'_>) -> ProxyKind {
        match ctx
            .request
            .url
            .path()
            .rsplit_once('.')
            .map(|(_, extension)| extension)
        {
            Some("jpg" | "png") => ProxyKind::MediaAsset,
            _ => ProxyKind::Default,
        }
    }
}

#[test]
fn strategy_routes_media_extensions() {
    let strategy = MediaStrategy;
    assert_eq!(
        strategy.route(&ProxyRouteContext::new(
            &request("https://example.com/image.jpg"),
            0
        )),
        ProxyKind::MediaAsset
    );
    assert_eq!(
        strategy.route(&ProxyRouteContext::new(
            &request("https://example.com/page"),
            0
        )),
        ProxyKind::Default
    );
}
