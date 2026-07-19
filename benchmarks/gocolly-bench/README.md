# gocolly benchmark child

This standalone Go program plugs into `benchmarks/spider-bench`'s
`ready` / `TrialWire JSON` / `go` child protocol and emits the exact Rust
`Sample` JSON shape with `engine: "gocolly"`.

Build and test:

```sh
cd benchmarks/gocolly-bench
go test ./...
go build -trimpath -o target/gocolly-bench .
```

The parent invokes it with `--scenario`, `--url`, and `--concurrency` (an
optional leading `run` and the Rust compatibility flags are accepted). The
timed interval includes Collector/client construction, crawl, and callback
drain. Configuration is fixed to async mode, unlimited depth, concurrency C,
zero delay, no retries, same-hostname traversal, a 15-second request timeout,
seven redirects, no robots handling, and `millipede-bench/1.0` as user agent.

Raw scenarios do only body accounting plus Colly's native link parsing.
`books` and `hn` use goquery selectors and serialize records exactly like the
Rust scenario extractors. Checksums and record digests use a local compatible
implementation of Rust `seahash` 4.1.0. Peak RSS and timed-region user/system
CPU (final usage minus the `go` checkpoint) come from
`getrusage(RUSAGE_SELF)`; RSS is normalized to bytes on macOS and Linux.
