# Millipede vs Spider vs Colly vs Crawlee

**Date:** 2026-07-19 · **Machine:** Apple M2 Pro (10 cores, 32 GiB), macOS
Darwin 24.6.0 · **Concurrency:** 32 · **Trials:** median of 5 valid fresh-process
runs after one warm-up per engine

Versions: millipede workspace `0.1.x`, spider `2.52.9` (`sync` only), Colly
`2.3.0` on Go `1.26.0`, Crawlee `3.17.0` on Node `24.15.0`. Rust and Go were
capped at four scheduler workers; Crawlee's Cheerio handler/parsing work ran on
Node's single JavaScript thread. Every measured trial passed exact page-count,
decoded-byte, checksum, extraction-digest, dedup, robots, off-host, and server
hit-set validation.

## Throughput

| scenario | pages | millipede | spider | Colly | Crawlee | fastest crawler |
|---|---:|---:|---:|---:|---:|---|
| `books` (DOM extraction) | 5,100 | **32,511 p/s** | 16,512 p/s | 25,687 p/s | 973 p/s | millipede |
| `hn` (DOM extraction) | 1,040 | **7,242 p/s** | 2,394 p/s | 7,091 p/s | 341 p/s | millipede / Colly tie¹ |
| `tree` | 8,191 | 38,006 p/s | **60,749 p/s** | 31,803 p/s | 1,127 p/s | spider |
| `wide` | 5,001 | 47,229 p/s | **60,707 p/s** | 38,240 p/s | 537 p/s | spider |
| `mesh` (dedup stress) | 8,192 | 33,764 p/s | **56,327 p/s** | 23,651 p/s | 1,050 p/s | spider |
| `payload` (256 KiB/page) | 1,023 | 1,132 p/s | **11,942 p/s** | 2,924 p/s | 254 p/s | spider |
| `compressed` (gzip) | 4,095 | 4,354 p/s | **20,353 p/s** | 4,784 p/s | 328 p/s | spider |
| `latency` (10 ms response delay)² | 2,047 | 1,966 p/s | 1,983 p/s | **2,222 p/s** | 669 p/s | server-bound |
| `redirects`² ³ | 2,047 | **30,799 p/s** | N/A | 21,943 p/s | 859 p/s | server-bound |

¹ The 2.1% gap is below the suite's 5% noise threshold.

² The fastest crawler reached at least 70% of the raw-client ceiling, so this
row is useful as a pipeline/behavior check but not a publishable framework
speed claim without redesigning or scaling the scenario.

³ Spider 2.52.9 rejects redirects to loopback/private addresses through its
SSRF guard, so the offline redirect scenario cannot exercise Spider.

## Peak RSS

| scenario | millipede | spider | Colly | Crawlee |
|---|---:|---:|---:|---:|
| `books` | **17.9 MiB** | 45.8 MiB | 22.7 MiB | 484.7 MiB |
| `hn` | **22.0 MiB** | 55.1 MiB | **22.0 MiB** | 450.8 MiB |
| `tree` | **21.7 MiB** | 62.9 MiB | 61.1 MiB | 515.1 MiB |
| `payload` | **55.8 MiB** | 291.4 MiB | 126.1 MiB | 569.3 MiB |
| `compressed` | **28.7 MiB** | 151.4 MiB | 46.3 MiB | 496.1 MiB |

## Interpretation

- Millipede wins the realistic `books` extraction workload by 1.27× over
  Colly, 1.97× over Spider, and 33.4× over Crawlee. On `hn`, Millipede and
  Colly are effectively tied.
- Spider is the raw-fetch/link-discovery leader. Its streaming link parser is
  especially strong on large payloads and gzip, while Millipede's full DOM is
  costly there.
- Colly is competitive and memory-efficient after configuring a concurrency-
  sized HTTP connection pool. Its asynchronous built-in visited check races on
  dense frontiers, so the runner uses an atomic admission set to guarantee the
  exact-once contract.
- Crawlee/CheerioCrawler is much slower and uses substantially more memory in
  this synthetic loopback suite. These rows emphasize framework, JavaScript
  parsing, queue, and runtime overhead; they should not be extrapolated to
  latency-heavy internet crawls without a separate live-network study.

The scope is HTTP/1.1 loopback success paths. These results do not characterize
TLS, DNS, retries/errors, anti-bot behavior, JavaScript rendering, or politeness.
Raw samples and the generated detailed report for this run are under
`benchmarks/spider-bench/target/results/1784472890-*` in the machine that ran
the suite.
