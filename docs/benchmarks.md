# Micro-benchmark baseline

These micro-benchmarks are for relative regression tracking only. They are **not absolute
performance claims or service-level agreements (SLAs)**. Results vary with hardware, operating
system activity, compiler version, and benchmark configuration.

## Commands

The baseline was collected with these reduced-sample commands:

```text
cargo bench -p millipede-storage-memory --bench queue_ops -- --sample-size 10 --warm-up-time 1 --measurement-time 3
cargo bench -p millipede-core --bench engine_overhead -- --sample-size 10 --warm-up-time 1 --measurement-time 5
cargo bench -p millipede-html --bench link_extraction -- --sample-size 10 --warm-up-time 1 --measurement-time 3
```

## Machine and source context

- Architecture (`uname -m`): `arm64`
- CPU: `Apple M2 Pro` (from `system_profiler SPHardwareDataType`). The managed benchmark
  environment denied `sysctl -n machdep.cpu.brand_string` with `Operation not permitted`.
- Rust compiler: `rustc 1.96.1 (31fca3adb 2026-06-26)`
- Git commit: `4fe557a122d26104a9fe89e7ca63b7a6155acf9c`

## Results

Criterion's reported interval is shown as `[lower, point estimate, upper]`. Derived rates and
per-element times use the point estimate. An element is one queue operation, one handled crawler
request, or one extracted link, according to the benchmark.

| bench id | time/iter | derived ops/sec or ns/request |
| --- | ---: | ---: |
| `queue_ops/enqueue_1000_unique` | `[397.25, 420.80, 433.50] µs` | `2.3764 M ops/s` (`420.80 ns/request`) |
| `queue_ops/dedup_hit` | `[794.39, 887.02, 928.69] ns` | `1.1274 M ops/s` (`887.02 ns/request`) |
| `queue_ops/lease_cycle` | `[546.73, 548.55, 549.71] µs` | `1.8230 M ops/s` (`548.55 ns/request`) |
| `engine_overhead/run_200` | `[933.40 µs, 986.02 µs, 1.0882 ms]` | `202.84 K requests/s` (`4,930.1 ns/request`) |
| `link_extraction/full_1000_links` | `[157.10, 164.25, 168.99] µs` | `6.0885 M links/s` (`164.25 ns/link`) |
| `link_extraction/subset_20_links` | `[4.8619, 5.0686, 5.3654] µs` | `3.9458 M links/s` (`253.43 ns/link`) |
