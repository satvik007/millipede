# Crawlee benchmark child

This directory contains the Node.js/Crawlee child runner used by the
`spider-bench` orchestration protocol. It uses `CheerioCrawler` (no browser),
fixed concurrency, an in-memory non-persistent request queue, no retries,
robots disabled, zero crawl delays, same-hostname link admission, a 15-second
navigation timeout, and a seven-hop redirect limit.

The runner prints `ready`, reads one `TrialWire` JSON line and then `go` from
stdin, and emits the same `Sample` JSON schema as the Rust children. Crawler
construction and complete queue drain are inside the timed region. Decoded
body checksums and extraction digests are byte-for-byte compatible with Rust's
`seahash` crate; 64-bit wire integers are parsed without IEEE-754 precision
loss.

Install and test:

```sh
npm ci
npm test
```

Run (normally launched by the orchestrator):

```sh
node runner.mjs --scenario tree \
  --url http://127.0.0.1:PORT/NONCE/p/0 --concurrency 32 \
  --nonce NONCE --json
```

`--nonce`, `--depth`, `--runtime-workers`, and `--json` are accepted for CLI
compatibility. Node controls its own event-loop worker model, so
`--runtime-workers` is intentionally not applied.
