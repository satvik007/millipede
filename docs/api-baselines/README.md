# Public API baselines

Regenerate the committed baselines with the same pinned toolchain and tool version used by CI:

```sh
rustup toolchain install nightly-2026-07-12 --profile minimal
cargo install --locked cargo-public-api --version 0.52.0
# The umbrella crate needs --all-features so feature-gated re-exports are captured.
cargo +nightly-2026-07-12 public-api -p millipede --all-features --simplified > "docs/api-baselines/millipede.txt"
for c in millipede-core millipede-storage-memory millipede-storage-fs millipede-http millipede-html millipede-browser millipede-browser-chromiumoxide millipede-fingerprint; do
  cargo +nightly-2026-07-12 public-api -p "$c" --simplified > "docs/api-baselines/$c.txt"
done
```
