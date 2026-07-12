# Release Process

## Versioning

Millipede follows SemVer with `0.x` releases until 1.0. A `0.x.0` release may break the public API; a `0.x.y` release must not.

## API discipline

Starting at the close of Phase 1, a `cargo public-api` baseline is captured. CI runs `cargo public-api` and `cargo semver-checks` on every pull request that touches public API. Non-additive changes must update the baseline with an explanatory pull-request note or be reworked.

## Publish checklist

For version 0.1.0 and later:

1. Confirm CI is green on `main`.
2. Add the release to `CHANGELOG`.
3. Flip `publish = false` to make the release crates publishable.
4. Publish in this order: `millipede-core`, `millipede-storage-memory`, `millipede-storage-fs`, `millipede-http`, `millipede-html`, `millipede-browser`, `millipede-browser-chromiumoxide`, `millipede-fingerprint`, then `millipede`. Dependents are published last.
5. Create the git tag `vX.Y.Z`.

## Supply chain

Both cargo-deny and cargo-audit run on every PR through `ci.yml`. A nightly scheduled workflow re-runs both against the fresh advisory database.

## License policy

Dependency licenses must be on the `deny.toml` allowlist: MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0, or Zlib. Adding a license requires a pull request that edits `deny.toml` with justification.
