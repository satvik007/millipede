# Release Process

## Versioning

Millipede follows SemVer with `0.x` releases until 1.0. A `0.x.0` release may break the public API; a `0.x.y` release must not.

## API discipline

Starting at the close of Phase 1, a `cargo public-api` baseline is captured. CI runs `cargo public-api` and `cargo semver-checks` on every pull request that touches public API. Non-additive changes must update the baseline with an explanatory pull-request note or be reworked.

### Phase 8 semver baseline

Run the in-phase compatibility check against the commit that closed the wave 1 API audit:

```console
cargo install cargo-semver-checks --locked
scripts/semver-baseline.sh <s1-audit-commit>
```

The script runs `cargo semver-checks check-release --workspace --baseline-rev <s1-audit-commit>`. It must be clean: no public API change may land after wave 1. Once the `v0.1.0` tag exists, use that tag as the baseline for all `0.1.x` releases.

**Result recorded at phase close by the lead:** _pending host run; `cargo-semver-checks` requires a network install and is not present in sandboxes._

## Publish checklist

For version 0.1.0 and later:

1. Confirm CI is green on `main`.
2. Add the release to `CHANGELOG.md`.
3. Confirm all crates remain permanently publishable (the manifests have been publishable since `0.1.0-rc`), then run `cargo package --workspace` before every release.
4. Run `scripts/semver-baseline.sh <BASELINE_REV>` and record the result above.
5. Run `scripts/validate-templates.sh` to verify every local `cargo-generate` starter.
6. Review and finalize the announcement draft in `docs/announcement-0.1.0.md`.
7. Publish in this topological order: `millipede-core`, `millipede-fingerprint`, `millipede-storage-memory`, `millipede-storage-fs`, `millipede-http`, `millipede-html`, `millipede-browser`, `millipede-browser-chromiumoxide`, then `millipede`. Dependents are published after their dependencies.
8. Create the git tag `vX.Y.Z` and push the release changes and tag.

Actual `cargo publish`, git tags, and pushes require the maintainer. They were **not executed during Phase 8**.

## Local packaging proxies

Use both workspace-aware checks before asking a maintainer to publish:

```console
# Workspace-aware packaging (Cargo 1.89+, verified with Cargo 1.96).
cargo package --workspace

# One dependency-aware dry-run invocation for the complete release set.
cargo publish --dry-run \
  -p millipede-core \
  -p millipede-fingerprint \
  -p millipede-storage-memory \
  -p millipede-storage-fs \
  -p millipede-http \
  -p millipede-html \
  -p millipede-browser \
  -p millipede-browser-chromiumoxide \
  -p millipede
```

A per-crate `cargo package -p <non-leaf>` or `cargo publish --dry-run -p <non-leaf>` invocation fails before publication with `no matching package found ... crates.io index` because its Millipede dependencies are not published yet. This is expected, not a bug; use the workspace package command or the single multi-`-p` dry-run above.

## Supply chain

Both cargo-deny and cargo-audit run on every PR through `ci.yml`. A nightly scheduled workflow re-runs both against the fresh advisory database.

## License policy

Dependency licenses must be on the `deny.toml` allowlist: MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0, or Zlib. Adding a license requires a pull request that edits `deny.toml` with justification.
