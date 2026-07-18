# The `millipede-extras` Policy

## Purpose

`millipede-extras` is the future community crate for useful helpers that are deliberately kept outside Millipede's core crates. The crate itself will ship after 1.0, but this policy ships now so contributors know where to propose and develop helpers without expanding the core API prematurely.

## Scope

Extras may contain helpers such as `infinite_scroll`, `save_snapshot`, `enqueue_links_by_click_elements`, and reusable login-flow utilities. A suitable helper composes Millipede's public API and does not need access to core internals.

## Out of Scope

Changes that require new core trait surface do not belong in extras. Alternate implementations of storage, HTTP, browser, or other backends also belong in dedicated backend crates rather than `millipede-extras`.

## Semver Bar

`millipede-extras` is versioned independently from the core Millipede crates. During its 0.x series, minor releases may contain breaking changes, and extras is not subject to public API baseline gating. Core retains the stricter compatibility bar described in [`docs/RELEASE.md`](../RELEASE.md).

## Governance

The crate will live under the Millipede organization. A change requires approval from one maintainer, tests, and a documentation example. Its CI mirrors the core repository's gates except for the public API diffing step.

## Promotion Path to Core

A helper may be proposed for core after it demonstrates demand, maintains a stable API across two extras releases, and has tests that meet core's bar. Promotion happens through an RFC-style pull request that explains the use cases, API, compatibility impact, and evidence of adoption. After acceptance, extras re-exports the core implementation for one release and then removes its own copy.

## Where to Send Pull Requests Today

Until the crate exists, open an issue on the Millipede repository describing the helper and its intended public-API composition. The maintainers will use that issue to decide whether the proposal should wait for `millipede-extras` or belongs elsewhere.
