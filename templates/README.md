# Millipede starter templates

These are three self-contained [`cargo-generate`](https://cargo-generate.github.io/cargo-generate/) starters for Millipede's HTTP, HTML, and Chromium-backed browser crawlers.

Once the standalone template repository exists, generate a starter with:

```sh
cargo generate --git https://github.com/satvik007/millipede-template basic-http
```

For local development in this repository, point `cargo-generate` directly at a template:

```sh
cargo generate --path templates/basic-http --name mycrawler
```

Each generated `Cargo.toml` includes a commented-out `[patch.crates-io]` example. Uncomment it and set its path to the `millipede/` umbrella crate in a local Millipede checkout when testing before publication. This patch is a local-development mechanism only and **must be removed once Millipede 0.1.0 is published on crates.io**.

Run `scripts/validate-templates.sh` from any directory to smoke-check all three templates.
