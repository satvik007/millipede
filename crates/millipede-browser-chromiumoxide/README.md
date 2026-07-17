# millipede-browser-chromiumoxide

Chromium CDP browser provider for the Millipede web crawler, via chromiumoxide.

Browser discovery checks `MILLIPEDE_CHROME`, then `CHROME`, then conventional Chrome and Chromium
installation paths for the current platform. An explicitly configured path is returned even when
it is missing so launch reports the configuration error instead of silently selecting a different
browser.

The chromiumoxide dependency is pinned to exactly `=0.9.1` as required by ADR-0006. Its generated
CDP surface is version-sensitive, so upgrades require a dedicated compatibility review and spike.

```text
MILLIPEDE_CHROME=/path/to/chrome cargo test -p millipede-browser-chromiumoxide
```
