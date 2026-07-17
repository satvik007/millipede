# millipede-browser

A provider-erased browser abstraction for the Millipede web crawler, including the `BrowserProvider` and `BrowserPage` traits, `BrowserPool`, `BrowserKind`, and smart mode. Concrete providers live in sibling crates.

The core surface is provider-neutral so request handlers can operate on browser pages without depending on a concrete browser driver.
