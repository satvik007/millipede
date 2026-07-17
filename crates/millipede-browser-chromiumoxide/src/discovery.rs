//! Browser executable discovery.

use std::path::{Path, PathBuf};

/// Finds a supported Chromium or Google Chrome executable.
///
/// `MILLIPEDE_CHROME` has priority over `CHROME`. An explicitly configured path is returned even
/// when it does not exist, so launch fails loudly instead of silently selecting another browser.
/// Without either variable, conventional platform paths are probed in a stable order.
pub fn find_browser() -> Option<PathBuf> {
    discover_with(|variable| std::env::var(variable).ok(), Path::is_file)
}

pub(crate) fn discover_with(
    env: impl Fn(&str) -> Option<String>,
    exists: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    for variable in ["MILLIPEDE_CHROME", "CHROME"] {
        if let Some(configured) = env(variable) {
            let path = PathBuf::from(configured);
            if !exists(&path) {
                tracing::warn!(
                    variable,
                    path = %path.display(),
                    "configured browser executable does not exist"
                );
            }
            return Some(path);
        }
    }

    platform_candidates()
        .iter()
        .map(PathBuf::from)
        .find(|path| exists(path))
}

#[cfg(target_os = "macos")]
const fn platform_candidates() -> &'static [&'static str] {
    &[
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
    ]
}

#[cfg(target_os = "linux")]
const fn platform_candidates() -> &'static [&'static str] {
    &[
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium",
    ]
}

#[cfg(target_os = "windows")]
const fn platform_candidates() -> &'static [&'static str] {
    &[
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const fn platform_candidates() -> &'static [&'static str] {
    &[]
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::Path};

    use super::{discover_with, platform_candidates};

    #[test]
    fn millipede_chrome_wins_over_chrome() {
        let vars = HashMap::from([
            ("MILLIPEDE_CHROME", "/custom/millipede"),
            ("CHROME", "/custom/chrome"),
        ]);
        let found = discover_with(|name| vars.get(name).map(ToString::to_string), |_| true);
        assert_eq!(found.as_deref(), Some(Path::new("/custom/millipede")));
    }

    #[test]
    fn configured_path_is_returned_when_missing() {
        let found = discover_with(
            |name| (name == "MILLIPEDE_CHROME").then(|| "/missing/chrome".to_owned()),
            |_| false,
        );
        assert_eq!(found.as_deref(), Some(Path::new("/missing/chrome")));
    }

    #[test]
    fn first_existing_probe_is_returned() {
        let Some(expected) = platform_candidates().first() else {
            return;
        };
        let found = discover_with(|_| None, |path| path == Path::new(expected));
        assert_eq!(found.as_deref(), Some(Path::new(expected)));
    }

    #[test]
    fn later_existing_probe_is_returned_after_missing_candidates() {
        let Some(expected) = platform_candidates().get(1) else {
            return;
        };
        let found = discover_with(
            |name| {
                assert!(matches!(name, "MILLIPEDE_CHROME" | "CHROME"));
                None
            },
            |path| path == Path::new(expected),
        );
        assert_eq!(found.as_deref(), Some(Path::new(expected)));
    }

    #[test]
    fn no_configuration_or_probe_returns_none() {
        assert_eq!(discover_with(|_| None, |_| false), None);
    }
}
