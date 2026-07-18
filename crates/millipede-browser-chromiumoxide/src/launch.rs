//! Chromium process launch options.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

/// Options used to launch one Chromium process.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use = "launch options do nothing unless passed to a browser builder"]
pub struct ChromiumLaunchOptions {
    /// Explicit browser executable, bypassing discovery.
    pub executable: Option<PathBuf>,
    /// Whether to run without a visible browser window.
    pub headless: bool,
    /// Additional Chromium command-line arguments.
    pub args: Vec<String>,
    /// Persistent profile directory. `None` creates a fresh temporary profile per browser.
    pub user_data_dir: Option<PathBuf>,
    /// Browser viewport dimensions.
    pub window_size: Option<(u32, u32)>,
    /// Timeout applied to CDP requests.
    pub request_timeout: Duration,
    /// Maximum time allowed for Chromium startup.
    pub launch_timeout: Duration,
}

impl ChromiumLaunchOptions {
    /// Sets the browser executable.
    pub fn with_executable(mut self, executable: impl Into<PathBuf>) -> Self {
        self.executable = Some(executable.into());
        self
    }

    /// Sets headless operation.
    pub fn with_headless(mut self, headless: bool) -> Self {
        self.headless = headless;
        self
    }

    /// Replaces additional Chromium arguments.
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    /// Appends one additional Chromium argument.
    pub fn with_arg(mut self, argument: impl Into<String>) -> Self {
        self.args.push(argument.into());
        self
    }

    /// Sets a persistent profile directory.
    pub fn with_user_data_dir(mut self, user_data_dir: impl Into<PathBuf>) -> Self {
        self.user_data_dir = Some(user_data_dir.into());
        self
    }

    /// Sets browser window dimensions.
    pub fn with_window_size(mut self, width: u32, height: u32) -> Self {
        self.window_size = Some((width, height));
        self
    }

    /// Sets the CDP request timeout.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Sets the browser launch timeout.
    pub fn with_launch_timeout(mut self, launch_timeout: Duration) -> Self {
        self.launch_timeout = launch_timeout;
        self
    }

    pub(crate) fn executable_path(&self) -> Option<&Path> {
        self.executable.as_deref()
    }

    pub(crate) fn is_headless(&self) -> bool {
        self.headless
    }

    pub(crate) fn additional_args(&self) -> &[String] {
        &self.args
    }

    pub(crate) fn profile_path(&self) -> Option<&Path> {
        self.user_data_dir.as_deref()
    }

    pub(crate) fn viewport(&self) -> Option<(u32, u32)> {
        self.window_size
    }

    pub(crate) fn cdp_request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub(crate) fn browser_launch_timeout(&self) -> Duration {
        self.launch_timeout
    }
}

impl Default for ChromiumLaunchOptions {
    fn default() -> Self {
        Self {
            executable: None,
            headless: true,
            args: Vec::new(),
            user_data_dir: None,
            window_size: None,
            request_timeout: Duration::from_secs(30),
            launch_timeout: Duration::from_secs(20),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChromiumLaunchOptions;
    use std::time::Duration;

    fn assert_launch_options<T: Default + Clone + Send + Sync + 'static>() {}

    #[test]
    fn launch_options_satisfy_provider_contract() {
        assert_launch_options::<ChromiumLaunchOptions>();
        let options = ChromiumLaunchOptions::default();
        assert!(options.is_headless());
        assert!(options.executable_path().is_none());
        assert!(options.additional_args().is_empty());
        assert!(options.profile_path().is_none());
    }

    #[test]
    fn chainable_setters_update_state() {
        let options = ChromiumLaunchOptions::default()
            .with_executable("/browser")
            .with_headless(false)
            .with_arg("first")
            .with_args(["second", "third"])
            .with_user_data_dir("/profile")
            .with_window_size(1024, 768)
            .with_request_timeout(Duration::from_secs(12))
            .with_launch_timeout(Duration::from_secs(34));

        assert_eq!(
            options.executable_path(),
            Some(std::path::Path::new("/browser"))
        );
        assert!(!options.is_headless());
        assert_eq!(options.additional_args(), ["second", "third"]);
        assert_eq!(
            options.profile_path(),
            Some(std::path::Path::new("/profile"))
        );
        assert_eq!(options.viewport(), Some((1024, 768)));
        assert_eq!(options.cdp_request_timeout(), Duration::from_secs(12));
        assert_eq!(options.browser_launch_timeout(), Duration::from_secs(34));
    }
}
