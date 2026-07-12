//! Crawler configuration and environment-variable resolution.

use crate::events::EventBus;
use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

/// Logging verbosity for crawler diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Disables logging.
    Off,
    /// Emits errors only.
    Error,
    /// Emits warnings and errors.
    Warn,
    /// Emits informational messages and above.
    Info,
    /// Emits debug messages and above.
    Debug,
    /// Emits all trace messages.
    Trace,
}

impl FromStr for LogLevel {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(ConfigError::InvalidLogLevel(value.to_owned())),
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        })
    }
}

/// Errors produced while resolving crawler configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// An environment variable contains an invalid value.
    #[error("invalid value {value:?} for {name}: {message}")]
    InvalidEnvVar {
        /// The environment variable name.
        name: &'static str,
        /// The rejected value.
        value: String,
        /// A description of the required format.
        message: String,
    },
    /// A string is not a supported logging level.
    #[error("invalid log level {0:?}")]
    InvalidLogLevel(String),
}

/// Builds a resolved [`Configuration`].
#[derive(Default, Clone)]
pub struct ConfigurationBuilder {
    default_dataset_id: Option<String>,
    default_key_value_store_id: Option<String>,
    default_request_queue_id: Option<String>,
    storage_dir: Option<PathBuf>,
    max_used_cpu_ratio: Option<f32>,
    available_memory_ratio: Option<f32>,
    memory_bytes: Option<u64>,
    persist_state_interval: Option<Duration>,
    purge_on_start: Option<bool>,
    log_level: Option<LogLevel>,
    storage_client: Option<Arc<dyn crate::storage::StorageClient>>,
}

impl fmt::Debug for ConfigurationBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigurationBuilder")
            .field("default_dataset_id", &self.default_dataset_id)
            .field(
                "default_key_value_store_id",
                &self.default_key_value_store_id,
            )
            .field("default_request_queue_id", &self.default_request_queue_id)
            .field("storage_dir", &self.storage_dir)
            .field("max_used_cpu_ratio", &self.max_used_cpu_ratio)
            .field("available_memory_ratio", &self.available_memory_ratio)
            .field("memory_bytes", &self.memory_bytes)
            .field("persist_state_interval", &self.persist_state_interval)
            .field("purge_on_start", &self.purge_on_start)
            .field("log_level", &self.log_level)
            .field(
                "storage_client",
                &self.storage_client.as_ref().map(|_| "<dyn StorageClient>"),
            )
            .finish()
    }
}

impl ConfigurationBuilder {
    /// Sets the default dataset identifier.
    pub fn default_dataset_id(mut self, value: impl Into<String>) -> Self {
        self.default_dataset_id = Some(value.into());
        self
    }
    /// Sets the default key-value store identifier.
    pub fn default_key_value_store_id(mut self, value: impl Into<String>) -> Self {
        self.default_key_value_store_id = Some(value.into());
        self
    }
    /// Sets the default request queue identifier.
    pub fn default_request_queue_id(mut self, value: impl Into<String>) -> Self {
        self.default_request_queue_id = Some(value.into());
        self
    }
    /// Sets the storage directory.
    pub fn storage_dir(mut self, value: impl Into<PathBuf>) -> Self {
        self.storage_dir = Some(value.into());
        self
    }
    /// Sets the maximum used CPU ratio.
    pub fn max_used_cpu_ratio(mut self, value: f32) -> Self {
        self.max_used_cpu_ratio = Some(value);
        self
    }
    /// Sets the available memory ratio.
    pub fn available_memory_ratio(mut self, value: f32) -> Self {
        self.available_memory_ratio = Some(value);
        self
    }
    /// Sets the memory limit in bytes.
    pub fn memory_bytes(mut self, value: u64) -> Self {
        self.memory_bytes = Some(value);
        self
    }
    /// Sets the interval between state persistence events.
    pub fn persist_state_interval(mut self, value: Duration) -> Self {
        self.persist_state_interval = Some(value);
        self
    }
    /// Sets whether storage is purged at startup.
    pub fn purge_on_start(mut self, value: bool) -> Self {
        self.purge_on_start = Some(value);
        self
    }
    /// Sets the logging verbosity.
    pub fn log_level(mut self, value: LogLevel) -> Self {
        self.log_level = Some(value);
        self
    }

    /// Sets the storage backend client.
    pub fn storage_client(mut self, value: Arc<dyn crate::storage::StorageClient>) -> Self {
        self.storage_client = Some(value);
        self
    }

    /// Resolves this builder using process environment overrides.
    pub fn build(self) -> Result<Configuration, ConfigError> {
        self.build_with_env(|name| std::env::var(name).ok())
    }

    pub(crate) fn build_with_env(
        self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Configuration, ConfigError> {
        let default_dataset_id = self
            .default_dataset_id
            .or_else(|| lookup("CRAWLEE_DEFAULT_DATASET_ID"))
            .unwrap_or_else(|| "default".into());
        let default_key_value_store_id = self
            .default_key_value_store_id
            .or_else(|| lookup("CRAWLEE_DEFAULT_KEY_VALUE_STORE_ID"))
            .unwrap_or_else(|| "default".into());
        let default_request_queue_id = self
            .default_request_queue_id
            .or_else(|| lookup("CRAWLEE_DEFAULT_REQUEST_QUEUE_ID"))
            .unwrap_or_else(|| "default".into());
        let storage_dir = self
            .storage_dir
            .or_else(|| lookup("CRAWLEE_STORAGE_DIR").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("./storage"));
        let max_used_cpu_ratio = match self.max_used_cpu_ratio {
            Some(value) => Some(value),
            None => optional_parse(
                &lookup,
                "CRAWLEE_MAX_USED_CPU_RATIO",
                "expected a floating-point number",
            )?,
        };
        let available_memory_ratio = match self.available_memory_ratio {
            Some(value) => Some(value),
            None => optional_parse(
                &lookup,
                "CRAWLEE_AVAILABLE_MEMORY_RATIO",
                "expected a floating-point number",
            )?,
        };
        let memory_bytes = match self.memory_bytes {
            Some(value) => Some(value),
            None => match optional_parse::<u64>(
                &lookup,
                "CRAWLEE_MEMORY_MBYTES",
                "expected an unsigned integer",
            )? {
                Some(megabytes) => Some(megabytes.checked_mul(1024 * 1024).ok_or_else(|| {
                    ConfigError::InvalidEnvVar {
                        name: "CRAWLEE_MEMORY_MBYTES",
                        value: megabytes.to_string(),
                        message: "value is too large to convert to bytes".into(),
                    }
                })?),
                None => None,
            },
        };
        let persist_state_interval = match self.persist_state_interval {
            Some(value) => value,
            None => Duration::from_millis(
                optional_parse(
                    &lookup,
                    "CRAWLEE_PERSIST_STATE_INTERVAL_MILLIS",
                    "expected an unsigned integer",
                )?
                .unwrap_or(60_000),
            ),
        };
        let purge_on_start = match self.purge_on_start {
            Some(value) => value,
            None => match lookup("CRAWLEE_PURGE_ON_START") {
                Some(value) => parse_bool("CRAWLEE_PURGE_ON_START", value)?,
                None => true,
            },
        };
        let log_level = match self.log_level {
            Some(value) => value,
            None => match lookup("CRAWLEE_LOG_LEVEL") {
                Some(value) => value.parse().map_err(|_| ConfigError::InvalidEnvVar {
                    name: "CRAWLEE_LOG_LEVEL",
                    value,
                    message: "expected off, error, warn, info, debug, or trace".into(),
                })?,
                None => LogLevel::Info,
            },
        };

        Ok(Configuration {
            events: EventBus::default(),
            default_dataset_id,
            default_key_value_store_id,
            default_request_queue_id,
            storage_dir,
            max_used_cpu_ratio,
            available_memory_ratio,
            memory_bytes,
            persist_state_interval,
            purge_on_start,
            log_level,
            storage_client: self.storage_client,
        })
    }
}

fn optional_parse<T: FromStr>(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    message: &str,
) -> Result<Option<T>, ConfigError> {
    lookup(name)
        .map(|value| {
            value.parse().map_err(|_| ConfigError::InvalidEnvVar {
                name,
                value,
                message: message.into(),
            })
        })
        .transpose()
}

fn parse_bool(name: &'static str, value: String) -> Result<bool, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => Err(ConfigError::InvalidEnvVar {
            name,
            value,
            message: "expected 1, true, 0, or false".into(),
        }),
    }
}

/// Fully resolved crawler configuration.
///
/// Unlike the interface's non-optional storage getter, core exposes an optional client because it
/// cannot depend on `millipede-storage-memory` without a dependency cycle. The Phase 2 crawler
/// builder injects the default backend.
pub struct Configuration {
    events: EventBus,
    default_dataset_id: String,
    default_key_value_store_id: String,
    default_request_queue_id: String,
    storage_dir: PathBuf,
    max_used_cpu_ratio: Option<f32>,
    available_memory_ratio: Option<f32>,
    memory_bytes: Option<u64>,
    persist_state_interval: Duration,
    purge_on_start: bool,
    log_level: LogLevel,
    storage_client: Option<Arc<dyn crate::storage::StorageClient>>,
}

impl fmt::Debug for Configuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Configuration")
            .field("events", &self.events)
            .field("default_dataset_id", &self.default_dataset_id)
            .field(
                "default_key_value_store_id",
                &self.default_key_value_store_id,
            )
            .field("default_request_queue_id", &self.default_request_queue_id)
            .field("storage_dir", &self.storage_dir)
            .field("max_used_cpu_ratio", &self.max_used_cpu_ratio)
            .field("available_memory_ratio", &self.available_memory_ratio)
            .field("memory_bytes", &self.memory_bytes)
            .field("persist_state_interval", &self.persist_state_interval)
            .field("purge_on_start", &self.purge_on_start)
            .field("log_level", &self.log_level)
            .field(
                "storage_client",
                &self.storage_client.as_ref().map(|_| "<dyn StorageClient>"),
            )
            .finish()
    }
}

impl Configuration {
    /// Creates an empty configuration builder.
    pub fn builder() -> ConfigurationBuilder {
        ConfigurationBuilder::default()
    }
    /// Returns the crawler event bus.
    pub fn events(&self) -> &EventBus {
        &self.events
    }
    /// Returns the default dataset identifier.
    pub fn default_dataset_id(&self) -> &str {
        &self.default_dataset_id
    }
    /// Returns the default key-value store identifier.
    pub fn default_key_value_store_id(&self) -> &str {
        &self.default_key_value_store_id
    }
    /// Returns the default request queue identifier.
    pub fn default_request_queue_id(&self) -> &str {
        &self.default_request_queue_id
    }
    /// Returns the storage directory.
    pub fn storage_dir(&self) -> &Path {
        &self.storage_dir
    }
    /// Returns the maximum used CPU ratio, when configured.
    pub fn max_used_cpu_ratio(&self) -> Option<f32> {
        self.max_used_cpu_ratio
    }
    /// Returns the available memory ratio, when configured.
    pub fn available_memory_ratio(&self) -> Option<f32> {
        self.available_memory_ratio
    }
    /// Returns the memory limit in bytes, when configured.
    pub fn memory_bytes(&self) -> Option<u64> {
        self.memory_bytes
    }
    /// Returns the interval between state persistence events.
    pub fn persist_state_interval(&self) -> Duration {
        self.persist_state_interval
    }
    /// Returns whether storage is purged at startup.
    pub fn purge_on_start(&self) -> bool {
        self.purge_on_start
    }
    /// Returns the logging verbosity.
    pub fn log_level(&self) -> LogLevel {
        self.log_level
    }
    /// Returns the configured storage client, when one was injected.
    pub fn storage_client(&self) -> Option<&Arc<dyn crate::storage::StorageClient>> {
        self.storage_client.as_ref()
    }
}

impl Default for Configuration {
    fn default() -> Self {
        ConfigurationBuilder::default()
            .build_with_env(|_| None)
            .expect("built-in configuration defaults are valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn with_env(
        builder: ConfigurationBuilder,
        values: &[(&str, &str)],
    ) -> Result<Configuration, ConfigError> {
        let values: HashMap<&str, &str> = values.iter().copied().collect();
        builder.build_with_env(|name| values.get(name).map(|value| (*value).to_owned()))
    }

    #[test]
    fn defaults_are_resolved_without_environment() {
        let config = with_env(Configuration::builder(), &[]).unwrap();
        assert_eq!(config.default_dataset_id(), "default");
        assert_eq!(config.default_key_value_store_id(), "default");
        assert_eq!(config.default_request_queue_id(), "default");
        assert_eq!(config.storage_dir(), Path::new("./storage"));
        assert_eq!(config.persist_state_interval(), Duration::from_secs(60));
        assert!(config.purge_on_start());
        assert_eq!(config.log_level(), LogLevel::Info);
        assert_eq!(config.available_memory_ratio(), None);
        assert_eq!(config.memory_bytes(), None);
    }

    #[test]
    fn environment_overrides_purge_on_start() {
        let config = with_env(
            Configuration::builder(),
            &[("CRAWLEE_PURGE_ON_START", "false")],
        )
        .unwrap();
        assert!(!config.purge_on_start());
    }

    #[test]
    fn environment_memory_megabytes_are_converted_to_bytes() {
        let config = with_env(
            Configuration::builder(),
            &[("CRAWLEE_MEMORY_MBYTES", "512")],
        )
        .unwrap();
        assert_eq!(config.memory_bytes(), Some(512 * 1024 * 1024));
    }

    #[test]
    fn environment_max_used_cpu_ratio_is_resolved() {
        let config = with_env(
            Configuration::builder(),
            &[("CRAWLEE_MAX_USED_CPU_RATIO", "0.75")],
        )
        .unwrap();
        assert_eq!(config.max_used_cpu_ratio(), Some(0.75));
    }

    #[test]
    fn environment_persist_interval_is_parsed_as_milliseconds() {
        let config = with_env(
            Configuration::builder(),
            &[("CRAWLEE_PERSIST_STATE_INTERVAL_MILLIS", "5000")],
        )
        .unwrap();
        assert_eq!(config.persist_state_interval(), Duration::from_secs(5));
    }

    #[test]
    fn environment_log_level_is_case_insensitive() {
        let config = with_env(Configuration::builder(), &[("CRAWLEE_LOG_LEVEL", "debug")]).unwrap();
        assert_eq!(config.log_level(), LogLevel::Debug);
    }

    #[test]
    fn builder_value_beats_environment() {
        let config = with_env(
            Configuration::builder().purge_on_start(true),
            &[("CRAWLEE_PURGE_ON_START", "false")],
        )
        .unwrap();
        assert!(config.purge_on_start());
    }

    #[test]
    fn invalid_float_identifies_environment_variable() {
        let error = with_env(
            Configuration::builder(),
            &[("CRAWLEE_AVAILABLE_MEMORY_RATIO", "nope")],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigError::InvalidEnvVar {
                name: "CRAWLEE_AVAILABLE_MEMORY_RATIO",
                ..
            }
        ));
    }
}
