use std::{collections::HashSet, time::Duration};

use futures::FutureExt;
use vector_lib::{
    configurable::configurable_component, lookup::lookup_v2::ConfigTargetPath,
    sensitive_string::SensitiveString,
};

use crate::{
    config::ValidatedSink,
    sinks::{fluss::sink::FlussSink, prelude::*},
};

/// Fluss data type for a column mapping.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FlussColumnType {
    /// Boolean value.
    Boolean,
    /// 8-bit signed integer.
    Tinyint,
    /// 16-bit signed integer.
    Smallint,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    Bigint,
    /// 32-bit floating point.
    Float,
    /// 64-bit floating point.
    Double,
    /// Decimal with precision and scale.
    Decimal,
    /// UTF-8 string.
    String,
    /// Fixed-length string.
    Char,
    /// Raw byte array.
    Bytes,
    /// Fixed-length byte array.
    Binary,
    /// Date (days since epoch).
    Date,
    /// Time (milliseconds since midnight).
    Time,
    /// Timestamp without timezone (milliseconds since epoch).
    Timestamp,
    /// Timestamp with local timezone (milliseconds since epoch).
    #[serde(alias = "timestamp_ltz")]
    TimestampLtz,
    /// Array type (currently unsupported; rejected rather than serialized as a string).
    Array,
    /// Map type (currently unsupported; rejected rather than serialized as a string).
    Map,
}

/// Defines how a Vector event field maps to a Fluss table column.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ColumnMapping {
    /// Path to the field in the Vector event (e.g., ".message", ".timestamp").
    #[configurable(metadata(docs::examples = ".message", docs::examples = ".timestamp"))]
    pub from_field: ConfigTargetPath,

    /// Column name in the Fluss table.
    #[configurable(metadata(docs::examples = "message", docs::examples = "event_time"))]
    pub to_column: String,

    /// Fluss data type for this column.
    pub data_type: FlussColumnType,
}

/// Schema mode for the sink.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaMode {
    /// Manually specify column mappings.
    Manual,
    /// Auto-discover schema from Fluss table metadata.
    /// Fields are mapped by name; types are inferred from the table schema.
    Auto,
}

/// Configuration for the `fluss` sink.
#[configurable_component(sink(
    "fluss",
    "Publish observability event data to Apache Fluss log tables."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FlussSinkConfig {
    /// Fluss coordinator bootstrap server address.
    ///
    /// Must be in the form of `host:port`.
    #[configurable(metadata(docs::examples = "127.0.0.1:9123"))]
    pub bootstrap_servers: String,

    /// Database name in Fluss.
    #[configurable(metadata(docs::examples = "fluss"))]
    pub database: String,

    /// The Fluss table name to write events to.
    ///
    /// Supports templates for dynamic routing (e.g., `{{ .table }}`).
    #[configurable(metadata(docs::templateable))]
    #[configurable(metadata(docs::examples = "events", docs::examples = "{{ .table }}"))]
    pub table: Template,

    /// Schema mode: "auto" to discover schema from Fluss, "manual" to use column mappings.
    #[serde(default = "default_schema_mode")]
    pub schema_mode: SchemaMode,

    /// Column mappings (only used when schema_mode is "manual").
    #[serde(default)]
    pub columns: Vec<ColumnMapping>,

    /// Event field removed after rendering the target table name.
    /// Useful for routing fields in dynamic table templates.
    #[configurable(metadata(docs::examples = "table"))]
    pub table_field: Option<ConfigTargetPath>,

    /// How often to refresh table metadata, in seconds, in either schema mode.
    #[serde(default = "default_schema_refresh_interval_sec")]
    #[configurable(metadata(docs::advanced))]
    pub schema_refresh_interval_sec: u64,

    /// SASL authentication username.
    #[configurable(metadata(docs::examples = "user"))]
    pub sasl_username: Option<String>,

    /// SASL authentication password.
    #[configurable(metadata(docs::examples = "password"))]
    pub sasl_password: Option<SensitiveString>,

    /// Writer batch size in bytes.
    #[serde(default = "default_batch_size")]
    #[configurable(metadata(docs::advanced))]
    pub batch_size: i32,

    /// Writer batch timeout in milliseconds.
    #[serde(default = "default_batch_timeout_ms")]
    #[configurable(metadata(docs::advanced))]
    pub batch_timeout_ms: i64,

    /// Writer buffer memory size in bytes.
    #[serde(default = "default_buffer_memory_size")]
    #[configurable(metadata(docs::advanced))]
    pub buffer_memory_size: usize,

    /// Connection timeout in milliseconds.
    #[serde(default = "default_connect_timeout_ms")]
    #[configurable(metadata(docs::advanced))]
    pub connect_timeout_ms: u64,

    /// Maximum events awaiting submission or server acknowledgement.
    /// Vector's driver may additionally prefetch up to 1024 events.
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,

    /// Maximum cached tables. Reaching this limit drains and replaces the connection,
    /// also bounding the native client's table and writer caches.
    #[serde(default = "default_max_cached_tables")]
    pub max_cached_tables: usize,

    /// Maximum time spent waiting for native writer buffer space, in milliseconds.
    #[serde(default = "default_buffer_wait_timeout_ms")]
    pub buffer_wait_timeout_ms: u64,

    /// Submission retries and delivery deadline per event, in milliseconds.
    /// An already running append may exceed this by buffer_wait_timeout_ms.
    /// Expired writes have unknown delivery status and are not submitted again.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Deadline for draining and closing after the input ends, in milliseconds.
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

const fn default_schema_mode() -> SchemaMode {
    SchemaMode::Auto
}

const fn default_schema_refresh_interval_sec() -> u64 {
    60
}

const fn default_batch_size() -> i32 {
    2 * 1024 * 1024
}

const fn default_batch_timeout_ms() -> i64 {
    100
}

const fn default_buffer_memory_size() -> usize {
    64 * 1024 * 1024
}

const fn default_connect_timeout_ms() -> u64 {
    120_000
}

const fn default_max_in_flight() -> usize {
    256
}
const fn default_max_cached_tables() -> usize {
    128
}
const fn default_buffer_wait_timeout_ms() -> u64 {
    1_000
}
const fn default_request_timeout_ms() -> u64 {
    60_000
}
const fn default_shutdown_timeout_ms() -> u64 {
    30_000
}

#[async_trait::async_trait]
impl ValidatedSink for FlussSinkConfig {
    type Validated = ConfinedTemplate;

    fn validate(&self) -> crate::Result<Self::Validated> {
        if self.bootstrap_servers.trim().is_empty()
            || self.database.trim().is_empty()
            || self.table.get_ref().trim().is_empty()
        {
            return Err("bootstrap_servers, database and table must be nonempty".into());
        }
        if let Some(error) = fluss::metadata::TablePath::detect_invalid_name(&self.database) {
            return Err(format!("invalid database name: {error}").into());
        }
        if !self.table.is_dynamic()
            && let Some(error) =
                fluss::metadata::TablePath::detect_invalid_name(self.table.get_ref())
        {
            return Err(format!("invalid table name: {error}").into());
        }
        if self.sasl_username.is_some() != self.sasl_password.is_some() {
            return Err("sasl_username and sasl_password must be supplied together".into());
        }
        if self.max_in_flight == 0
            || self.max_in_flight > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_cached_tables == 0
        {
            return Err("max_in_flight must fit a positive semaphore capacity; max_cached_tables must be positive".into());
        }
        for (name, millis) in [
            ("connect_timeout_ms", self.connect_timeout_ms),
            ("buffer_wait_timeout_ms", self.buffer_wait_timeout_ms),
            ("request_timeout_ms", self.request_timeout_ms),
            ("shutdown_timeout_ms", self.shutdown_timeout_ms),
            (
                "schema_refresh_interval_sec",
                self.schema_refresh_interval_sec.saturating_mul(1000),
            ),
        ] {
            if millis == 0 || millis > 86_400_000 {
                return Err(format!("{name} must be positive and at most one day").into());
            }
        }
        if self.buffer_wait_timeout_ms > self.request_timeout_ms {
            return Err("buffer_wait_timeout_ms must not exceed request_timeout_ms".into());
        }
        if self.batch_size <= 0
            || self.batch_timeout_ms <= 0
            || self.batch_timeout_ms as u64 >= self.request_timeout_ms
        {
            return Err("batch_size must be positive; batch_timeout_ms must be positive and less than request_timeout_ms".into());
        }
        match self.schema_mode {
            SchemaMode::Manual if self.columns.is_empty() => {
                return Err("manual schema mode requires columns".into());
            }
            SchemaMode::Auto if !self.columns.is_empty() => {
                return Err("columns requires manual schema mode".into());
            }
            _ => {}
        }
        let mut names = HashSet::new();
        for column in &self.columns {
            if column.to_column.is_empty() || !names.insert(&column.to_column) {
                return Err("column mappings must have unique, nonempty to_column names".into());
            }
        }
        let native = build_fluss_config(self);
        native.validate_security()?;
        native.validate_writer()?;
        // Dynamic routing stays within the operator-configured database namespace.
        Template::try_from(format!("{}.{}", self.database, self.table.get_ref()))?.confine(
            &Default::default(),
            "fluss",
            "table",
        )
    }

    async fn build(
        &self,
        table_template: &Self::Validated,
        cx: SinkContext,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = FlussSink::new(self.clone(), table_template.clone());
        let healthcheck = healthcheck(self.clone(), cx.healthcheck.timeout).boxed();
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }
}

impl GenerateConfig for FlussSinkConfig {
    fn generate_config() -> serde_json::Value {
        serde_json::to_value(Self {
            bootstrap_servers: "127.0.0.1:9123".to_owned(),
            database: "fluss".to_owned(),
            table: Template::try_from("events".to_owned()).unwrap(),
            schema_mode: SchemaMode::Auto,
            columns: vec![],
            table_field: None,
            schema_refresh_interval_sec: default_schema_refresh_interval_sec(),
            sasl_username: None,
            sasl_password: None,
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
            buffer_memory_size: default_buffer_memory_size(),
            connect_timeout_ms: default_connect_timeout_ms(),
            max_in_flight: default_max_in_flight(),
            max_cached_tables: default_max_cached_tables(),
            buffer_wait_timeout_ms: default_buffer_wait_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            shutdown_timeout_ms: default_shutdown_timeout_ms(),
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "fluss")]
impl SinkConfig for FlussSinkConfig {
    fn input(&self) -> Input {
        Input::new(DataType::Log | DataType::Trace)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

async fn healthcheck(config: FlussSinkConfig, timeout: Duration) -> crate::Result<()> {
    tokio::time::timeout(timeout, async {
        let connection = fluss::client::FlussConnection::new(build_fluss_config(&config)).await?;
        // A static table can be checked completely; a dynamic template has no target yet.
        let result = if !config.table.is_dynamic() {
            let name = config.table.get_ref().to_owned();
            let path = fluss::metadata::TablePath::new(config.database.clone(), name);
            match connection.get_table(&path).await {
                Ok(table) => {
                    if table.has_primary_key() {
                        Err("Fluss sink requires a log table without a primary key".into())
                    } else {
                        super::encoding::TableSchema::new(
                            table.get_table_info().get_row_type(),
                            &config,
                        )
                        .map(|_| ())
                        .map_err(Into::into)
                    }
                }
                Err(error) => Err(error.into()),
            }
        } else {
            Ok(())
        };
        connection.close(Duration::ZERO).await?;
        result
    })
    .await?
}

pub(super) fn build_fluss_config(config: &FlussSinkConfig) -> fluss::config::Config {
    let (security_protocol, security_sasl_username, security_sasl_password) =
        match (&config.sasl_username, &config.sasl_password) {
            (Some(username), Some(password)) => (
                "sasl".to_string(),
                username.clone(),
                password.inner().to_owned(),
            ),
            _ => {
                let defaults = fluss::config::Config::default();
                (
                    defaults.security_protocol,
                    defaults.security_sasl_username,
                    defaults.security_sasl_password,
                )
            }
        };

    fluss::config::Config {
        bootstrap_servers: config.bootstrap_servers.clone(),
        connect_timeout_ms: config.connect_timeout_ms,
        writer_batch_size: config.batch_size,
        writer_batch_timeout_ms: config.batch_timeout_ms,
        writer_buffer_memory_size: config.buffer_memory_size,
        writer_buffer_wait_timeout_ms: config.buffer_wait_timeout_ms,
        security_protocol,
        security_sasl_username,
        security_sasl_password,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn config() -> FlussSinkConfig {
        serde_json::from_value(FlussSinkConfig::generate_config()).unwrap()
    }

    #[test]
    fn generate_config() {
        config().validate().unwrap();
    }

    #[test]
    fn rejects_invalid_configuration_and_redacts_password() {
        let mut invalid = config();
        invalid.sasl_password = Some("sensitive-password".to_owned().into());
        assert!(!format!("{invalid:?}").contains("sensitive-password"));
        assert!(invalid.validate().is_err());
        invalid.sasl_username = Some("user".into());
        assert!(invalid.validate().is_ok());
        for value in [0, u64::MAX] {
            let mut invalid = config();
            invalid.buffer_wait_timeout_ms = value;
            assert!(invalid.validate().is_err());
        }
        let mut invalid = config();
        invalid.max_in_flight = 0;
        assert!(invalid.validate().is_err());
        invalid = config();
        invalid.buffer_memory_size = 1;
        assert!(invalid.validate().is_err());
        invalid = config();
        invalid.schema_mode = SchemaMode::Manual;
        assert!(invalid.validate().is_err());
    }

    #[tokio::test]
    async fn healthcheck_reports_unreachable_server() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut config = config();
        config.bootstrap_servers = listener.local_addr().unwrap().to_string();
        drop(listener);
        assert!(
            healthcheck(config, Duration::from_millis(100))
                .await
                .is_err()
        );
    }
}
