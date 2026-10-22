use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use serde_with::formats::CommaSeparator;
use serde_with::{StringWithSeparator, serde_as};

/// Debezium configuration properties.
///
///     plugin.name: pgoutput
///     slot.name: TBD # matcher det vi setter opp ved bootstrap
///     slot.failover: ? # noe greier her med pg <= 16
///     publication.name: TBD # må matche bootstrap
///     database.*: <db params>
///     topic.prefix: ? # kansje, men må aldri endre en man har satt
///     [schema|table|column].[include|exclude].list: []
///     database.ssl*: ?
///     message.key.columns: ? # muligens ved composite keys
///     publication.autocreate.mode: disabled # all_tables som er default trenger superuser
///     heartbeat.interval.ms: ? # må nok justere oss inn etter hvert, starte med hvert 10 sek?
///     heartbeat.action.query: "INSERT INTO dbz_heartbeat_table (text) VALUES ('dbz_heartbeat')"
///     lsn.flush.mode: ? # er preview men vi kunne muligens slippe heartbeat
///     read.only: # må muligens se på den avhengig av rettigheter vi får
#[serde_as]
#[derive(Debug, Default, Serialize)]
pub struct DebeziumConfig {
    #[serde(rename = "plugin.name")]
    pub plugin_name: String,
    #[serde(rename = "slot.name")]
    pub slot_name: String,
    #[serde(rename = "slot.failover")]
    pub slot_failover: String,
    #[serde(rename = "publication.name")]
    pub publication_name: String,
    #[serde(rename = "database.hostname")]
    pub database_hostname: String,
    #[serde(rename = "database.port")]
    pub database_port: String,
    #[serde(rename = "database.user")]
    pub database_user: String,
    #[serde(rename = "database.password")]
    pub database_password: String,
    #[serde(rename = "database.dbname")]
    pub database_dbname: String,
    #[serde(rename = "database.sslmode")]
    pub database_sslmode: String,
    #[serde(rename = "database.sslcert")]
    pub database_sslcert: String,
    #[serde(rename = "database.sslkey")]
    pub database_sslkey: String,
    #[serde(rename = "database.sslpassword")]
    pub database_sslpassword: String,
    #[serde(rename = "database.sslrootcert")]
    pub database_sslrootcert: String,
    #[serde(rename = "topic.prefix")]
    pub topic_prefix: String,
    #[serde(rename = "schema.include.list")]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub schema_include_list: Vec<String>,
    #[serde(rename = "schema.exclude.list")]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub schema_exclude_list: Vec<String>,
    #[serde(rename = "table.include.list")]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub table_include_list: Vec<String>,
    #[serde(rename = "table.exclude.list")]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub table_exclude_list: Vec<String>,
    #[serde(rename = "column.include.list")]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub column_include_list: Vec<String>,
    #[serde(rename = "column.exclude.list")]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub column_exclude_list: Vec<String>,
    #[serde(rename = "message.key.columns")]
    pub message_key_columns: String,
    #[serde(rename = "publication.autocreate.mode")]
    pub publication_autocreate_mode: String,
    #[serde(rename = "heartbeat.interval.ms")]
    pub heartbeat_interval_ms: String,
    #[serde(rename = "heartbeat.action.query")]
    pub heartbeat_action_query: String,
    #[serde(rename = "lsn.flush.mode")]
    pub lsn_flush_mode: String,
    #[serde(rename = "read.only")]
    pub read_only: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_comma_separation_naive() {
        let mut cfg = super::DebeziumConfig::default();
        cfg.schema_include_list = vec!["foo".into(), "bar".into(), "baz".into()];
        let serialized = serde_json::to_string(&cfg).unwrap();
        assert!(serialized.contains("foo,bar,baz"))
    }
}

/// Debezium server spec for Kubernetes.
///
///     version: "3.2.0"
///     source: {{postgres_conf}}
///     sink: "{{kafka conf, tror noe ca '{type: kafka, config: {bootstrap.servers: kafka:9092}'}"
///     format: {{format_conf, mener avro greier går her}}
///     runtime: {{runtime_conf, tror vi kan komme unna med default på det meste, men må antagelig confe otel}}
///     transforms: {{ transformation_conf, muligens trenger vi å håndtere tombstone events }}
///     predicates: {{predicate_conf, trenger antagelig ikke dette, mulig å filtrere events}}
///     quarkus: {{quarkus greier som sendes til vel quarkus}}
#[derive(CustomResource, Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[kube(
    group = "debezium.io",
    version = "v1alpha1",
    kind = "DebeziumServer",
    namespaced
)]
pub struct DebeziumServerSpec {
    version: String,
    source: (),
    sink: (),
    format: (),
    runtime: (),
    transforms: (),
    predicates: (),
    quarkus: (),
}
