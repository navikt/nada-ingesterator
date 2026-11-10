use crate::crd::debezium::{
    DebeziumServer, DebeziumServerSink, DebeziumServerSource, DebeziumServerSpec,
};
use crate::crd::ingesterator::Ingest;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::runtime::reflector::Lookup;
use sqlx::postgres::PgConnectOptions;
use std::collections::BTreeMap;

pub struct DebeziumOpts<'a> {
    pub ingest: &'a Ingest,
    pub postgres_options: PgConnectOptions,
    pub replication_config: ReplicationConfig,
}

pub struct ReplicationConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    pub schema: String,
    pub tables: Vec<String>,
}

pub fn new(opts: DebeziumOpts) -> DebeziumServer {
    let metadata = ObjectMeta {
        name: opts.ingest.name().map(|x| x.to_string()),
        namespace: opts.ingest.namespace().map(|x| x.to_string()),
        ..Default::default()
    };

    let source_config = make_source_config(&opts.postgres_options, &opts.replication_config);

    let source = DebeziumServerSource {
        class: Some("io.debezium.connector.postgresql.PostgresConnector".into()),
        config: Some(source_config),
        offset: None,
        schema_history: None,
    };

    let sink_config = make_sink_config(&opts.postgres_options, &opts.replication_config);

    let sink = DebeziumServerSink {
        config: Some(sink_config),
        r#type: Some("kafka".into()),
    };

    let spec = DebeziumServerSpec {
        sink: Some(sink),
        source: Some(source),
        format: None,
        image: None,
        predicates: None,
        quarkus: None,
        runtime: None,
        transforms: None,
        version: None,
    };

    DebeziumServer {
        metadata,
        spec,
        status: None,
    }
}
/// Upstream doc: https://debezium.io/documentation/reference/3.7/operations/debezium-operator.html#debezium-operator-schema-reference-sink-config
fn make_sink_config(
    postgres_options: &PgConnectOptions,
    replication_config: &ReplicationConfig,
) -> BTreeMap<String, serde_json::Value> {
    let mut config = BTreeMap::new();

    macro_rules! prop {
        ($key:expr, $value:expr $(,)?) => {{
            config.insert($key.into(), $value.into());
        }};
    }

    prop!("bootstrap.servers", "kafka:9092");

    config
}

fn make_source_config(
    postgres_options: &PgConnectOptions,
    replication_config: &ReplicationConfig,
) -> BTreeMap<String, serde_json::Value> {
    let mut config = BTreeMap::new();

    macro_rules! prop {
        ($key:expr, $value:expr $(,)?) => {{
            config.insert($key.into(), $value.into());
        }};
    }

    prop!("plugin.name", "pgoutput");

    // The default `all_tables` needs superuser
    prop!("publication.autocreate.mode", "disabled");

    // Sane default?
    prop!("heartbeat.interval.ms", 10_000);

    // Database options. Find a way to use a secret instead of putting it here where everyone can see it.
    // database.ssl*: ?
    prop!("database.dbname", replication_config.database.as_str());
    prop!("database.username", replication_config.username.as_str());
    prop!("database.host", postgres_options.get_host());
    prop!("database.port", postgres_options.get_port());

    // Whitelist tables that should be replicated.
    // Should match what we set up in replication bootstrap.
    prop!("table.include.list", replication_config.tables.as_slice());

    // Insert data into heartbeat table
    // lsn.flush.mode: ? # er preview men vi kunne muligens slippe heartbeat
    prop!(
        "heartbeat.action.query",
        "UPDATE debezium.heartbeat SET timestamp = NOW();"
    );

    // slot.name: TBD # matcher det vi setter opp ved bootstrap
    // slot.failover: ? # noe greier her med pg <= 16
    // publication.name: TBD # må matche bootstrap
    // topic.prefix: ? # kansje, men må aldri endre en man har satt
    // [schema|table|column].[include|exclude].list: []
    // message.key.columns: ? # muligens ved composite keys
    // read.only: # må muligens se på den avhengig av rettigheter vi får

    config
}
