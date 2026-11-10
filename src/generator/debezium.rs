use crate::crd::debezium::{
    DebeziumServer, DebeziumServerSink, DebeziumServerSource, DebeziumServerSpec,
};
use crate::crd::ingesterator::Ingest;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource;
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

pub struct GeneratedResources {
    pub debezium_server: DebeziumServer,
    pub secret: Secret,
}

impl<'a> DebeziumOpts<'a> {
    pub fn generate(self) -> GeneratedResources {
        let metadata = ObjectMeta {
            name: self.ingest.name().map(|x| x.to_string()),
            namespace: self.ingest.namespace().map(|x| x.to_string()),
            ..Default::default()
        };

        let source_config = self.make_source_config();

        let source = DebeziumServerSource {
            class: Some("io.debezium.connector.postgresql.PostgresConnector".into()),
            config: Some(source_config),
            offset: None,
            schema_history: None,
        };

        let sink_config = self.make_sink_config();

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

        let debezium_server = DebeziumServer {
            metadata,
            spec,
            status: None,
        };

        let secret = Secret::default();

        GeneratedResources {
            debezium_server,
            secret,
        }
    }

    /// Create a secret that can be mounted into a Debezium server pod.
    ///
    /// The secret contains a single value which encodes credentials to
    /// the Postgres server as a Java properties file.
    ///
    /// `credentials.properties`:
    ///
    ///     username = foo
    ///     password = bar
    fn make_secret(&self) -> Secret {
        let mut string_data = BTreeMap::new();
        let mut propfile = String::new();

        propfile.push_str("username = ".into());
        propfile.push_str(&self.replication_config.username);
        propfile.push_str("\n");

        propfile.push_str("password = ".into());
        propfile.push_str(&self.replication_config.password);
        propfile.push_str("\n");

        string_data.insert("credentials.properties".to_string(), propfile);

        Secret {
            metadata: ObjectMeta {
                name: self.ingest.metadata.name.as_ref().cloned(),
                namespace: self.ingest.metadata.namespace.as_ref().cloned(),
                ..Default::default()
            },
            string_data: Some(string_data),
            ..Default::default()
        }
    }

    /// Upstream doc: https://debezium.io/documentation/reference/3.7/operations/debezium-operator.html#debezium-operator-schema-reference-sink-config
    fn make_sink_config(&self) -> BTreeMap<String, serde_json::Value> {
        let mut config = BTreeMap::new();

        macro_rules! prop {
            ($key:expr, $value:expr $(,)?) => {{
                config.insert($key.into(), $value.into());
            }};
        }

        prop!("bootstrap.servers", "kafka:9092");

        config
    }

    fn make_source_config(&self) -> BTreeMap<String, serde_json::Value> {
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
        prop!("database.dbname", self.replication_config.database.as_str());
        prop!(
            "database.username",
            self.replication_config.username.as_str()
        );
        prop!("database.host", self.postgres_options.get_host());
        prop!("database.port", self.postgres_options.get_port());

        // Whitelist tables that should be replicated.
        // Should match what we set up in replication bootstrap.
        prop!(
            "table.include.list",
            self.replication_config.tables.as_slice()
        );

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
}
