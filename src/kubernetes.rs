use crate::database::ReplicationConfig;
use crate::debezium::{DebeziumServer, DebeziumServerSource, DebeziumServerSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams};
use kube::runtime::reflector::Lookup;
use kube::{Api, Client, Error, ResourceExt};
use sqlx::postgres::PgConnectOptions;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::log::{info, warn};

pub async fn save_to_kubernetes(resource: &DebeziumServer) -> Result<(), Error> {
    info!("Connecting to Kubernetes...");
    let client = Client::try_default().await?;

    let debz: Api<DebeziumServer> = Api::namespaced(client, "kimtore");

    if let Some(name) = resource.name() {
        // copy some existing props over to the new object
        // https://cloudnative-pg.io/docs/1.30/replication/#streaming-replication-support
        if let Some(existing_resource) = debz.get_opt(&name).await? {
            warn!(
                "Deleting existing DebeziumServer '{}'...",
                existing_resource.name_any()
            );
            debz.delete(&name, &DeleteParams::foreground()).await?;
            sleep(Duration::from_millis(50)).await; // hack to prevent race condition
        }
    }

    info!("Creating DebeziumServer...");
    debz.create(&PostParams::default(), &resource).await?;

    Ok(())
}

pub fn make_debezium_kubernetes_resource(
    postgres_options: PgConnectOptions,
    replication_config: &ReplicationConfig,
) -> DebeziumServer {
    let metadata = ObjectMeta {
        name: Some("kimtore".into()),
        namespace: Some("kimtore".into()),
        ..Default::default()
    };

    let config = make_config(&postgres_options, replication_config);

    let source = DebeziumServerSource {
        class: Some("io.debezium.connector.postgresql.PostgresConnector".into()),
        config: Some(config),
        offset: None,
        schema_history: None,
    };

    let spec = DebeziumServerSpec {
        format: None,
        image: None,
        predicates: None,
        quarkus: None,
        runtime: None,
        sink: None,
        source: Some(source),
        transforms: None,
        version: None,
    };

    DebeziumServer {
        metadata,
        spec,
        status: None,
    }
}

fn make_config(
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
