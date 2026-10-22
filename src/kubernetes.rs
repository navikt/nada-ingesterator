use crate::debezium::{DebeziumServer, DebeziumServerSource, DebeziumServerSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::PostParams;
use kube::{Api, Client, Error};
use std::collections::BTreeMap;
use tracing::log::info;

pub async fn save_to_kubernetes(resource: &DebeziumServer) -> Result<(), Error> {
    info!("Connecting to Kubernetes...");
    let client = Client::try_default().await?;

    let debz: Api<DebeziumServer> = Api::namespaced(client, "kimtore");

    info!("Creating DebeziumServer...");
    debz.create(&PostParams::default(), &resource).await?;

    Ok(())
}

pub fn make_debezium_kubernetes_resource() -> DebeziumServer {
    let metadata = ObjectMeta {
        name: Some("kimtore".into()),
        namespace: Some("kimtore".into()),
        ..Default::default()
    };

    // plugin.name: pgoutput
    // slot.name: TBD # matcher det vi setter opp ved bootstrap
    // slot.failover: ? # noe greier her med pg <= 16
    // publication.name: TBD # må matche bootstrap
    // database.*: <db params>
    // topic.prefix: ? # kansje, men må aldri endre en man har satt
    // [schema|table|column].[include|exclude].list: []
    // database.ssl*: ?
    // message.key.columns: ? # muligens ved composite keys
    // publication.autocreate.mode: disabled # all_tables som er default trenger superuser
    // heartbeat.interval.ms: ? # må nok justere oss inn etter hvert, starte med hvert 10 sek?
    // heartbeat.action.query: "INSERT INTO dbz_heartbeat_table (text) VALUES ('dbz_heartbeat')"
    // lsn.flush.mode: ? # er preview men vi kunne muligens slippe heartbeat
    // read.only: # må muligens se på den avhengig av rettigheter vi får
    let config = BTreeMap::new();

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
