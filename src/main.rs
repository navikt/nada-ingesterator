use crate::database::{ReplicationConfig, SourceDatabase};
use sqlx::postgres::PgConnectOptions;
use tracing::log::error;

mod database;
mod debezium;
mod kubernetes;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let kube_resource = kubernetes::make_debezium_kubernetes_resource();

    match kubernetes::save_to_kubernetes(&kube_resource).await {
        Ok(_) => {}
        Err(err) => {
            error!("kubernetes: {err}")
        }
    }

    let replication_config = ReplicationConfig {
        username: "cdc_replication".to_string(),
        password: "strong_password".to_string(),
        database: "ingesterator".to_string(),
        schema: "public".to_string(),
        tables: vec![],
    };

    let postgres_connect_options = PgConnectOptions::new_without_pgpass()
        .username("ingesterator")
        .password("ingesterator")
        .host("127.0.0.1")
        .port(5432);

    let mut db = match SourceDatabase::new(postgres_connect_options, &replication_config).await {
        Ok(db) => db,
        Err(err) => {
            error!("connect database: {err}");
            return;
        }
    };

    let _ = db
        .bootstrap_database_for_replication()
        .await
        .inspect_err(|err| error!("bootstrap database: {}", err));
}
