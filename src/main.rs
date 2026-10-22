use crate::database::{ReplicationConfig, SourceDatabase};
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

    let config = ReplicationConfig {
        username: "cdc_replication".to_string(),
        password: "strong_password".to_string(),
        database: "ingesterator".to_string(),
        schema: "public".to_string(),
        tables: vec![],
    };

    let mut db = match SourceDatabase::new(&config).await {
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
