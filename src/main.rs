use crate::database::ReplicationConfig;
use tracing::log::error;

mod database;
mod debezium;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = ReplicationConfig {
        username: "cdc_replication".to_string(),
        password: "strong_password".to_string(),
        database: "ingesterator".to_string(),
        tables: vec![],
    };

    match database::bootstrap_database_for_replication(&config).await {
        Ok(_) => {}
        Err(err) => {
            error!("bootstrap database: {err}")
        }
    }
}
