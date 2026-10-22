use crate::database::ReplicationConfig;

mod database;
mod debezium;

#[tokio::main]
async fn main() {
    let config = ReplicationConfig {
        username: "cdc_replication".to_string(),
        password: "strong_password".to_string(),
        database: "ingesterator".to_string(),
        tables: vec![],
    };

    database::bootstrap_database_for_replication(&config)
        .await
        .expect("Database bootstrap")
}
