use crate::database::ReplicationConfig;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

mod database;

#[tokio::main]
async fn main() {
    let connection_string = "postgres://ingesterator:ingesterator@127.0.0.1:5432";

    println!("Connecting to {}...", connection_string);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect(connection_string)
        .await
        .expect("database is offline");

    println!("Connection success");

    let config = ReplicationConfig {
        username: "cdc_replication".to_string(),
        password: "strong_password".to_string(),
        database: "ingesterator".to_string(),
        tables: vec![],
    };

    database::setup_logical_replication(&pool).await.unwrap();
    database::create_replication_role(&pool, &config)
        .await
        .unwrap();

    println!("Database setup for logical replication");
}
