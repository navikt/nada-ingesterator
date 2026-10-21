use std::time::Duration;
use sqlx::postgres::PgPoolOptions;

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

    println!("Connection success, bye");
}
