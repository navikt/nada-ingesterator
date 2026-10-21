use sqlx::{AssertSqlSafe, Pool, Postgres};

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    pub tables: Vec<String>,
}

pub async fn setup_logical_replication(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    // Enable logical replication (requires restart)
    // wal_level must be 'logical' for CDC to work
    sqlx::query!("ALTER SYSTEM SET wal_level = 'logical';")
        .execute(pool)
        .await?;

    // Number of replication slots to support
    // Each Debezium connector requires one slot
    sqlx::query!("ALTER SYSTEM SET max_replication_slots = 4;")
        .execute(pool)
        .await?;

    // Number of concurrent WAL sender processes
    // Should be at least equal to max_replication_slots
    sqlx::query!("ALTER SYSTEM SET max_wal_senders = 4;")
        .execute(pool)
        .await?;

    // Cap WAL retained by inactive or lagging replication slots
    // Choose a value high enough for your recovery window and alert before it is reached
    sqlx::query!("ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';")
        .execute(pool)
        .await?;

    Ok(())
}

/// Create a replication user and grant privileges.
///
/// Note that parametrized values are not supported for these kinds of queries,
/// so we do manual SQL string injection here.
pub async fn create_replication_role(
    pool: &Pool<Postgres>,
    config: &ReplicationConfig,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Use dedicated user with minimal privileges.
    let sql = AssertSqlSafe(format!(
        "CREATE ROLE {} WITH REPLICATION LOGIN PASSWORD '{}';",
        config.username, config.password
    ));
    sqlx::query(sql).execute(&mut *tx).await?;

    // Create replication user with minimal privileges
    let sql = AssertSqlSafe(format!(
        "GRANT CONNECT ON DATABASE {} TO {};",
        config.database, config.username,
    ));
    sqlx::query(sql).execute(&mut *tx).await?;

    // Grant replication permissions on tables
    for table in &config.tables {
        let sql = AssertSqlSafe(format!("GRANT SELECT ON {} TO {};", table, config.username));
        sqlx::query(sql).execute(&mut *tx).await?;
    }

    tx.commit().await
}
