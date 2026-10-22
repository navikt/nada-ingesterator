use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, Pool, Postgres, query};
use std::time::Duration;
use tracing::log::{info, warn};

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    pub schema: String,
    pub tables: Vec<String>,
}

pub async fn bootstrap_database_for_replication(
    config: &ReplicationConfig,
) -> Result<(), sqlx::Error> {
    let connection_string = "postgres://ingesterator:ingesterator@127.0.0.1:5432";

    info!("Connecting to {}...", connection_string);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect(connection_string)
        .await?;

    info!("Connection success");

    setup_logical_replication(&pool).await?;
    info!("Database configured for logical replication");

    if cdc_user_exists(&pool, &config).await? {
        warn!("Dropping existing CDC replication role and privileges");
        drop_replication_role(&pool, &config).await?;
    }

    create_replication_role(&pool, &config).await?;
    info!("Replication user configured");

    Ok(())
}

/// Returns true if the CDC user exists in the database.
async fn cdc_user_exists(
    pool: &Pool<Postgres>,
    config: &ReplicationConfig,
) -> Result<bool, sqlx::Error> {
    Ok(query!(
        "SELECT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = $1);",
        &config.username
    )
    .fetch_one(pool)
    .await?
    .exists
    .unwrap_or_default())
}

async fn setup_logical_replication(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
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

/// Uninstall the CDC replication user by dropping all their privileges and deleting the role.
async fn drop_replication_role(
    pool: &Pool<Postgres>,
    config: &ReplicationConfig,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let sql = AssertSqlSafe(format!(
        "REVOKE ALL PRIVILEGES ON SCHEMA {} FROM {};",
        config.schema, config.username
    ));
    sqlx::query(sql).execute(&mut *tx).await?;

    // Drop any previous privileges on the database.
    let sql = AssertSqlSafe(format!(
        "REVOKE ALL PRIVILEGES ON DATABASE {} FROM {};",
        config.database, config.username
    ));
    sqlx::query(sql).execute(&mut *tx).await?;

    // Drop the CDC replication role if it exists.
    let sql = AssertSqlSafe(format!("DROP ROLE {};", config.username));
    sqlx::query(sql).execute(&mut *tx).await?;

    tx.commit().await
}

/// Create a replication user and grant privileges.
///
/// Note that parametrized values are not supported for these kinds of queries,
/// so we do manual SQL string injection here.
async fn create_replication_role(
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
