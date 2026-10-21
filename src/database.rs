use sqlx::{Pool, Postgres};

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
