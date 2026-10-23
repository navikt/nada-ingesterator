use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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

/// A connection to a source database where CDC should be set up.
pub struct SourceDatabase<'a> {
    pool: Pool<Postgres>,
    config: &'a ReplicationConfig,
}

impl<'a> SourceDatabase<'a> {
    pub async fn new(
        options: PgConnectOptions,
        config: &'a ReplicationConfig,
    ) -> Result<Self, sqlx::Error> {
        info!(
            "Connecting to PostgreSQL on {} port {} as '{}'...",
            options.get_host(),
            options.get_port(),
            options.get_username(),
        );

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_with(options)
            .await?;

        Ok(Self { pool, config })
    }

    pub async fn bootstrap_database_for_replication(&mut self) -> Result<(), sqlx::Error> {
        self.setup_logical_replication().await?;
        info!("Logical replication configured");

        if self.cdc_user_exists().await? {
            warn!("Drop existing CDC replication role and privileges");
            self.drop_replication_role().await?;
        }

        self.create_replication_role().await?;
        info!("Replication role configured");

        self.create_heartbeat_table().await?;
        info!("Heartbeat table configured");

        Ok(())
    }

    /// Returns true if the CDC user exists in the database.
    async fn cdc_user_exists(&self) -> Result<bool, sqlx::Error> {
        Ok(query!(
            "SELECT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = $1);",
            &self.config.username
        )
        .fetch_one(&self.pool)
        .await?
        .exists
        .unwrap_or_default())
    }

    async fn setup_logical_replication(&self) -> Result<(), sqlx::Error> {
        // Enable logical replication (requires restart)
        // wal_level must be 'logical' for CDC to work
        sqlx::query!("ALTER SYSTEM SET wal_level = 'logical';")
            .execute(&self.pool)
            .await?;

        // Number of replication slots to support
        // Each Debezium connector requires one slot
        sqlx::query!("ALTER SYSTEM SET max_replication_slots = 4;")
            .execute(&self.pool)
            .await?;

        // Number of concurrent WAL sender processes
        // Should be at least equal to max_replication_slots
        sqlx::query!("ALTER SYSTEM SET max_wal_senders = 4;")
            .execute(&self.pool)
            .await?;

        // Cap WAL retained by inactive or lagging replication slots
        // Choose a value high enough for your recovery window and alert before it is reached
        sqlx::query!("ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Uninstall the CDC replication user by dropping all their privileges and deleting the role.
    async fn drop_replication_role(&mut self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let sql = AssertSqlSafe(format!(
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA debezium FROM {};",
            &self.config.username
        ));
        sqlx::query(sql).execute(&mut *tx).await?;

        let sql = AssertSqlSafe(format!(
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} FROM {};",
            &self.config.schema, &self.config.username
        ));
        sqlx::query(sql).execute(&mut *tx).await?;

        // Drop any previous privileges on the database.
        let sql = AssertSqlSafe(format!(
            "REVOKE ALL PRIVILEGES ON DATABASE {} FROM {};",
            &self.config.database, &self.config.username
        ));
        sqlx::query(sql).execute(&mut *tx).await?;

        // Drop the heartbeat table
        sqlx::query("DROP TABLE IF EXISTS debezium.heartbeat;")
            .execute(&mut *tx)
            .await?;

        // Drop the debezium schema
        sqlx::query("DROP SCHEMA debezium;")
            .execute(&mut *tx)
            .await?;

        // Finally, drop the CDC replication role if it exists.
        let sql = AssertSqlSafe(format!("DROP ROLE {};", &self.config.username));
        sqlx::query(sql).execute(&mut *tx).await?;

        tx.commit().await
    }

    /// Create a replication user and grant privileges.
    ///
    /// Note that parametrized values are not supported for these kinds of queries,
    /// so we do manual SQL string injection here.
    async fn create_replication_role(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // Use dedicated user with minimal privileges.
        let sql = AssertSqlSafe(format!(
            "CREATE ROLE {} WITH REPLICATION LOGIN PASSWORD '{}';",
            self.config.username, self.config.password
        ));
        sqlx::query(sql).execute(&mut *tx).await?;

        // Create replication user with minimal privileges
        let sql = AssertSqlSafe(format!(
            "GRANT CONNECT ON DATABASE {} TO {};",
            self.config.database, self.config.username,
        ));
        sqlx::query(sql).execute(&mut *tx).await?;

        // Grant replication permissions on tables
        for table in &self.config.tables {
            let sql = AssertSqlSafe(format!(
                "GRANT SELECT ON {} TO {};",
                table, self.config.username
            ));
            sqlx::query(sql).execute(&mut *tx).await?;
        }

        tx.commit().await
    }

    async fn create_heartbeat_table(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!("CREATE SCHEMA debezium;")
            .execute(&mut *tx)
            .await?;

        sqlx::query!("CREATE TABLE debezium.heartbeat (timestamp TIMESTAMPTZ);")
            .execute(&mut *tx)
            .await?;

        sqlx::query("INSERT INTO debezium.heartbeat VALUES (NOW());")
            .execute(&mut *tx)
            .await?;

        let sql = AssertSqlSafe(format!(
            "ALTER TABLE debezium.heartbeat OWNER TO {}",
            self.config.username
        ));
        sqlx::query(sql).execute(&mut *tx).await?;

        // let sql = AssertSqlSafe(format!(
        //     "GRANT ALL ON debezium.heartbeat TO {}",
        //     self.config.username
        // ));
        // sqlx::query(sql).execute(&mut *tx).await?;

        tx.commit().await
    }
}
