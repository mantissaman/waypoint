//! Schema history table operations (create, query, insert, update, delete).

use chrono::{DateTime, Utc};
use tokio_postgres::Client;

use crate::db::quote_ident;
use crate::error::Result;

/// A row from the schema history table.
#[derive(Debug, Clone)]
pub struct AppliedMigration {
    /// Monotonically increasing rank indicating the order of installation.
    pub installed_rank: i32,
    /// Migration version string, or `None` for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description of the migration.
    pub description: String,
    /// Type of migration (e.g., `"SQL"`, `"SQL_REPEATABLE"`, `"UNDO_SQL"`, `"BASELINE"`).
    pub migration_type: String,
    /// Filename of the migration script.
    pub script: String,
    /// CRC32 checksum of the migration SQL, or `None` for baselines.
    pub checksum: Option<i32>,
    /// Database user or custom identifier that applied the migration.
    pub installed_by: String,
    /// Timestamp when the migration was applied.
    pub installed_on: DateTime<Utc>,
    /// Time in milliseconds the migration took to execute.
    pub execution_time: i32,
    /// Whether the migration completed successfully.
    pub success: bool,
    /// Auto-generated reverse SQL, if available.
    pub reversal_sql: Option<String>,
}

/// Create the schema history table if it does not exist.
pub async fn create_history_table(client: &Client, schema: &str, table: &str) -> Result<()> {
    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let idx_name = format!("{}_s_idx", table);
    let ver_idx_name = format!("{}_v_idx", table);
    let sql = format!(
        r#"
CREATE TABLE IF NOT EXISTS {fq} (
    installed_rank INTEGER PRIMARY KEY,
    version        VARCHAR(50),
    description    VARCHAR(200) NOT NULL,
    type           VARCHAR(20) NOT NULL,
    script         VARCHAR(1000) NOT NULL,
    checksum       INTEGER,
    installed_by   VARCHAR(100) NOT NULL,
    installed_on   TIMESTAMPTZ NOT NULL DEFAULT now(),
    execution_time INTEGER NOT NULL,
    success        BOOLEAN NOT NULL,
    reversal_sql   TEXT
);

CREATE INDEX IF NOT EXISTS {idx_name} ON {fq} (success);
CREATE INDEX IF NOT EXISTS {ver_idx_name} ON {fq} (version);
"#,
        fq = fq,
        idx_name = quote_ident(&idx_name),
        ver_idx_name = quote_ident(&ver_idx_name),
    );

    client.batch_execute(&sql).await?;

    // Auto-upgrade: add reversal_sql column if table already existed without it
    upgrade_history_table(client, schema, table).await?;

    Ok(())
}

/// Auto-upgrade the history table to add new columns if they don't exist.
async fn upgrade_history_table(client: &Client, schema: &str, table: &str) -> Result<()> {
    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    // Add reversal_sql column if it doesn't exist
    let sql = format!(
        "ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS reversal_sql TEXT",
        fq = fq,
    );
    // Ignore errors (e.g., if the column already exists on older PG without IF NOT EXISTS)
    if let Err(e) = client.batch_execute(&sql).await {
        log::debug!("History table upgrade (reversal_sql): {}", e);
    }
    Ok(())
}

/// Check if the history table exists.
pub async fn history_table_exists(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &table],
        )
        .await?;

    Ok(row.get::<_, bool>(0))
}

/// Get the next installed_rank value.
pub async fn next_installed_rank(client: &Client, schema: &str, table: &str) -> Result<i32> {
    let sql = format!(
        "SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {}.{}",
        quote_ident(schema),
        quote_ident(table)
    );
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get::<_, i32>(0))
}

/// Query all applied migrations from the history table.
pub async fn get_applied_migrations(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<AppliedMigration>> {
    let sql = format!(
        "SELECT installed_rank, version, description, type, script, checksum, \
         installed_by, installed_on, execution_time, success, reversal_sql \
         FROM {}.{} ORDER BY installed_rank",
        quote_ident(schema),
        quote_ident(table)
    );

    let rows = client.query(&sql, &[]).await?;

    let mut migrations = Vec::with_capacity(rows.len());
    for row in rows {
        migrations.push(AppliedMigration {
            installed_rank: row.get(0),
            version: row.get(1),
            description: row.get(2),
            migration_type: row.get(3),
            script: row.get(4),
            checksum: row.get(5),
            installed_by: row.get(6),
            installed_on: row.get(7),
            execution_time: row.get(8),
            success: row.get(9),
            reversal_sql: row.get(10),
        });
    }

    Ok(migrations)
}

/// Insert a migration record into the history table with atomic rank assignment.
///
/// Uses a subquery to atomically compute the next installed_rank within the INSERT,
/// eliminating the race between reading the max rank and inserting.
#[allow(clippy::too_many_arguments)]
pub async fn insert_applied_migration(
    client: &Client,
    schema: &str,
    table: &str,
    version: Option<&str>,
    description: &str,
    migration_type: &str,
    script: &str,
    checksum: Option<i32>,
    installed_by: &str,
    execution_time: i32,
    success: bool,
) -> Result<()> {
    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let sql = format!(
        "INSERT INTO {fq} \
         (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) \
         VALUES (\
            (SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {fq}), \
            $1, $2, $3, $4, $5, $6, $7, $8\
         )",
        fq = fq,
    );

    client
        .execute(
            &sql,
            &[
                &version,
                &description,
                &migration_type,
                &script,
                &checksum,
                &installed_by,
                &execution_time,
                &success,
            ],
        )
        .await?;

    Ok(())
}

/// Delete all failed migration records (success = FALSE).
pub async fn delete_failed_migrations(client: &Client, schema: &str, table: &str) -> Result<u64> {
    let sql = format!(
        "DELETE FROM {}.{} WHERE success = FALSE",
        quote_ident(schema),
        quote_ident(table)
    );
    let count = client.execute(&sql, &[]).await?;
    Ok(count)
}

/// Update the checksum for a specific migration by version.
pub async fn update_checksum(
    client: &Client,
    schema: &str,
    table: &str,
    version: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {}.{} SET checksum = $1 WHERE version = $2",
        quote_ident(schema),
        quote_ident(table)
    );
    client.execute(&sql, &[&new_checksum, &version]).await?;
    Ok(())
}

/// Update the checksum for a repeatable migration by script name (version is NULL).
pub async fn update_repeatable_checksum(
    client: &Client,
    schema: &str,
    table: &str,
    script: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {}.{} SET checksum = $1 WHERE script = $2 AND version IS NULL",
        quote_ident(schema),
        quote_ident(table)
    );
    client.execute(&sql, &[&new_checksum, &script]).await?;
    Ok(())
}

/// Compute the set of versions that are currently effectively applied.
///
/// Processes history rows in `installed_rank` order (assumed already sorted).
/// For each version, tracks whether the latest successful action was a forward
/// migration (`"SQL"` / `"BASELINE"`) or an undo (`"UNDO_SQL"`).
/// Returns the set of version strings that are currently applied.
pub fn effective_applied_versions(
    applied: &[AppliedMigration],
) -> std::collections::HashSet<String> {
    let mut effective = std::collections::HashSet::new();
    for am in applied {
        if !am.success {
            continue;
        }
        if let Some(ref version) = am.version {
            if am.migration_type == "UNDO_SQL" {
                effective.remove(version);
            } else {
                effective.insert(version.clone());
            }
        }
    }
    effective
}

/// Check if the history table has any entries.
pub async fn has_entries(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM {}.{})",
        quote_ident(schema),
        quote_ident(table)
    );
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get::<_, bool>(0))
}
