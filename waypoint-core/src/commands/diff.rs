//! Compare live database schema against a target and generate migration SQL.

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::Result;
use crate::schema::{self, SchemaDiff};

/// Target to compare the current schema against.
pub enum DiffTarget {
    /// Compare against another database identified by its connection URL.
    Database(String),
}

/// Report produced by the diff command.
#[derive(Debug, Serialize)]
pub struct DiffReport {
    /// List of individual schema differences found.
    pub diffs: Vec<SchemaDiff>,
    /// DDL SQL statements generated to reconcile the differences.
    pub generated_sql: String,
    /// Whether any differences were detected.
    pub has_changes: bool,
}

/// Execute the diff command.
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target: DiffTarget,
) -> Result<DiffReport> {
    let schema_name = &config.migrations.schema;

    // Introspect the current database schema
    let current = schema::introspect(client, schema_name).await?;

    let target_snapshot = match target {
        DiffTarget::Database(ref url) => {
            let target_client = crate::db::connect(url).await?;
            schema::introspect(&target_client, schema_name).await?
        }
    };

    let diffs = schema::diff(&current, &target_snapshot);
    let generated_sql = schema::generate_ddl(&diffs);
    let has_changes = !diffs.is_empty();

    Ok(DiffReport {
        diffs,
        generated_sql,
        has_changes,
    })
}
