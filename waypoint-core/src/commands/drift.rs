//! Detect manual schema changes that bypassed migrations.
//!
//! Creates a temporary schema, applies all migrations to it,
//! then compares it against the live schema to detect drift.

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db;
use crate::error::Result;
use crate::history;
use crate::migration::scan_migrations;
use crate::placeholder::build_placeholders;
use crate::schema::{self, SchemaDiff};

/// Type of drift detected.
#[derive(Debug, Clone, Serialize)]
pub enum DriftType {
    /// An object exists in the live database but not in the expected migration state.
    ExtraObject,
    /// An object is expected from migrations but missing from the live database.
    MissingObject,
    /// An object exists in both but its definition has been changed outside migrations.
    ModifiedObject,
}

impl std::fmt::Display for DriftType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriftType::ExtraObject => write!(f, "Extra (not in migrations)"),
            DriftType::MissingObject => write!(f, "Missing (in migrations but not in DB)"),
            DriftType::ModifiedObject => write!(f, "Modified (differs from migrations)"),
        }
    }
}

/// A single drift finding.
#[derive(Debug, Clone, Serialize)]
pub struct DriftEntry {
    /// Category of drift (extra, missing, or modified).
    pub drift_type: DriftType,
    /// Identifier of the affected database object (e.g. "TABLE users").
    pub object: String,
    /// Human-readable description of the drift.
    pub detail: String,
}

/// Drift detection report.
#[derive(Debug, Serialize)]
pub struct DriftReport {
    /// All drift findings detected.
    pub drifts: Vec<DriftEntry>,
    /// Whether any drift was detected.
    pub has_drift: bool,
    /// Name of the schema that was checked for drift.
    pub schema: String,
}

/// Execute the drift command.
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<DriftReport> {
    let schema_name = &config.migrations.schema;
    let table = &config.migrations.table;

    // Generate a random temp schema name
    let temp_schema = format!(
        "waypoint_drift_check_{}",
        chrono::Utc::now().format("%Y%m%d%H%M%S")
    );

    // Create temp schema
    client
        .batch_execute(&format!("CREATE SCHEMA {}", db::quote_ident(&temp_schema)))
        .await?;

    let result = run_drift_check(client, config, schema_name, table, &temp_schema).await;

    // Always clean up temp schema
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA {} CASCADE",
            db::quote_ident(&temp_schema)
        ))
        .await;

    result
}

async fn run_drift_check(
    client: &Client,
    config: &WaypointConfig,
    schema_name: &str,
    table: &str,
    temp_schema: &str,
) -> Result<DriftReport> {
    // Create history table in temp schema
    history::create_history_table(client, temp_schema, table).await?;

    // Get applied migrations (successful ones only)
    let applied = history::get_applied_migrations(client, schema_name, table).await?;
    let effective = history::effective_applied_versions(&applied);

    // Scan migration files
    let resolved = scan_migrations(&config.migrations.locations)?;

    // Get DB info for placeholders
    let db_user = db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    // Set search_path to temp schema and apply migrations
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            db::quote_ident(temp_schema)
        ))
        .await?;

    // Apply versioned migrations that were successfully applied
    for migration in resolved.iter().filter(|m| m.is_versioned()) {
        let version = migration.version().unwrap();
        if !effective.contains(&version.raw) {
            continue;
        }

        let placeholders = build_placeholders(
            &config.placeholders,
            temp_schema,
            &db_user,
            &db_name,
            &migration.script,
        );
        let sql = crate::placeholder::replace_placeholders(&migration.sql, &placeholders)?;
        client.batch_execute(&sql).await.map_err(|e| {
            crate::error::WaypointError::MigrationFailed {
                script: migration.script.clone(),
                reason: format!("Drift check: {}", e),
            }
        })?;
    }

    // Reset search_path
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            db::quote_ident(schema_name)
        ))
        .await?;

    // Introspect both schemas
    let live_snapshot = schema::introspect(client, schema_name).await?;
    let expected_snapshot = schema::introspect(client, temp_schema).await?;

    // Diff: expected (from migrations) vs live (actual DB state)
    let diffs = schema::diff(&expected_snapshot, &live_snapshot);

    let mut drifts = Vec::new();
    for d in &diffs {
        let (drift_type, object, detail) = match d {
            SchemaDiff::TableAdded(t) => (
                DriftType::ExtraObject,
                format!("TABLE {}", t.name),
                "Table exists in DB but not in migrations".to_string(),
            ),
            SchemaDiff::TableDropped(n) => (
                DriftType::MissingObject,
                format!("TABLE {}", n),
                "Table exists in migrations but not in DB".to_string(),
            ),
            SchemaDiff::ColumnAdded { table, column } => (
                DriftType::ExtraObject,
                format!("COLUMN {}.{}", table, column.name),
                format!("Column added outside migrations ({})", column.data_type),
            ),
            SchemaDiff::ColumnDropped { table, column } => (
                DriftType::MissingObject,
                format!("COLUMN {}.{}", table, column),
                "Column removed outside migrations".to_string(),
            ),
            SchemaDiff::ColumnAltered { table, column, .. } => (
                DriftType::ModifiedObject,
                format!("COLUMN {}.{}", table, column),
                "Column definition changed outside migrations".to_string(),
            ),
            SchemaDiff::IndexAdded(idx) => (
                DriftType::ExtraObject,
                format!("INDEX {}", idx.name),
                "Index exists in DB but not in migrations".to_string(),
            ),
            SchemaDiff::IndexDropped(n) => (
                DriftType::MissingObject,
                format!("INDEX {}", n),
                "Index missing from DB".to_string(),
            ),
            _ => {
                // Generic handling for other diff types
                let detail = format!("{}", d);
                let drift_type = if detail.starts_with('+') {
                    DriftType::ExtraObject
                } else if detail.starts_with('-') {
                    DriftType::MissingObject
                } else {
                    DriftType::ModifiedObject
                };
                (drift_type, detail.clone(), detail)
            }
        };

        // Filter out the history table itself from drift results
        if object.contains(table) || object.contains("waypoint_drift_check") {
            continue;
        }

        drifts.push(DriftEntry {
            drift_type,
            object,
            detail,
        });
    }

    let has_drift = !drifts.is_empty();

    Ok(DriftReport {
        drifts,
        has_drift,
        schema: schema_name.to_string(),
    })
}
