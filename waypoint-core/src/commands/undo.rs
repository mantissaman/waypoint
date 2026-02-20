//! Undo applied migrations by executing U{version}__*.sql files.

use std::collections::HashMap;

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::migration::{scan_migrations, MigrationVersion, ResolvedMigration};
use crate::placeholder::{build_placeholders, replace_placeholders};

/// How many / which versions to undo.
#[derive(Debug, Clone)]
pub enum UndoTarget {
    /// Undo the single most recently applied migration.
    Last,
    /// Undo all migrations above this version (the target version itself stays applied).
    Version(MigrationVersion),
    /// Undo the last N applied migrations in reverse order.
    Count(usize),
}

/// Report returned after an undo operation.
#[derive(Debug, Serialize)]
pub struct UndoReport {
    /// Number of migrations that were undone.
    pub migrations_undone: usize,
    /// Total execution time of all undo operations in milliseconds.
    pub total_time_ms: i32,
    /// Per-migration details for each undone migration.
    pub details: Vec<UndoDetail>,
}

/// Details of a single undone migration.
#[derive(Debug, Serialize)]
pub struct UndoDetail {
    /// Version string of the migration that was undone.
    pub version: String,
    /// Human-readable description from the undo migration filename.
    pub description: String,
    /// Filename of the undo migration script that was executed.
    pub script: String,
    /// Execution time of the undo operation in milliseconds.
    pub execution_time_ms: i32,
}

/// Execute the undo command.
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    let table = &config.migrations.table;

    // Acquire advisory lock
    db::acquire_advisory_lock(client, table).await?;

    let result = run_undo(client, config, target).await;

    // Always release the advisory lock
    if let Err(e) = db::release_advisory_lock(client, table).await {
        log::warn!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => {
            log::info!("Undo completed; migrations_undone={}, total_time_ms={}", report.migrations_undone, report.total_time_ms);
        }
        Err(e) => {
            log::error!("Undo failed: {}", e);
        }
    }

    result
}

async fn run_undo(
    client: &Client,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create history table if not exists
    history::create_history_table(client, schema, table).await?;

    // Scan migration files — build map of undo files by version
    let resolved = scan_migrations(&config.migrations.locations)?;
    let undo_by_version: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_undo())
        .filter_map(|m| m.version().map(|v| (v.raw.clone(), m)))
        .collect();

    // Get applied history and compute effective set
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let effective = history::effective_applied_versions(&applied);

    // Build list of currently-applied versioned migrations, sorted descending by version
    let mut applied_versions: Vec<MigrationVersion> = effective
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .collect();
    applied_versions.sort();
    applied_versions.reverse(); // newest first

    // Determine which versions to undo
    let versions_to_undo: Vec<MigrationVersion> = match target {
        UndoTarget::Last => applied_versions.into_iter().take(1).collect(),
        UndoTarget::Count(n) => applied_versions.into_iter().take(n).collect(),
        UndoTarget::Version(ref target_ver) => applied_versions
            .into_iter()
            .filter(|v| v > target_ver)
            .collect(),
    };

    // Get database user info for placeholders
    let db_user = db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or(&db_user);

    let mut report = UndoReport {
        migrations_undone: 0,
        total_time_ms: 0,
        details: Vec::new(),
    };

    // Execute undo for each version (newest first)
    for version in &versions_to_undo {
        let undo_migration = undo_by_version.get(&version.raw).ok_or_else(|| {
            WaypointError::UndoMissing {
                version: version.raw.clone(),
            }
        })?;

        log::info!("Undoing migration; migration={}, schema={}", undo_migration.script, schema);

        // Build and apply placeholders
        let placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &db_user,
            &db_name,
            &undo_migration.script,
        );
        let sql = replace_placeholders(&undo_migration.sql, &placeholders)?;

        // Execute in transaction
        match db::execute_in_transaction(client, &sql).await {
            Ok(exec_time) => {
                // Record UNDO_SQL success
                history::insert_applied_migration(
                    client,
                    schema,
                    table,
                    Some(&version.raw),
                    &undo_migration.description,
                    "UNDO_SQL",
                    &undo_migration.script,
                    Some(undo_migration.checksum),
                    installed_by,
                    exec_time,
                    true,
                )
                .await?;

                report.migrations_undone += 1;
                report.total_time_ms += exec_time;
                report.details.push(UndoDetail {
                    version: version.raw.clone(),
                    description: undo_migration.description.clone(),
                    script: undo_migration.script.clone(),
                    execution_time_ms: exec_time,
                });
            }
            Err(e) => {
                // Record failure
                if let Err(record_err) = history::insert_applied_migration(
                    client,
                    schema,
                    table,
                    Some(&version.raw),
                    &undo_migration.description,
                    "UNDO_SQL",
                    &undo_migration.script,
                    Some(undo_migration.checksum),
                    installed_by,
                    0,
                    false,
                )
                .await
                {
                    log::warn!("Failed to record undo failure in history table; script={}, error={}", undo_migration.script, record_err);
                }

                let reason = match &e {
                    WaypointError::DatabaseError(db_err) => crate::error::format_db_error(db_err),
                    other => other.to_string(),
                };
                log::error!("Undo failed; script={}, reason={}", undo_migration.script, reason);
                return Err(WaypointError::UndoFailed {
                    script: undo_migration.script.clone(),
                    reason,
                });
            }
        }
    }

    Ok(report)
}
