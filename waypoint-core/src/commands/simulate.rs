//! Migration simulation: run pending migrations in a throwaway schema
//! to prove they will succeed before applying to the real schema.

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db::quote_ident;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::migration::scan_migrations;
use crate::placeholder::{build_placeholders, replace_placeholders};
use crate::schema;

/// Report from a migration simulation.
#[derive(Debug, Clone, Serialize)]
pub struct SimulationReport {
    /// Whether all pending migrations passed simulation.
    pub passed: bool,
    /// Number of migrations simulated.
    pub migrations_simulated: usize,
    /// Name of the temporary schema used.
    pub temp_schema: String,
    /// Errors encountered during simulation.
    pub errors: Vec<SimulationError>,
}

/// An error encountered during simulation.
#[derive(Debug, Clone, Serialize)]
pub struct SimulationError {
    /// The migration script that failed.
    pub script: String,
    /// Error message.
    pub error: String,
}

/// Execute migration simulation in a throwaway schema.
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<SimulationReport> {
    let schema_name = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create history table if needed (for querying applied state)
    history::create_history_table(client, schema_name, table).await?;

    // Generate a unique temp schema name
    let temp_schema = format!(
        "waypoint_sim_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );

    let result = run_simulation(client, config, &temp_schema).await;

    // Always clean up the temp schema (retry once on failure)
    let drop_sql = format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_ident(&temp_schema)
    );
    if let Err(e) = client.batch_execute(&drop_sql).await {
        log::warn!(
            "First attempt to drop simulation schema {} failed, retrying: {}",
            temp_schema,
            e
        );
        if let Err(e2) = client.batch_execute(&drop_sql).await {
            log::error!(
                "Failed to drop simulation schema {} after retry: {}",
                temp_schema,
                e2
            );
        }
    }

    result
}

async fn run_simulation(
    client: &Client,
    config: &WaypointConfig,
    temp_schema: &str,
) -> Result<SimulationReport> {
    let schema_name = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create the temp schema
    let create_sql = format!("CREATE SCHEMA {}", quote_ident(temp_schema));
    client
        .batch_execute(&create_sql)
        .await
        .map_err(|e| WaypointError::SimulationFailed {
            reason: format!("Failed to create simulation schema: {}", e),
        })?;

    // Replicate current schema structure into temp schema
    let snapshot = schema::introspect(client, schema_name).await?;
    let ddl = schema::to_ddl(&snapshot);

    if !ddl.is_empty() {
        // Set search_path to temp schema for DDL execution
        let set_path = format!("SET search_path TO {}", quote_ident(temp_schema));
        client
            .batch_execute(&set_path)
            .await
            .map_err(|e| WaypointError::SimulationFailed {
                reason: format!("Failed to set search_path: {}", e),
            })?;

        // Execute DDL to replicate structure (ignore errors for complex objects)
        if let Err(e) = client.batch_execute(&ddl).await {
            log::debug!("Partial schema replication in simulation: {}", e);
        }
    }

    // Set search_path to temp schema
    let set_path = format!("SET search_path TO {}", quote_ident(temp_schema));
    client
        .batch_execute(&set_path)
        .await
        .map_err(|e| WaypointError::SimulationFailed {
            reason: format!("Failed to set search_path: {}", e),
        })?;

    // Get pending migrations
    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations(client, schema_name, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let db_user = crate::db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = crate::db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    let mut errors = Vec::new();
    let mut simulated = 0;

    for migration in &resolved {
        if migration.is_undo() {
            continue;
        }
        if let Some(version) = migration.version() {
            if effective.contains(&version.raw) {
                continue; // Already applied
            }
        }

        let placeholders = build_placeholders(
            &config.placeholders,
            temp_schema,
            &db_user,
            &db_name,
            &migration.script,
        );
        let sql = match replace_placeholders(&migration.sql, &placeholders) {
            Ok(s) => s,
            Err(e) => {
                errors.push(SimulationError {
                    script: migration.script.clone(),
                    error: e.to_string(),
                });
                continue;
            }
        };

        match client.batch_execute(&sql).await {
            Ok(_) => {
                simulated += 1;
            }
            Err(e) => {
                errors.push(SimulationError {
                    script: migration.script.clone(),
                    error: crate::error::format_db_error(&e),
                });
            }
        }
    }

    // Restore search_path
    let restore_path = format!("SET search_path TO {}", quote_ident(schema_name));
    if let Err(e) = client.batch_execute(&restore_path).await {
        log::warn!("Failed to restore search_path: {}", e);
    }

    Ok(SimulationReport {
        passed: errors.is_empty(),
        migrations_simulated: simulated,
        temp_schema: temp_schema.to_string(),
        errors,
    })
}
