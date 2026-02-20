//! Baseline an existing database at a specific version.

use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db;
use crate::error::{Result, WaypointError};
use crate::history;

/// Execute the baseline command.
///
/// 1. Fail if history table already has entries
/// 2. Create history table
/// 3. Insert a single baseline row
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    baseline_version: Option<&str>,
    baseline_description: Option<&str>,
) -> Result<()> {
    let table = &config.migrations.table;

    // Acquire advisory lock to prevent concurrent operations
    db::acquire_advisory_lock(client, table).await?;

    let result = execute_inner(client, config, baseline_version, baseline_description).await;

    // Always release the lock
    if let Err(e) = db::release_advisory_lock(client, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

async fn execute_inner(
    client: &Client,
    config: &WaypointConfig,
    baseline_version: Option<&str>,
    baseline_description: Option<&str>,
) -> Result<()> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;
    let version = baseline_version.unwrap_or(&config.migrations.baseline_version);
    let description = baseline_description.unwrap_or("<< Waypoint Baseline >>");

    // Create history table if not exists
    history::create_history_table(client, schema, table).await?;

    // Check if history table already has entries
    if history::has_entries(client, schema, table).await? {
        return Err(WaypointError::BaselineExists);
    }

    // Insert baseline row
    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or("waypoint");

    history::insert_applied_migration(
        client,
        schema,
        table,
        Some(version),
        description,
        "BASELINE",
        "<< Waypoint Baseline >>",
        None,
        installed_by,
        0,
        true,
    )
    .await?;

    log::info!(
        "Successfully baselined schema; version={}, schema={}",
        version,
        schema
    );
    Ok(())
}
