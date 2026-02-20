//! Logical schema snapshots for rollback without undo files.
//!
//! Takes a snapshot of the current schema as DDL, stores it as a SQL file,
//! and can restore from a previous snapshot.

use std::path::PathBuf;

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::{Result, WaypointError};
use crate::schema;

/// Configuration for snapshots.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Directory where snapshot files are stored.
    pub directory: PathBuf,
    /// Whether to automatically take a snapshot before each migration.
    pub auto_snapshot_on_migrate: bool,
    /// Maximum number of snapshots to retain (oldest are pruned).
    pub max_snapshots: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from(".waypoint/snapshots"),
            auto_snapshot_on_migrate: false,
            max_snapshots: 10,
        }
    }
}

/// Report from a snapshot operation.
#[derive(Debug, Serialize)]
pub struct SnapshotReport {
    /// Unique identifier for the snapshot (timestamp-based).
    pub snapshot_id: String,
    /// Filesystem path where the snapshot SQL file was written.
    pub snapshot_path: String,
    /// Total number of schema objects captured in the snapshot.
    pub objects_captured: usize,
}

/// Report from a restore operation.
#[derive(Debug, Serialize)]
pub struct RestoreReport {
    /// Identifier of the snapshot that was restored.
    pub snapshot_id: String,
    /// Number of schema objects successfully restored.
    pub objects_restored: usize,
}

/// Info about an available snapshot.
#[derive(Debug, Serialize)]
pub struct SnapshotInfo {
    /// Unique identifier for the snapshot.
    pub id: String,
    /// Filesystem path to the snapshot SQL file.
    pub path: PathBuf,
    /// Size of the snapshot file in bytes.
    pub size_bytes: u64,
    /// Human-readable creation timestamp.
    pub created: String,
}

/// Take a snapshot of the current schema.
pub async fn execute_snapshot(
    client: &Client,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
) -> Result<SnapshotReport> {
    let schema_name = &config.migrations.schema;

    // Introspect the schema
    let snapshot = schema::introspect(client, schema_name).await?;

    // Generate DDL
    let ddl = schema::to_ddl(&snapshot);

    // Create snapshot directory
    let dir = &snapshot_config.directory;
    std::fs::create_dir_all(dir)?;

    // Generate snapshot ID
    let snapshot_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let sql_path = dir.join(format!("{}.sql", snapshot_id));
    let meta_path = dir.join(format!("{}.json", snapshot_id));

    // Count objects
    let objects_captured = snapshot.tables.len()
        + snapshot.views.len()
        + snapshot.indexes.len()
        + snapshot.sequences.len()
        + snapshot.functions.len()
        + snapshot.enums.len()
        + snapshot.constraints.len()
        + snapshot.triggers.len();

    // Write SQL file
    std::fs::write(&sql_path, &ddl)?;

    // Write metadata
    let meta = serde_json::json!({
        "snapshot_id": snapshot_id,
        "schema": schema_name,
        "objects_captured": objects_captured,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "tables": snapshot.tables.len(),
        "views": snapshot.views.len(),
        "indexes": snapshot.indexes.len(),
        "sequences": snapshot.sequences.len(),
        "functions": snapshot.functions.len(),
        "enums": snapshot.enums.len(),
    });
    std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap())?;

    // Prune old snapshots if over max
    prune_snapshots(dir, snapshot_config.max_snapshots)?;

    Ok(SnapshotReport {
        snapshot_id,
        snapshot_path: sql_path.display().to_string(),
        objects_captured,
    })
}

/// Restore a schema from a snapshot.
pub async fn execute_restore(
    client: &Client,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
    snapshot_id: &str,
) -> Result<RestoreReport> {
    let schema_name = &config.migrations.schema;
    let sql_path = snapshot_config.directory.join(format!("{}.sql", snapshot_id));

    if !sql_path.exists() {
        return Err(WaypointError::SnapshotError {
            reason: format!("Snapshot '{}' not found at {}", snapshot_id, sql_path.display()),
        });
    }

    let sql = std::fs::read_to_string(&sql_path)?;

    // Drop all objects in schema (like clean)
    let drop_sql = format!(
        "DROP SCHEMA IF EXISTS {} CASCADE; CREATE SCHEMA {};",
        crate::db::quote_ident(schema_name),
        crate::db::quote_ident(schema_name),
    );
    client.batch_execute(&drop_sql).await?;

    // Set search_path and execute snapshot DDL
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            crate::db::quote_ident(schema_name)
        ))
        .await?;

    // Execute the snapshot SQL
    let statements = crate::sql_parser::split_statements(&sql);
    let mut objects_restored = 0;
    for stmt in &statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        match client.batch_execute(trimmed).await {
            Ok(()) => objects_restored += 1,
            Err(e) => {
                log::warn!("Failed to restore statement, continuing; statement={}, error={}", &trimmed[..trimmed.len().min(80)], e);
            }
        }
    }

    Ok(RestoreReport {
        snapshot_id: snapshot_id.to_string(),
        objects_restored,
    })
}

/// List available snapshots.
pub fn list_snapshots(snapshot_config: &SnapshotConfig) -> Result<Vec<SnapshotInfo>> {
    let dir = &snapshot_config.directory;
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut snapshots = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "sql") {
            let id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let meta = entry.metadata()?;
            let created = meta
                .modified()
                .ok()
                .and_then(|t| {
                    t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .ok()
                })
                .map(|d| {
                    chrono::DateTime::from_timestamp(d.as_secs() as i64, 0)
                        .unwrap_or_default()
                        .format("%Y-%m-%d %H:%M:%S UTC")
                        .to_string()
                })
                .unwrap_or_default();

            snapshots.push(SnapshotInfo {
                id,
                path,
                size_bytes: meta.len(),
                created,
            });
        }
    }

    snapshots.sort_by(|a, b| b.id.cmp(&a.id)); // Newest first
    Ok(snapshots)
}

fn prune_snapshots(dir: &PathBuf, max: usize) -> Result<()> {
    let mut sql_files: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "sql")
        })
        .collect();

    sql_files.sort_by_key(|e| e.file_name());

    while sql_files.len() > max {
        if let Some(oldest) = sql_files.first() {
            let sql_path = oldest.path();
            let json_path = sql_path.with_extension("json");
            let _ = std::fs::remove_file(&sql_path);
            let _ = std::fs::remove_file(&json_path);
            sql_files.remove(0);
        }
    }

    Ok(())
}
