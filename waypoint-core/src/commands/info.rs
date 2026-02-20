//! Show migration status by merging resolved files with applied history.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::Result;
use crate::history;
use crate::migration::{scan_migrations, MigrationKind, MigrationVersion, ResolvedMigration};

/// The state of a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum MigrationState {
    /// Migration file exists on disk but has not been applied yet.
    Pending,
    /// Migration has been successfully applied to the database.
    Applied,
    /// Migration execution failed (recorded in history as unsuccessful).
    Failed,
    /// Migration is recorded in history but its file is missing from disk.
    Missing,
    /// Repeatable migration whose checksum has changed since last application.
    Outdated,
    /// Versioned migration with a version lower than the highest applied version.
    OutOfOrder,
    /// Versioned migration with a version at or below the baseline.
    BelowBaseline,
    /// Migration was skipped (e.g. filtered by environment).
    Ignored,
    /// A baseline marker entry in the history table.
    Baseline,
    /// Migration was applied but subsequently reverted by an undo operation.
    Undone,
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationState::Pending => write!(f, "Pending"),
            MigrationState::Applied => write!(f, "Applied"),
            MigrationState::Failed => write!(f, "Failed"),
            MigrationState::Missing => write!(f, "Missing"),
            MigrationState::Outdated => write!(f, "Outdated"),
            MigrationState::OutOfOrder => write!(f, "Out of Order"),
            MigrationState::BelowBaseline => write!(f, "Below Baseline"),
            MigrationState::Ignored => write!(f, "Ignored"),
            MigrationState::Baseline => write!(f, "Baseline"),
            MigrationState::Undone => write!(f, "Undone"),
        }
    }
}

/// Combined view of a migration (file + history).
#[derive(Debug, Clone, Serialize)]
pub struct MigrationInfo {
    /// Version string, or None for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description from the migration filename.
    pub description: String,
    /// Type of migration (e.g. "SQL", "BASELINE", "UNDO_SQL").
    pub migration_type: String,
    /// Filename of the migration script.
    pub script: String,
    /// Current state of this migration.
    pub state: MigrationState,
    /// Timestamp when the migration was applied, if recorded in history.
    pub installed_on: Option<DateTime<Utc>>,
    /// Execution time in milliseconds, if recorded in history.
    pub execution_time: Option<i32>,
    /// CRC32 checksum of the migration SQL content.
    pub checksum: Option<i32>,
}

/// Execute the info command: merge resolved files and applied history into a unified view.
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<Vec<MigrationInfo>> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    // Ensure history table exists
    if !history::history_table_exists(client, schema, table).await? {
        // No history table — all resolved migrations are Pending (skip undo files)
        let resolved = scan_migrations(&config.migrations.locations)?;
        return Ok(resolved
            .into_iter()
            .filter(|m| !m.is_undo())
            .map(|m| {
                let version = m.version().map(|v| v.raw.clone());
                let migration_type = m.migration_type().to_string();
                MigrationInfo {
                    version,
                    description: m.description,
                    migration_type,
                    script: m.script,
                    state: MigrationState::Pending,
                    installed_on: None,
                    execution_time: None,
                    checksum: Some(m.checksum),
                }
            })
            .collect());
    }

    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations(client, schema, table).await?;

    // Compute effective applied versions (respects undo state)
    let effective = history::effective_applied_versions(&applied);

    // Build lookup maps (exclude undo files — they aren't shown as separate info rows)
    let resolved_by_version: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter_map(|m| m.version().map(|v| (v.raw.clone(), m)))
        .collect();

    let resolved_by_script: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| !m.is_versioned() && !m.is_undo())
        .map(|m| (m.script.clone(), m))
        .collect();

    // Find baseline version
    let baseline_version = applied
        .iter()
        .find(|a| a.migration_type == "BASELINE")
        .and_then(|a| a.version.as_ref())
        .map(|v| MigrationVersion::parse(v))
        .transpose()?;

    // Highest effectively-applied version
    let highest_applied = effective
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .max();

    let mut infos: Vec<MigrationInfo> = Vec::new();

    // Process applied migrations first (to track what's in history)
    let mut seen_versions: HashMap<String, bool> = HashMap::new();
    let mut seen_scripts: HashMap<String, bool> = HashMap::new();

    for am in &applied {
        // Distinguish versioned vs repeatable by presence of version (not type string),
        // for compatibility with Flyway which stores both as type "SQL".
        let is_versioned = am.version.is_some();
        let is_repeatable = am.version.is_none() && am.migration_type != "BASELINE";

        let state = if am.migration_type == "BASELINE" {
            MigrationState::Baseline
        } else if am.migration_type == "UNDO_SQL" {
            MigrationState::Undone
        } else if !am.success {
            MigrationState::Failed
        } else if is_versioned {
            if let Some(ref version) = am.version {
                if !effective.contains(version) {
                    // This forward migration was later undone
                    MigrationState::Undone
                } else if resolved_by_version.contains_key(version) {
                    MigrationState::Applied
                } else {
                    MigrationState::Missing
                }
            } else {
                MigrationState::Applied
            }
        } else if is_repeatable {
            // Check if file still exists and if checksum changed
            if let Some(resolved) = resolved_by_script.get(&am.script) {
                if Some(resolved.checksum) != am.checksum {
                    MigrationState::Outdated
                } else {
                    MigrationState::Applied
                }
            } else {
                MigrationState::Missing
            }
        } else {
            MigrationState::Applied
        };

        if let Some(ref v) = am.version {
            seen_versions.insert(v.clone(), true);
        }
        if am.version.is_none() {
            seen_scripts.insert(am.script.clone(), true);
        }

        infos.push(MigrationInfo {
            version: am.version.clone(),
            description: am.description.clone(),
            migration_type: am.migration_type.clone(),
            script: am.script.clone(),
            state,
            installed_on: Some(am.installed_on),
            execution_time: Some(am.execution_time),
            checksum: am.checksum,
        });
    }

    // Add pending resolved migrations not in history (skip undo files)
    for m in &resolved {
        if m.is_undo() {
            continue;
        }
        match &m.kind {
            MigrationKind::Versioned(version) => {
                if seen_versions.contains_key(&version.raw) {
                    continue;
                }

                let state = if let Some(ref bv) = baseline_version {
                    if version <= bv {
                        MigrationState::BelowBaseline
                    } else if let Some(ref highest) = highest_applied {
                        if version < highest {
                            MigrationState::OutOfOrder
                        } else {
                            MigrationState::Pending
                        }
                    } else {
                        MigrationState::Pending
                    }
                } else if let Some(ref highest) = highest_applied {
                    if version < highest {
                        MigrationState::OutOfOrder
                    } else {
                        MigrationState::Pending
                    }
                } else {
                    MigrationState::Pending
                };

                infos.push(MigrationInfo {
                    version: Some(version.raw.clone()),
                    description: m.description.clone(),
                    migration_type: m.migration_type().to_string(),
                    script: m.script.clone(),
                    state,
                    installed_on: None,
                    execution_time: None,
                    checksum: Some(m.checksum),
                });
            }
            MigrationKind::Repeatable => {
                if seen_scripts.contains_key(&m.script) {
                    continue; // Already handled above (Applied or Outdated)
                }

                infos.push(MigrationInfo {
                    version: None,
                    description: m.description.clone(),
                    migration_type: m.migration_type().to_string(),
                    script: m.script.clone(),
                    state: MigrationState::Pending,
                    installed_on: None,
                    execution_time: None,
                    checksum: Some(m.checksum),
                });
            }
            MigrationKind::Undo(_) => unreachable!("undo files are skipped above"),
        }
    }

    // Sort: versioned by version, then repeatable by description
    infos.sort_by(|a, b| match (&a.version, &b.version) {
        (Some(av), Some(bv)) => {
            let pa = MigrationVersion::parse(av);
            let pb = MigrationVersion::parse(bv);
            match (pa, pb) {
                (Ok(pa), Ok(pb)) => pa.cmp(&pb),
                _ => av.cmp(bv),
            }
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.description.cmp(&b.description),
    });

    Ok(infos)
}
