//! Standalone `waypoint safety` command for analyzing migration files.

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::Result;
use crate::safety;

/// Report from the standalone safety analysis command.
#[derive(Debug, Clone, Serialize)]
pub struct SafetyCommandReport {
    /// Per-file safety reports.
    pub reports: Vec<safety::SafetyReport>,
    /// Overall verdict across all files.
    pub overall_verdict: safety::SafetyVerdict,
}

/// Analyze a single migration file for safety.
pub async fn execute_file(
    client: &Client,
    config: &WaypointConfig,
    file_path: &str,
) -> Result<safety::SafetyReport> {
    let sql = std::fs::read_to_string(file_path)?;
    let script = std::path::Path::new(file_path)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| file_path.to_string());

    safety::analyze_migration(
        client,
        &config.migrations.schema,
        &sql,
        &script,
        &config.safety,
    )
    .await
}

/// Analyze all pending migration files for safety.
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<SafetyCommandReport> {
    use crate::history;
    use crate::migration::scan_migrations;

    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    history::create_history_table(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let mut reports = Vec::new();
    let mut overall = safety::SafetyVerdict::Safe;

    for migration in &resolved {
        if migration.is_undo() {
            continue;
        }
        if let Some(version) = migration.version() {
            if effective.contains(&version.raw) {
                continue; // Already applied
            }
        }

        let report = safety::analyze_migration(
            client,
            schema,
            &migration.sql,
            &migration.script,
            &config.safety,
        )
        .await?;

        if report.overall_verdict > overall {
            overall = report.overall_verdict;
        }
        reports.push(report);
    }

    Ok(SafetyCommandReport {
        reports,
        overall_verdict: overall,
    })
}
