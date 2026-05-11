//! Standalone `waypoint advise` command for schema advisory analysis.

use std::path::Path;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::advisor::{self, AdvisorReport};
use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::error::Result;

/// Execute the advise command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<AdvisorReport> {
    advisor::analyze(client, &config.migrations.schema, &config.advisor).await
}

/// Execute the advise command (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<AdvisorReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    advisor::analyze_db(client, &schema, &config.advisor).await
}

/// Write fix SQL from an advisor report to a file.
pub fn write_fix_file(report: &AdvisorReport, path: &str) -> Result<()> {
    let sql = advisor::generate_fix_sql(report);
    if sql.is_empty() {
        return Ok(());
    }

    // Ensure parent directory exists
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(path, sql)?;
    Ok(())
}
