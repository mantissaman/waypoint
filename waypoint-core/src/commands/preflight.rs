//! Standalone preflight command wrapper.

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::db::DbClient;
use crate::error::Result;
use crate::preflight::{self, PreflightConfig, PreflightReport};

/// Execute the standalone preflight command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(client: &Client, config: &PreflightConfig) -> Result<PreflightReport> {
    preflight::run_preflight(client, config).await
}

/// Execute the standalone preflight command (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &PreflightConfig) -> Result<PreflightReport> {
    preflight::run_preflight_db(client, config).await
}
