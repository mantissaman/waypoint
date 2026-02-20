//! Standalone preflight command wrapper.

use tokio_postgres::Client;

use crate::error::Result;
use crate::preflight::{self, PreflightConfig, PreflightReport};

/// Execute the standalone preflight command.
pub async fn execute(client: &Client, config: &PreflightConfig) -> Result<PreflightReport> {
    preflight::run_preflight(client, config).await
}
