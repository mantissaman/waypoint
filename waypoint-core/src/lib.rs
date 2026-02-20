//! Lightweight, Flyway-compatible PostgreSQL migration library.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use waypoint_core::config::WaypointConfig;
//! use waypoint_core::Waypoint;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let config = WaypointConfig::load(None, None)?;
//! let wp = Waypoint::new(config).await?;
//! let report = wp.migrate(None).await?;
//! println!("Applied {} migrations", report.migrations_applied);
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! - [`config`] — Configuration loading (TOML, env vars, CLI overrides)
//! - [`migration`] — Migration file parsing and scanning
//! - [`db`] — Database connections, TLS, advisory locks
//! - [`history`] — Schema history table operations
//! - [`commands`] — Individual command implementations
//! - [`checksum`] — CRC32 checksums (Flyway-compatible)
//! - [`placeholder`] — `${key}` placeholder replacement in SQL
//! - [`hooks`] — SQL callback hooks (before/after migrate)
//! - [`directive`] — `-- waypoint:*` comment directive parsing
//! - [`guard`] — Guard expression parser and evaluator for pre/post conditions
//! - [`sql_parser`] — Regex-based DDL extraction
//! - [`safety`] — Migration safety analysis (lock levels, impact, verdicts)
//! - [`schema`] — PostgreSQL schema introspection + diff
//! - [`dependency`] — Migration dependency graph
//! - [`preflight`] — Pre-migration health checks
//! - [`multi`] — Multi-database orchestration
//! - [`error`] — Error types

pub mod advisor;
pub mod checksum;
pub mod commands;
pub mod config;
pub mod db;
pub mod dependency;
pub mod directive;
pub mod error;
pub mod guard;
pub mod history;
pub mod hooks;
pub mod migration;
pub mod multi;
pub mod placeholder;
pub mod preflight;
pub mod reversal;
pub mod safety;
pub mod schema;
pub mod sql_parser;

use std::path::PathBuf;

use config::WaypointConfig;
use error::Result;
use tokio_postgres::Client;

pub use advisor::AdvisorReport;
pub use commands::changelog::ChangelogReport;
pub use commands::check_conflicts::ConflictReport;
pub use commands::diff::DiffReport;
pub use commands::drift::DriftReport;
pub use commands::explain::ExplainReport;
pub use commands::info::{MigrationInfo, MigrationState};
pub use commands::lint::LintReport;
pub use commands::migrate::MigrateReport;
pub use commands::repair::RepairReport;
pub use commands::safety::SafetyCommandReport;
pub use commands::simulate::SimulationReport;
pub use commands::snapshot::{RestoreReport, SnapshotReport};
pub use commands::undo::{UndoReport, UndoTarget};
pub use commands::validate::ValidateReport;
pub use config::CliOverrides;
pub use multi::MultiWaypoint;
pub use preflight::PreflightReport;
pub use safety::SafetyReport;

/// Main entry point for the Waypoint library.
///
/// Create a `Waypoint` instance with a config and use its methods to
/// run migration commands programmatically.
pub struct Waypoint {
    pub config: WaypointConfig,
    client: Client,
}

impl Waypoint {
    /// Create a new Waypoint instance, connecting to the database.
    ///
    /// If `connect_retries` is configured, retries with exponential backoff.
    pub async fn new(config: WaypointConfig) -> Result<Self> {
        let conn_string = config.connection_string()?;
        let client = db::connect_with_full_config(
            &conn_string,
            &config.database.ssl_mode,
            config.database.connect_retries,
            config.database.connect_timeout_secs,
            config.database.statement_timeout_secs,
            config.database.keepalive_secs,
        )
        .await?;
        Ok(Self { config, client })
    }

    /// Create a new Waypoint instance with an existing database client.
    pub fn with_client(config: WaypointConfig, client: Client) -> Self {
        Self { config, client }
    }

    /// Get a reference to the underlying database client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Apply pending migrations.
    pub async fn migrate(&self, target_version: Option<&str>) -> Result<MigrateReport> {
        commands::migrate::execute(&self.client, &self.config, target_version).await
    }

    /// Show migration status information.
    pub async fn info(&self) -> Result<Vec<MigrationInfo>> {
        commands::info::execute(&self.client, &self.config).await
    }

    /// Validate applied migrations against local files.
    pub async fn validate(&self) -> Result<ValidateReport> {
        commands::validate::execute(&self.client, &self.config).await
    }

    /// Repair the schema history table.
    pub async fn repair(&self) -> Result<RepairReport> {
        commands::repair::execute(&self.client, &self.config).await
    }

    /// Baseline an existing database.
    pub async fn baseline(&self, version: Option<&str>, description: Option<&str>) -> Result<()> {
        commands::baseline::execute(&self.client, &self.config, version, description).await
    }

    /// Undo applied migrations.
    pub async fn undo(&self, target: UndoTarget) -> Result<UndoReport> {
        commands::undo::execute(&self.client, &self.config, target).await
    }

    /// Drop all objects in managed schemas.
    pub async fn clean(&self, allow_clean: bool) -> Result<Vec<String>> {
        commands::clean::execute(&self.client, &self.config, allow_clean).await
    }

    /// Run lint on migration files (no DB required).
    pub fn lint(locations: &[PathBuf], disabled_rules: &[String]) -> Result<LintReport> {
        commands::lint::execute(locations, disabled_rules)
    }

    /// Generate changelog from migration files (no DB required).
    pub fn changelog(
        locations: &[PathBuf],
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ChangelogReport> {
        commands::changelog::execute(locations, from, to)
    }

    /// Compare database schema against a target.
    pub async fn diff(&self, target: commands::diff::DiffTarget) -> Result<DiffReport> {
        commands::diff::execute(&self.client, &self.config, target).await
    }

    /// Detect schema drift.
    pub async fn drift(&self) -> Result<DriftReport> {
        commands::drift::execute(&self.client, &self.config).await
    }

    /// Take a schema snapshot.
    pub async fn snapshot(
        &self,
        snapshot_config: &commands::snapshot::SnapshotConfig,
    ) -> Result<SnapshotReport> {
        commands::snapshot::execute_snapshot(&self.client, &self.config, snapshot_config).await
    }

    /// Restore from a schema snapshot.
    pub async fn restore(
        &self,
        snapshot_config: &commands::snapshot::SnapshotConfig,
        snapshot_id: &str,
    ) -> Result<RestoreReport> {
        commands::snapshot::execute_restore(
            &self.client,
            &self.config,
            snapshot_config,
            snapshot_id,
        )
        .await
    }

    /// Run enhanced dry-run with EXPLAIN.
    pub async fn explain(&self) -> Result<ExplainReport> {
        commands::explain::execute(&self.client, &self.config).await
    }

    /// Run pre-flight health checks.
    pub async fn preflight(&self) -> Result<PreflightReport> {
        preflight::run_preflight(&self.client, &self.config.preflight).await
    }

    /// Check for branch conflicts (no DB required).
    pub fn check_conflicts(locations: &[PathBuf], base_branch: &str) -> Result<ConflictReport> {
        commands::check_conflicts::execute(locations, base_branch)
    }

    /// Analyze pending migrations for safety (lock analysis, impact estimation).
    pub async fn safety(&self) -> Result<SafetyCommandReport> {
        commands::safety::execute(&self.client, &self.config).await
    }

    /// Run schema advisor to suggest improvements.
    pub async fn advise(&self) -> Result<AdvisorReport> {
        commands::advisor::execute(&self.client, &self.config).await
    }

    /// Simulate pending migrations in a throwaway schema.
    pub async fn simulate(&self) -> Result<SimulationReport> {
        commands::simulate::execute(&self.client, &self.config).await
    }
}
