//! Apply pending migrations to the database.
//!
//! This module owns the engine-agnostic public types ([`MigrateReport`],
//! [`MigrateDetail`]) and a handful of shared helpers used by both
//! engine-specific implementations. The actual `execute*` entry points
//! live in [`crate::engines::postgres::migrate`] and
//! [`crate::engines::mysql::migrate`] and are re-exported here so that
//! downstream callers (and the library `Waypoint` façade) can keep using
//! the historical paths under `crate::commands::migrate::*`.

use serde::Serialize;

use crate::directive::MigrationDirectives;
use crate::error::WaypointError;

// ── Re-exports of the engine-specific entry points ──────────────────────────
//
// `multi.rs` and `lib.rs` reference these paths today. Keeping the names
// where they used to live preserves the public API and back-compat.

#[cfg(feature = "mysql")]
pub use crate::engines::mysql::migrate::{
    execute as execute_mysql, execute_with_options as execute_mysql_with_options,
};
#[cfg(feature = "postgres")]
pub use crate::engines::postgres::migrate::{execute, execute_with_options};

// ── Engine-agnostic public types ────────────────────────────────────────────

/// Report returned after a migrate operation.
#[derive(Debug, Serialize)]
pub struct MigrateReport {
    /// Number of migrations that were applied in this run.
    pub migrations_applied: usize,
    /// Total execution time of all migrations in milliseconds.
    pub total_time_ms: i32,
    /// Per-migration details for each applied migration.
    pub details: Vec<MigrateDetail>,
    /// Number of lifecycle hooks that were executed.
    pub hooks_executed: usize,
    /// Total execution time of all hooks in milliseconds.
    pub hooks_time_ms: i32,
}

/// Details of a single applied migration within a migrate run.
#[derive(Debug, Serialize)]
pub struct MigrateDetail {
    /// Version string, or None for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description from the migration filename.
    pub description: String,
    /// Filename of the migration script.
    pub script: String,
    /// Execution time of this migration in milliseconds.
    pub execution_time_ms: i32,
}

// ── Shared helpers used by both engine paths ────────────────────────────────

/// Result of evaluating require-guard preconditions for a single migration.
pub(crate) enum GuardAction {
    /// All preconditions passed; proceed with the migration.
    Continue,
    /// A precondition failed with on_require_fail=Skip; skip this migration.
    Skip,
    /// A precondition failed fatally; abort with the given error.
    Error(WaypointError),
}

/// Check if a migration should run in the current environment.
///
/// Returns true if:
/// - The migration has no env directives (runs everywhere)
/// - No environment is configured (runs everything)
/// - The migration's env list includes the current environment
pub(crate) fn should_run_in_environment(
    directives: &MigrationDirectives,
    current_env: Option<&str>,
) -> bool {
    if directives.env.is_empty() {
        return true;
    }
    let env = match current_env {
        Some(e) => e,
        None => return true,
    };
    directives.env.iter().any(|e| e.eq_ignore_ascii_case(env))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_run_in_environment_no_directives() {
        let directives = MigrationDirectives::default();
        assert!(should_run_in_environment(&directives, Some("production")));
        assert!(should_run_in_environment(&directives, None));
    }

    #[test]
    fn test_should_run_in_environment_matches() {
        let directives = MigrationDirectives {
            env: vec!["production".to_string(), "staging".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, Some("production")));
        assert!(should_run_in_environment(&directives, Some("staging")));
        assert!(!should_run_in_environment(&directives, Some("dev")));
    }

    #[test]
    fn test_should_run_in_environment_case_insensitive() {
        let directives = MigrationDirectives {
            env: vec!["PROD".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, Some("prod")));
        assert!(should_run_in_environment(&directives, Some("PROD")));
        assert!(should_run_in_environment(&directives, Some("Prod")));
        assert!(!should_run_in_environment(&directives, Some("dev")));
    }

    #[test]
    fn test_should_run_in_environment_no_env_configured() {
        let directives = MigrationDirectives {
            env: vec!["prod".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, None));
    }
}
