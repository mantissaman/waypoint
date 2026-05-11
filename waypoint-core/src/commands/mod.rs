//! Command implementations: migrate, info, validate, repair, baseline, clean,
//! lint, changelog, diff, drift, snapshot, explain, check-conflicts, safety,
//! advisor, simulate. The `preflight` command is exposed via
//! [`crate::preflight::run_preflight_db`] directly (no command-wrapper module).

pub mod advisor;
pub mod baseline;
pub mod changelog;
pub mod check_conflicts;
pub mod clean;
pub mod diff;
pub mod drift;
pub mod explain;
pub mod info;
pub mod lint;
pub mod migrate;
pub mod repair;
pub mod safety;
pub mod simulate;
pub mod snapshot;
pub mod undo;
pub mod validate;
