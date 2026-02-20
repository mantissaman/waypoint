//! Pre-flight health checks run before migrations.
//!
//! Checks database health metrics like recovery mode, active connections,
//! long-running queries, replication lag, and lock contention.

use serde::Serialize;
use tokio_postgres::Client;

use crate::error::Result;

/// Result of a single pre-flight check.
#[derive(Debug, Clone, Serialize)]
pub struct PreflightCheck {
    /// Human-readable name of the check (e.g. "Recovery Mode").
    pub name: String,
    /// Whether the check passed, warned, or failed.
    pub status: CheckStatus,
    /// Descriptive detail about the check result.
    pub detail: String,
}

/// Status of a pre-flight check.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum CheckStatus {
    /// The check passed successfully.
    Pass,
    /// The check produced a non-blocking warning.
    Warn,
    /// The check failed and should block migration.
    Fail,
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckStatus::Pass => write!(f, "PASS"),
            CheckStatus::Warn => write!(f, "WARN"),
            CheckStatus::Fail => write!(f, "FAIL"),
        }
    }
}

/// Aggregate report of all pre-flight checks.
#[derive(Debug, Serialize)]
pub struct PreflightReport {
    /// Individual check results.
    pub checks: Vec<PreflightCheck>,
    /// Whether all checks passed (no failures).
    pub passed: bool,
}

/// Configuration for pre-flight checks.
#[derive(Debug, Clone)]
pub struct PreflightConfig {
    /// Whether pre-flight checks are enabled before migrations.
    pub enabled: bool,
    /// Maximum acceptable replication lag in megabytes before warning.
    pub max_replication_lag_mb: i64,
    /// Threshold in seconds for detecting long-running queries.
    pub long_query_threshold_secs: i64,
}

impl Default for PreflightConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_replication_lag_mb: 100,
            long_query_threshold_secs: 300,
        }
    }
}

/// Run all pre-flight checks against the database.
pub async fn run_preflight(client: &Client, config: &PreflightConfig) -> Result<PreflightReport> {
    let mut checks = Vec::new();

    checks.push(check_recovery_mode(client).await);
    checks.push(check_active_connections(client).await);
    checks.push(check_long_running_queries(client, config.long_query_threshold_secs).await);
    checks.push(check_replication_lag(client, config.max_replication_lag_mb).await);
    checks.push(check_database_size(client).await);
    checks.push(check_lock_contention(client).await);

    let passed = !checks.iter().any(|c| c.status == CheckStatus::Fail);

    Ok(PreflightReport { checks, passed })
}

async fn check_recovery_mode(client: &Client) -> PreflightCheck {
    match client.query_one("SELECT pg_is_in_recovery()", &[]).await {
        Ok(row) => {
            let in_recovery: bool = row.get(0);
            if in_recovery {
                PreflightCheck {
                    name: "Recovery Mode".to_string(),
                    status: CheckStatus::Fail,
                    detail: "Database is in recovery mode (read-only replica)".to_string(),
                }
            } else {
                PreflightCheck {
                    name: "Recovery Mode".to_string(),
                    status: CheckStatus::Pass,
                    detail: "Not in recovery mode".to_string(),
                }
            }
        }
        Err(e) => PreflightCheck {
            name: "Recovery Mode".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

async fn check_active_connections(client: &Client) -> PreflightCheck {
    let query = "SELECT count(*)::int as active,
                        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_conn
                 FROM pg_stat_activity";
    match client.query_one(query, &[]).await {
        Ok(row) => {
            let active: i32 = row.get(0);
            let max_conn: i32 = row.get(1);
            let pct = (active as f64 / max_conn as f64) * 100.0;
            let status = if pct >= 80.0 {
                CheckStatus::Warn
            } else {
                CheckStatus::Pass
            };
            PreflightCheck {
                name: "Active Connections".to_string(),
                status,
                detail: format!("{}/{} ({:.0}%)", active, max_conn, pct),
            }
        }
        Err(e) => PreflightCheck {
            name: "Active Connections".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

async fn check_long_running_queries(client: &Client, threshold_secs: i64) -> PreflightCheck {
    let query = format!(
        "SELECT count(*)::int FROM pg_stat_activity
         WHERE state = 'active' AND now() - query_start > interval '{} seconds'",
        threshold_secs
    );
    match client.query_one(&query, &[]).await {
        Ok(row) => {
            let count: i32 = row.get(0);
            if count > 0 {
                PreflightCheck {
                    name: "Long-Running Queries".to_string(),
                    status: CheckStatus::Warn,
                    detail: format!(
                        "{} query(ies) running longer than {}s",
                        count, threshold_secs
                    ),
                }
            } else {
                PreflightCheck {
                    name: "Long-Running Queries".to_string(),
                    status: CheckStatus::Pass,
                    detail: format!("No queries running longer than {}s", threshold_secs),
                }
            }
        }
        Err(e) => PreflightCheck {
            name: "Long-Running Queries".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

async fn check_replication_lag(client: &Client, max_lag_mb: i64) -> PreflightCheck {
    let query = "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)
                 FROM pg_stat_replication
                 ORDER BY replay_lsn ASC LIMIT 1";
    match client.query_opt(query, &[]).await {
        Ok(Some(row)) => {
            let lag_bytes: Option<i64> = row.get(0);
            let lag_mb = lag_bytes.unwrap_or(0) / (1024 * 1024);
            let status = if lag_mb > max_lag_mb {
                CheckStatus::Warn
            } else {
                CheckStatus::Pass
            };
            PreflightCheck {
                name: "Replication Lag".to_string(),
                status,
                detail: format!("{}MB (threshold: {}MB)", lag_mb, max_lag_mb),
            }
        }
        Ok(None) => PreflightCheck {
            name: "Replication Lag".to_string(),
            status: CheckStatus::Pass,
            detail: "No replicas connected".to_string(),
        },
        Err(_) => PreflightCheck {
            name: "Replication Lag".to_string(),
            status: CheckStatus::Pass,
            detail: "Not a primary or no replication configured".to_string(),
        },
    }
}

async fn check_database_size(client: &Client) -> PreflightCheck {
    match client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await
    {
        Ok(row) => {
            let size_bytes: i64 = row.get(0);
            let size_mb = size_bytes / (1024 * 1024);
            let detail = if size_mb > 1024 {
                format!("{:.1}GB", size_mb as f64 / 1024.0)
            } else {
                format!("{}MB", size_mb)
            };
            PreflightCheck {
                name: "Database Size".to_string(),
                status: CheckStatus::Pass,
                detail,
            }
        }
        Err(e) => PreflightCheck {
            name: "Database Size".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

async fn check_lock_contention(client: &Client) -> PreflightCheck {
    match client
        .query_one("SELECT count(*)::int FROM pg_locks WHERE NOT granted", &[])
        .await
    {
        Ok(row) => {
            let blocked: i32 = row.get(0);
            if blocked > 0 {
                PreflightCheck {
                    name: "Lock Contention".to_string(),
                    status: CheckStatus::Warn,
                    detail: format!("{} blocked lock request(s)", blocked),
                }
            } else {
                PreflightCheck {
                    name: "Lock Contention".to_string(),
                    status: CheckStatus::Pass,
                    detail: "No blocked locks".to_string(),
                }
            }
        }
        Err(e) => PreflightCheck {
            name: "Lock Contention".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}
