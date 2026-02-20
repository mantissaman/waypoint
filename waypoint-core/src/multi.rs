//! Multi-database orchestration.
//!
//! Allows managing migrations across multiple named databases
//! with dependency ordering between them.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::{DatabaseConfig, HooksConfig, MigrationSettings, WaypointConfig};
use crate::error::{Result, WaypointError};

/// Configuration for a single named database within a multi-db setup.
#[derive(Debug, Clone)]
pub struct NamedDatabaseConfig {
    /// Unique logical name identifying this database.
    pub name: String,
    /// Database connection configuration.
    pub database: DatabaseConfig,
    /// Migration settings for this database.
    pub migrations: MigrationSettings,
    /// Hook configuration for this database.
    pub hooks: HooksConfig,
    /// Placeholder key-value pairs for SQL template substitution.
    pub placeholders: HashMap<String, String>,
    /// Names of other databases that must be migrated before this one.
    pub depends_on: Vec<String>,
}

impl NamedDatabaseConfig {
    /// Convert to a standalone WaypointConfig for running commands.
    pub fn to_waypoint_config(&self) -> WaypointConfig {
        WaypointConfig {
            database: self.database.clone(),
            migrations: self.migrations.clone(),
            hooks: self.hooks.clone(),
            placeholders: self.placeholders.clone(),
            ..WaypointConfig::default()
        }
    }
}

/// Multi-database orchestration entry point.
pub struct MultiWaypoint {
    /// List of all database configurations to orchestrate.
    pub databases: Vec<NamedDatabaseConfig>,
}

/// Result from a multi-db operation on a single database.
#[derive(Debug, Serialize)]
pub struct DatabaseResult {
    /// Logical name of the database.
    pub name: String,
    /// Whether the operation succeeded on this database.
    pub success: bool,
    /// Human-readable summary of the operation result.
    pub message: String,
}

/// Aggregate result from a multi-db operation.
#[derive(Debug, Serialize)]
pub struct MultiResult {
    /// Per-database operation results.
    pub results: Vec<DatabaseResult>,
    /// Whether every database operation succeeded.
    pub all_succeeded: bool,
}

impl MultiWaypoint {
    /// Determine execution order based on depends_on relationships (Kahn's algorithm).
    pub fn execution_order(databases: &[NamedDatabaseConfig]) -> Result<Vec<String>> {
        let all_names: HashSet<String> = databases.iter().map(|d| d.name.clone()).collect();

        // Build in-degree map
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut reverse_edges: HashMap<String, Vec<String>> = HashMap::new();

        for db in databases {
            in_degree.entry(db.name.clone()).or_insert(0);
            for dep in &db.depends_on {
                if !all_names.contains(dep) {
                    return Err(WaypointError::DatabaseNotFound {
                        name: dep.clone(),
                        available: all_names.iter().cloned().collect::<Vec<_>>().join(", "),
                    });
                }
                *in_degree.entry(db.name.clone()).or_insert(0) += 1;
                reverse_edges
                    .entry(dep.clone())
                    .or_default()
                    .push(db.name.clone());
            }
        }

        let mut queue: VecDeque<String> = VecDeque::new();
        for (name, deg) in &in_degree {
            if *deg == 0 {
                queue.push_back(name.clone());
            }
        }

        let mut sorted = Vec::new();
        while let Some(name) = queue.pop_front() {
            sorted.push(name.clone());
            if let Some(dependents) = reverse_edges.get(&name) {
                for dep in dependents {
                    let deg = in_degree.get_mut(dep).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }

        if sorted.len() != databases.len() {
            let in_cycle: Vec<String> = in_degree
                .iter()
                .filter(|(_, deg)| **deg > 0)
                .map(|(name, _)| name.clone())
                .collect();
            return Err(WaypointError::MultiDbDependencyCycle {
                path: in_cycle.join(" -> "),
            });
        }

        Ok(sorted)
    }

    /// Connect to all databases (or a filtered subset).
    pub async fn connect(
        databases: &[NamedDatabaseConfig],
        filter: Option<&str>,
    ) -> Result<HashMap<String, Client>> {
        let mut clients = HashMap::new();

        for db in databases {
            if let Some(name_filter) = filter {
                if db.name != name_filter {
                    continue;
                }
            }

            let config = db.to_waypoint_config();
            let conn_string = config.connection_string()?;
            let client = crate::db::connect_with_config(
                &conn_string,
                &config.database.ssl_mode,
                config.database.connect_retries,
                config.database.connect_timeout_secs,
                config.database.statement_timeout_secs,
            )
            .await?;
            clients.insert(db.name.clone(), client);
        }

        if let Some(name_filter) = filter {
            if !clients.contains_key(name_filter) {
                let available = databases
                    .iter()
                    .map(|d| d.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(WaypointError::DatabaseNotFound {
                    name: name_filter.to_string(),
                    available,
                });
            }
        }

        Ok(clients)
    }

    /// Run migrate on all databases in dependency order.
    pub async fn migrate(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, Client>,
        order: &[String],
        target_version: Option<&str>,
        fail_fast: bool,
    ) -> Result<MultiResult> {
        let mut results = Vec::new();

        for name in order {
            let db = databases.iter().find(|d| &d.name == name);
            let client = clients.get(name);

            match (db, client) {
                (Some(db), Some(client)) => {
                    let config = db.to_waypoint_config();
                    match crate::commands::migrate::execute(client, &config, target_version).await {
                        Ok(report) => {
                            results.push(DatabaseResult {
                                name: name.clone(),
                                success: true,
                                message: format!(
                                    "Applied {} migration(s) ({}ms)",
                                    report.migrations_applied, report.total_time_ms
                                ),
                            });
                        }
                        Err(e) => {
                            results.push(DatabaseResult {
                                name: name.clone(),
                                success: false,
                                message: format!("{}", e),
                            });
                            if fail_fast {
                                break;
                            }
                        }
                    }
                }
                _ => {
                    results.push(DatabaseResult {
                        name: name.clone(),
                        success: false,
                        message: "Database not connected".to_string(),
                    });
                    if fail_fast {
                        break;
                    }
                }
            }
        }

        let all_succeeded = results.iter().all(|r| r.success);
        Ok(MultiResult {
            results,
            all_succeeded,
        })
    }

    /// Run info on all databases in dependency order.
    pub async fn info(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, Client>,
        order: &[String],
    ) -> Result<HashMap<String, Vec<crate::commands::info::MigrationInfo>>> {
        let mut all_info = HashMap::new();

        for name in order {
            let db = databases.iter().find(|d| &d.name == name);
            let client = clients.get(name);

            if let (Some(db), Some(client)) = (db, client) {
                let config = db.to_waypoint_config();
                let info = crate::commands::info::execute(client, &config).await?;
                all_info.insert(name.clone(), info);
            }
        }

        Ok(all_info)
    }
}
