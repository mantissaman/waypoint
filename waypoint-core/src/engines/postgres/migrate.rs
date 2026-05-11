//! PostgreSQL implementation of the `migrate` command.
//!
//! The dialect-aware dispatchers (and the engine-agnostic public types
//! [`MigrateReport`], [`MigrateDetail`]) live in [`crate::commands::migrate`].
//! This module owns everything that is PG-specific: prepared setup,
//! batch-transaction support, safety/guard/reversal integration, and the
//! per-statement apply path.

use std::collections::{HashMap, HashSet};

use tokio_postgres::Client;

use crate::commands::migrate::{
    should_run_in_environment, GuardAction, MigrateDetail, MigrateReport,
};
use crate::config::WaypointConfig;
use crate::db;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::hooks::{self, HookType, ResolvedHook};
use crate::migration::{scan_migrations, MigrationVersion, ResolvedMigration};
use crate::placeholder::{build_placeholders, replace_placeholders};

/// Common state prepared by `prepare_migrate()` for both run modes.
struct MigrateSetup<'a> {
    /// All resolved migration files on disk.
    resolved: Vec<ResolvedMigration>,
    /// All hooks (from disk + config).
    all_hooks: Vec<ResolvedHook>,
    /// Current database user.
    db_user: String,
    /// Current database name.
    db_name: String,
    /// Who to record as the installer.
    installed_by: String,
    /// Parsed target version, if specified.
    target: Option<MigrationVersion>,
    /// Baseline version from history, if any.
    baseline_version: Option<MigrationVersion>,
    /// Set of effectively-applied version strings (respects undo).
    effective_versions: HashSet<String>,
    /// Highest effectively-applied version.
    highest_applied: Option<MigrationVersion>,
    /// Map of repeatable script name -> applied checksum (for checksum comparison).
    applied_scripts: HashMap<String, Option<i32>>,
    /// Current environment from config.
    current_env: Option<&'a str>,
}

/// Perform all shared setup: history table creation, validation, preflight,
/// file scanning, hooks loading, version computation.
async fn prepare_migrate<'a>(
    client: &Client,
    config: &'a WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateSetup<'a>> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    history::create_history_table(client, schema, table).await?;

    if config.migrations.validate_on_migrate {
        if let Err(e) = crate::commands::validate::execute(client, config).await {
            match &e {
                WaypointError::ValidationFailed(_) => return Err(e),
                _ => {
                    log::debug!("Validation skipped: {}", e);
                }
            }
        }
    }

    if config.preflight.enabled {
        let preflight_report = crate::preflight::run_preflight(client, &config.preflight).await?;
        if !preflight_report.passed {
            let failed_checks: Vec<String> = preflight_report
                .checks
                .iter()
                .filter(|c| c.status == crate::preflight::CheckStatus::Fail)
                .map(|c| format!("{}: {}", c.name, c.detail))
                .collect();
            return Err(WaypointError::PreflightFailed {
                checks: failed_checks.join("; "),
            });
        }
    }

    let resolved = scan_migrations(&config.migrations.locations)?;

    let mut all_hooks: Vec<ResolvedHook> = hooks::scan_hooks(&config.migrations.locations)?;
    let config_hooks = hooks::load_config_hooks(&config.hooks)?;
    all_hooks.extend(config_hooks);

    let applied = history::get_applied_migrations(client, schema, table).await?;

    let db_user = db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or(&db_user)
        .to_string();

    let target = target_version.map(MigrationVersion::parse).transpose()?;

    let baseline_version = applied
        .iter()
        .find(|a| a.migration_type == "BASELINE")
        .and_then(|a| a.version.as_ref())
        .map(|v| MigrationVersion::parse(v))
        .transpose()?;

    let effective_versions = history::effective_applied_versions(&applied);

    let highest_applied = effective_versions
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .max();

    let applied_scripts: HashMap<String, Option<i32>> = applied
        .iter()
        .filter(|a| a.success && a.version.is_none())
        .map(|a| (a.script.clone(), a.checksum))
        .collect();

    let current_env = config.migrations.environment.as_deref();

    Ok(MigrateSetup {
        resolved,
        all_hooks,
        db_user,
        db_name,
        installed_by,
        target,
        baseline_version,
        effective_versions,
        highest_applied,
        applied_scripts,
        current_env,
    })
}

/// Filter resolved migrations down to pending versioned ones, applying
/// baseline/target/out-of-order checks.
fn filter_pending_versioned<'a>(
    versioned: &[&'a ResolvedMigration],
    setup: &MigrateSetup<'_>,
    config: &WaypointConfig,
) -> Result<Vec<&'a ResolvedMigration>> {
    let mut pending = Vec::new();
    for migration in versioned {
        let version = migration.version().unwrap();

        if setup.effective_versions.contains(&version.raw) {
            continue;
        }

        if let Some(ref bv) = setup.baseline_version {
            if version <= bv {
                log::debug!("Skipping {} (below baseline)", migration.script);
                continue;
            }
        }

        if let Some(ref tv) = setup.target {
            if version > tv {
                log::debug!("Skipping {} (above target {})", migration.script, tv);
                break;
            }
        }

        if !config.migrations.out_of_order {
            if let Some(ref highest) = setup.highest_applied {
                if version < highest {
                    return Err(WaypointError::OutOfOrder {
                        version: version.raw.clone(),
                        highest: highest.raw.clone(),
                    });
                }
            }
        }

        pending.push(*migration);
    }
    Ok(pending)
}

/// Filter resolved migrations down to pending repeatable ones (checksum changed or new).
fn filter_pending_repeatables<'a>(
    repeatables: &[&'a ResolvedMigration],
    setup: &MigrateSetup<'_>,
) -> Vec<&'a ResolvedMigration> {
    let mut pending = Vec::new();
    for migration in repeatables {
        if let Some(&applied_checksum) = setup.applied_scripts.get(&migration.script) {
            if applied_checksum == Some(migration.checksum) {
                continue;
            }
        }
        pending.push(*migration);
    }
    pending
}

/// Evaluate all `-- waypoint:require` guard preconditions for a migration.
async fn evaluate_require_guards(
    client: &Client,
    schema: &str,
    migration: &ResolvedMigration,
    config: &WaypointConfig,
) -> Result<GuardAction> {
    if migration.directives.require.is_empty() {
        return Ok(GuardAction::Continue);
    }

    for expr_str in &migration.directives.require {
        match crate::guard::parse(expr_str) {
            Ok(expr) => match crate::guard::evaluate(client, schema, &expr).await {
                Ok(true) => {}
                Ok(false) => match config.guards.on_require_fail {
                    crate::guard::OnRequireFail::Skip => {
                        log::info!(
                            "Guard require failed, skipping migration; script={}, expr={}",
                            migration.script,
                            expr_str
                        );
                        return Ok(GuardAction::Skip);
                    }
                    crate::guard::OnRequireFail::Warn => log::warn!(
                        "Guard require failed (continuing); script={}, expr={}",
                        migration.script,
                        expr_str
                    ),
                    crate::guard::OnRequireFail::Error => {
                        return Ok(GuardAction::Error(WaypointError::GuardFailed {
                            kind: "require".to_string(),
                            script: migration.script.clone(),
                            expression: expr_str.clone(),
                        }));
                    }
                },
                Err(e) => {
                    log::warn!(
                        "Guard evaluation error; script={}, expr={}, error={}",
                        migration.script,
                        expr_str,
                        e
                    );
                    return Ok(GuardAction::Error(WaypointError::GuardFailed {
                        kind: "require".to_string(),
                        script: migration.script.clone(),
                        expression: format!("{} (evaluation error: {})", expr_str, e),
                    }));
                }
            },
            Err(e) => {
                return Ok(GuardAction::Error(WaypointError::GuardFailed {
                    kind: "require".to_string(),
                    script: migration.script.clone(),
                    expression: format!("{} (parse error: {})", expr_str, e),
                }));
            }
        }
    }
    Ok(GuardAction::Continue)
}

/// Evaluate all `-- waypoint:ensure` guard postconditions for a migration.
async fn evaluate_ensure_guards(
    client: &Client,
    schema: &str,
    migration: &ResolvedMigration,
) -> Result<()> {
    for expr_str in &migration.directives.ensure {
        match crate::guard::parse(expr_str) {
            Ok(expr) => match crate::guard::evaluate(client, schema, &expr).await {
                Ok(true) => {}
                Ok(false) => {
                    return Err(WaypointError::GuardFailed {
                        kind: "ensure".to_string(),
                        script: migration.script.clone(),
                        expression: expr_str.clone(),
                    });
                }
                Err(e) => {
                    return Err(WaypointError::GuardFailed {
                        kind: "ensure".to_string(),
                        script: migration.script.clone(),
                        expression: format!("{} (evaluation error: {})", expr_str, e),
                    });
                }
            },
            Err(e) => {
                return Err(WaypointError::GuardFailed {
                    kind: "ensure".to_string(),
                    script: migration.script.clone(),
                    expression: format!("{} (parse error: {})", expr_str, e),
                });
            }
        }
    }
    Ok(())
}

/// Execute the migrate command.
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    execute_with_options(client, config, target_version, false).await
}

/// Execute the migrate command with additional options.
pub async fn execute_with_options(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force: bool,
) -> Result<MigrateReport> {
    let table = &config.migrations.table;

    db::acquire_advisory_lock(client, table).await?;

    let result = if config.migrations.batch_transaction {
        run_batch_migrate(client, config, target_version, force).await
    } else {
        run_migrate(client, config, target_version, force).await
    };

    if let Err(e) = db::release_advisory_lock(client, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => log::info!(
            "Migrate completed; migrations_applied={}, total_time_ms={}, hooks_executed={}",
            report.migrations_applied,
            report.total_time_ms,
            report.hooks_executed
        ),
        Err(e) => log::error!("Migrate failed: {}", e),
    }

    result
}

async fn run_migrate(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force_override: bool,
) -> Result<MigrateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    let setup = prepare_migrate(client, config, target_version).await?;

    let mut report = MigrateReport {
        migrations_applied: 0,
        total_time_ms: 0,
        details: Vec::new(),
        hooks_executed: 0,
        hooks_time_ms: 0,
    };

    let before_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "beforeMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::BeforeMigrate,
        &before_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    let versioned: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter(|m| should_run_in_environment(&m.directives, setup.current_env))
        .collect();

    let pending_versioned = filter_pending_versioned(&versioned, &setup, config)?;

    for migration in &pending_versioned {
        let version = migration.version().unwrap();

        let each_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &setup.db_user,
            &setup.db_name,
            &migration.script,
        );
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::BeforeEachMigrate,
            &each_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        if config.safety.enabled {
            let safety_report = crate::safety::analyze_migration(
                client,
                schema,
                &migration.sql,
                &migration.script,
                &config.safety,
            )
            .await?;
            if safety_report.overall_verdict == crate::safety::SafetyVerdict::Danger
                && config.safety.block_on_danger
                && !migration.directives.safety_override
                && !force_override
            {
                return Err(WaypointError::MigrationBlocked {
                    script: migration.script.clone(),
                    reason: safety_report.suggestions.join("; "),
                });
            }
        }

        match evaluate_require_guards(client, schema, migration, config).await? {
            GuardAction::Continue => {}
            GuardAction::Skip => continue,
            GuardAction::Error(e) => return Err(e),
        }

        let before_snapshot = if config.reversals.enabled && migration.is_versioned() {
            Some(crate::reversal::capture_before(client, schema).await?)
        } else {
            None
        };

        let has_ensure_guards = !migration.directives.ensure.is_empty();
        let exec_time = apply_migration(
            client,
            config,
            migration,
            schema,
            table,
            &setup.installed_by,
            &setup.db_user,
            &setup.db_name,
            has_ensure_guards,
        )
        .await?;

        if has_ensure_guards {
            if let Err(guard_err) = evaluate_ensure_guards(client, schema, migration).await {
                if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                    log::error!(
                        "Failed to rollback after ensure guard failure: {}",
                        rollback_err
                    );
                }
                return Err(guard_err);
            }
            client.batch_execute("COMMIT").await?;
        }

        if let Some(ref before) = before_snapshot {
            if let Some(ver) = migration.version() {
                match crate::reversal::generate_reversal(
                    client,
                    schema,
                    before,
                    config.reversals.warn_data_loss,
                )
                .await
                {
                    Ok(result) => {
                        if let Some(ref reversal_sql) = result.reversal_sql {
                            if let Err(e) = crate::reversal::store_reversal(
                                client,
                                schema,
                                table,
                                &ver.raw,
                                reversal_sql,
                            )
                            .await
                            {
                                log::warn!(
                                    "Failed to store reversal SQL; version={}, error={}",
                                    ver.raw,
                                    e
                                );
                            }
                        }
                        for warning in &result.warnings {
                            log::warn!("Reversal warning for {}: {}", migration.script, warning);
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to generate reversal; script={}, error={}",
                            migration.script,
                            e
                        );
                    }
                }
            }
        }

        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::AfterEachMigrate,
            &each_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        report.migrations_applied += 1;
        report.total_time_ms += exec_time;
        report.details.push(MigrateDetail {
            version: Some(version.raw.clone()),
            description: migration.description.clone(),
            script: migration.script.clone(),
            execution_time_ms: exec_time,
        });
    }

    let repeatables: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| !m.is_versioned() && !m.is_undo())
        .filter(|m| should_run_in_environment(&m.directives, setup.current_env))
        .collect();

    for migration in &repeatables {
        if let Some(&applied_checksum) = setup.applied_scripts.get(&migration.script) {
            if applied_checksum == Some(migration.checksum) {
                continue;
            }
            log::info!(
                "Re-applying changed repeatable migration; migration={}",
                migration.script
            );
        }

        let each_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &setup.db_user,
            &setup.db_name,
            &migration.script,
        );
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::BeforeEachMigrate,
            &each_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        let exec_time = apply_migration(
            client,
            config,
            migration,
            schema,
            table,
            &setup.installed_by,
            &setup.db_user,
            &setup.db_name,
            false,
        )
        .await?;

        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::AfterEachMigrate,
            &each_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        report.migrations_applied += 1;
        report.total_time_ms += exec_time;
        report.details.push(MigrateDetail {
            version: None,
            description: migration.description.clone(),
            script: migration.script.clone(),
            execution_time_ms: exec_time,
        });
    }

    let after_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "afterMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::AfterMigrate,
        &after_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    Ok(report)
}

/// Pre-compiled regexes for batch-compatibility checks.
mod batch_regexes {
    use std::sync::LazyLock;
    pub static DROP_INDEX_CONCURRENT: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)DROP\s+INDEX\s+CONCURRENTLY").unwrap());
    pub static CREATE_DATABASE: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bCREATE DATABASE\b").unwrap());
    pub static DROP_DATABASE: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bDROP DATABASE\b").unwrap());
    pub static VACUUM: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bVACUUM\b").unwrap());
    pub static CLUSTER: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bCLUSTER\b").unwrap());
    pub static REINDEX_CONCURRENT: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bREINDEX\b.*\bCONCURRENTLY\b").unwrap());
}

/// Check that a migration's SQL does not contain statements that cannot run inside a transaction.
fn validate_batch_compatible(script: &str, sql: &str) -> Result<()> {
    let upper = sql.to_uppercase();

    if upper.contains("CONCURRENTLY") {
        let ops = crate::sql_parser::extract_ddl_operations(sql);
        for op in &ops {
            if let crate::sql_parser::DdlOperation::CreateIndex {
                is_concurrent: true,
                ..
            } = op
            {
                return Err(WaypointError::NonTransactionalStatement {
                    script: script.to_string(),
                    statement: op.to_string(),
                });
            }
        }
        if batch_regexes::DROP_INDEX_CONCURRENT.is_match(sql) {
            return Err(WaypointError::NonTransactionalStatement {
                script: script.to_string(),
                statement: "DROP INDEX CONCURRENTLY".to_string(),
            });
        }
    }

    if upper.contains("CREATE DATABASE") && batch_regexes::CREATE_DATABASE.is_match(sql) {
        return Err(WaypointError::NonTransactionalStatement {
            script: script.to_string(),
            statement: "CREATE DATABASE".to_string(),
        });
    }
    if upper.contains("DROP DATABASE") && batch_regexes::DROP_DATABASE.is_match(sql) {
        return Err(WaypointError::NonTransactionalStatement {
            script: script.to_string(),
            statement: "DROP DATABASE".to_string(),
        });
    }

    let checks: &[(&regex_lite::Regex, &str, &str)] = &[
        (&batch_regexes::VACUUM, "VACUUM", "VACUUM"),
        (&batch_regexes::CLUSTER, "CLUSTER", "CLUSTER"),
        (
            &batch_regexes::REINDEX_CONCURRENT,
            "REINDEX",
            "REINDEX CONCURRENTLY",
        ),
    ];
    for &(re, fast_check, desc) in checks {
        if upper.contains(fast_check) && re.is_match(sql) {
            return Err(WaypointError::NonTransactionalStatement {
                script: script.to_string(),
                statement: desc.to_string(),
            });
        }
    }

    Ok(())
}

/// Run all pending migrations in a single transaction (all-or-nothing batch mode).
async fn run_batch_migrate(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force_override: bool,
) -> Result<MigrateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    let setup = prepare_migrate(client, config, target_version).await?;

    let current_env = setup.current_env;

    let versioned: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter(|m| should_run_in_environment(&m.directives, current_env))
        .collect();

    let mut pending_versioned = filter_pending_versioned(&versioned, &setup, config)?;

    let repeatables: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| !m.is_versioned() && !m.is_undo())
        .filter(|m| should_run_in_environment(&m.directives, current_env))
        .collect();
    let pending_repeatables = filter_pending_repeatables(&repeatables, &setup);

    let placeholders_map = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "batch_validate",
    );
    for migration in pending_versioned.iter().chain(pending_repeatables.iter()) {
        let sql = replace_placeholders(&migration.sql, &placeholders_map)?;
        validate_batch_compatible(&migration.script, &sql)?;
    }

    if config.safety.enabled {
        for migration in &pending_versioned {
            let safety_report = crate::safety::analyze_migration(
                client,
                schema,
                &migration.sql,
                &migration.script,
                &config.safety,
            )
            .await?;
            if safety_report.overall_verdict == crate::safety::SafetyVerdict::Danger
                && config.safety.block_on_danger
                && !migration.directives.safety_override
                && !force_override
            {
                return Err(WaypointError::MigrationBlocked {
                    script: migration.script.clone(),
                    reason: safety_report.suggestions.join("; "),
                });
            }
        }
    }

    let mut skipped_scripts: HashSet<&str> = HashSet::new();
    for migration in &pending_versioned {
        match evaluate_require_guards(client, schema, migration, config).await? {
            GuardAction::Continue => {}
            GuardAction::Skip => {
                skipped_scripts.insert(&migration.script);
            }
            GuardAction::Error(e) => return Err(e),
        }
    }
    pending_versioned.retain(|m| !skipped_scripts.contains(m.script.as_str()));

    let mut report = MigrateReport {
        migrations_applied: 0,
        total_time_ms: 0,
        details: Vec::new(),
        hooks_executed: 0,
        hooks_time_ms: 0,
    };

    let before_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "beforeMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::BeforeMigrate,
        &before_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    if pending_versioned.is_empty() && pending_repeatables.is_empty() {
        let after_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &setup.db_user,
            &setup.db_name,
            "afterMigrate",
        );
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::AfterMigrate,
            &after_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;
        return Ok(report);
    }

    let before_snapshot = if config.reversals.enabled {
        match crate::reversal::capture_before(client, schema).await {
            Ok(snap) => Some(snap),
            Err(e) => {
                log::warn!(
                    "Failed to capture before-snapshot for batch reversal: {}",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    let batch_start = std::time::Instant::now();
    client.batch_execute("BEGIN").await?;

    let installed_by = &setup.installed_by;
    let batch_result = async {
        for migration in &pending_versioned {
            let version = migration.version().unwrap();
            let each_placeholders = build_placeholders(
                &config.placeholders,
                schema,
                &setup.db_user,
                &setup.db_name,
                &migration.script,
            );

            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::BeforeEachMigrate,
                &each_placeholders,
            )
            .await?;
            report.hooks_executed += count;
            report.hooks_time_ms += ms;

            let sql = replace_placeholders(&migration.sql, &each_placeholders)?;
            let start = std::time::Instant::now();
            client
                .batch_execute(&sql)
                .await
                .map_err(|e| WaypointError::MigrationFailed {
                    script: migration.script.clone(),
                    reason: crate::error::format_db_error(&e),
                })?;
            let exec_time = start.elapsed().as_millis() as i32;

            let version_str = Some(version.raw.as_str());
            let type_str = migration.migration_type().to_string();
            history::insert_applied_migration(
                client,
                schema,
                table,
                version_str,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                exec_time,
                true,
            )
            .await?;

            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::AfterEachMigrate,
                &each_placeholders,
            )
            .await?;
            report.hooks_executed += count;
            report.hooks_time_ms += ms;

            report.migrations_applied += 1;
            report.total_time_ms += exec_time;
            report.details.push(MigrateDetail {
                version: Some(version.raw.clone()),
                description: migration.description.clone(),
                script: migration.script.clone(),
                execution_time_ms: exec_time,
            });
        }

        for migration in &pending_repeatables {
            let each_placeholders = build_placeholders(
                &config.placeholders,
                schema,
                &setup.db_user,
                &setup.db_name,
                &migration.script,
            );

            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::BeforeEachMigrate,
                &each_placeholders,
            )
            .await?;
            report.hooks_executed += count;
            report.hooks_time_ms += ms;

            let sql = replace_placeholders(&migration.sql, &each_placeholders)?;
            let start = std::time::Instant::now();
            client
                .batch_execute(&sql)
                .await
                .map_err(|e| WaypointError::MigrationFailed {
                    script: migration.script.clone(),
                    reason: crate::error::format_db_error(&e),
                })?;
            let exec_time = start.elapsed().as_millis() as i32;

            let type_str = migration.migration_type().to_string();
            history::insert_applied_migration(
                client,
                schema,
                table,
                None,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                exec_time,
                true,
            )
            .await?;

            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::AfterEachMigrate,
                &each_placeholders,
            )
            .await?;
            report.hooks_executed += count;
            report.hooks_time_ms += ms;

            report.migrations_applied += 1;
            report.total_time_ms += exec_time;
            report.details.push(MigrateDetail {
                version: None,
                description: migration.description.clone(),
                script: migration.script.clone(),
                execution_time_ms: exec_time,
            });
        }

        Ok::<(), WaypointError>(())
    }
    .await;

    match batch_result {
        Ok(()) => {
            client.batch_execute("COMMIT").await?;
            report.total_time_ms = batch_start.elapsed().as_millis() as i32;

            if let Some(ref before) = before_snapshot {
                for migration in &pending_versioned {
                    if let Some(ver) = migration.version() {
                        match crate::reversal::generate_reversal(
                            client,
                            schema,
                            before,
                            config.reversals.warn_data_loss,
                        )
                        .await
                        {
                            Ok(result) => {
                                if let Some(ref reversal_sql) = result.reversal_sql {
                                    if let Err(e) = crate::reversal::store_reversal(
                                        client,
                                        schema,
                                        table,
                                        &ver.raw,
                                        reversal_sql,
                                    )
                                    .await
                                    {
                                        log::warn!(
                                            "Failed to store reversal SQL; version={}, error={}",
                                            ver.raw,
                                            e
                                        );
                                    }
                                }
                                for warning in &result.warnings {
                                    log::warn!(
                                        "Reversal warning for {}: {}",
                                        migration.script,
                                        warning
                                    );
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to generate reversal; script={}, error={}",
                                    migration.script,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                log::error!("Failed to rollback batch transaction: {}", rollback_err);
            }
            log::error!("Batch migration failed, all changes rolled back: {}", e);
            return Err(e);
        }
    }

    let after_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "afterMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::AfterMigrate,
        &after_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    Ok(report)
}

/// Apply a single migration within a transaction.
#[allow(clippy::too_many_arguments)]
async fn apply_migration(
    client: &Client,
    config: &WaypointConfig,
    migration: &ResolvedMigration,
    schema: &str,
    table: &str,
    installed_by: &str,
    db_user: &str,
    db_name: &str,
    hold_transaction: bool,
) -> Result<i32> {
    log::info!(
        "Applying migration; migration={}, schema={}",
        migration.script,
        schema
    );

    let placeholders = build_placeholders(
        &config.placeholders,
        schema,
        db_user,
        db_name,
        &migration.script,
    );

    let sql = replace_placeholders(&migration.sql, &placeholders)?;

    let version_str = migration.version().map(|v| v.raw.as_str());
    let type_str = migration.migration_type().to_string();

    let start = std::time::Instant::now();
    client.batch_execute("BEGIN").await?;

    match client.batch_execute(&sql).await {
        Ok(()) => {
            let exec_time = start.elapsed().as_millis() as i32;
            match history::insert_applied_migration(
                client,
                schema,
                table,
                version_str,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                exec_time,
                true,
            )
            .await
            {
                Ok(()) => {
                    if !hold_transaction {
                        client.batch_execute("COMMIT").await?;
                    }
                    Ok(exec_time)
                }
                Err(e) => {
                    if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                        log::error!("Failed to rollback transaction: {}", rollback_err);
                    }
                    Err(e)
                }
            }
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                log::error!("Failed to rollback transaction: {}", rollback_err);
            }

            if let Err(record_err) = history::insert_applied_migration(
                client,
                schema,
                table,
                version_str,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                0,
                false,
            )
            .await
            {
                log::warn!(
                    "Failed to record migration failure in history table; script={}, error={}",
                    migration.script,
                    record_err
                );
            }

            let reason = crate::error::format_db_error(&e);
            log::error!(
                "Migration failed; script={}, reason={}",
                migration.script,
                reason
            );
            Err(WaypointError::MigrationFailed {
                script: migration.script.clone(),
                reason,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_concurrent_index() {
        let sql = "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);";
        let result = validate_batch_compatible("V5__Add_index.sql", sql);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            WaypointError::NonTransactionalStatement { script, .. } => {
                assert_eq!(script, "V5__Add_index.sql");
            }
            _ => panic!("Expected NonTransactionalStatement, got {:?}", err),
        }
    }

    #[test]
    fn test_detect_drop_index_concurrently() {
        let sql = "DROP INDEX CONCURRENTLY idx_users_email;";
        let result = validate_batch_compatible("V6__Drop_index.sql", sql);
        assert!(result.is_err());
        match result.unwrap_err() {
            WaypointError::NonTransactionalStatement { statement, .. } => {
                assert!(statement.contains("DROP INDEX CONCURRENTLY"));
            }
            other => panic!("Expected NonTransactionalStatement, got {:?}", other),
        }
    }

    #[test]
    fn test_detect_vacuum() {
        let sql = "VACUUM ANALYZE users;";
        let result = validate_batch_compatible("V7__Vacuum.sql", sql);
        assert!(result.is_err());
        match result.unwrap_err() {
            WaypointError::NonTransactionalStatement { statement, .. } => {
                assert_eq!(statement, "VACUUM");
            }
            other => panic!("Expected NonTransactionalStatement, got {:?}", other),
        }
    }

    #[test]
    fn test_detect_create_database() {
        let sql = "CREATE DATABASE newdb;";
        let result = validate_batch_compatible("V8__Create_db.sql", sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_compatible_normal_ddl() {
        let sql =
            "CREATE TABLE users (id SERIAL PRIMARY KEY); CREATE INDEX idx_users ON users (id);";
        let result = validate_batch_compatible("V1__Init.sql", sql);
        assert!(result.is_ok());
    }
}
