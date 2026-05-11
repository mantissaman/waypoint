//! MySQL implementation of the `migrate` command.
//!
//! The dialect-aware dispatchers (and engine-agnostic public types
//! [`MigrateReport`], [`MigrateDetail`]) live in [`crate::commands::migrate`].
//! This module owns the MySQL-specific apply loop, guard-evaluator variants,
//! and hook firing.
//!
//! MySQL DDL is non-transactional, so `--transaction` batch mode is not
//! supported here. `ensure` guards become verify-after rather than
//! rollback-if-false — the documented caveat.

use std::collections::HashMap;

use crate::commands::migrate::{
    should_run_in_environment, GuardAction, MigrateDetail, MigrateReport,
};
use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::hooks::{self, HookType, ResolvedHook};
use crate::migration::{scan_migrations, MigrationVersion, ResolvedMigration};
use crate::placeholder::{build_placeholders, replace_placeholders};

/// Dialect-aware `require` guard evaluator. Mirrors the PG version but uses
/// `guard::evaluate_db` so the underlying SQL is dispatched per engine.
async fn evaluate_require_guards_db(
    client: &DbClient,
    schema: &str,
    migration: &ResolvedMigration,
    config: &WaypointConfig,
) -> Result<GuardAction> {
    if migration.directives.require.is_empty() {
        return Ok(GuardAction::Continue);
    }
    for expr_str in &migration.directives.require {
        match crate::guard::parse(expr_str) {
            Ok(expr) => match crate::guard::evaluate_db(client, schema, &expr).await {
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

/// Dialect-aware `ensure` guard evaluator.
async fn evaluate_ensure_guards_db(
    client: &DbClient,
    schema: &str,
    migration: &ResolvedMigration,
) -> Result<()> {
    for expr_str in &migration.directives.ensure {
        match crate::guard::parse(expr_str) {
            Ok(expr) => match crate::guard::evaluate_db(client, schema, &expr).await {
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

/// Execute the migrate command (MySQL).
pub async fn execute(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    execute_with_options(client, config, target_version, false).await
}

/// Execute the migrate command with options (MySQL).
pub async fn execute_with_options(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
    _force: bool,
) -> Result<MigrateReport> {
    if config.migrations.batch_transaction && !client.dialect().supports_transactional_ddl() {
        return Err(WaypointError::ConfigError(format!(
            "batch_transaction is not supported on {} — DDL is not transactional on this engine. \
             Remove `batch_transaction = true` or `--transaction` to proceed.",
            client.dialect_kind().name()
        )));
    }

    let table = &config.migrations.table;

    client.acquire_lock(table).await?;

    let result = run_migrate(client, config, target_version).await;

    if let Err(e) = client.release_lock(table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => {
            log::info!(
                "Migrate completed (mysql); migrations_applied={}, total_time_ms={}",
                report.migrations_applied,
                report.total_time_ms
            );
        }
        Err(e) => {
            log::error!("Migrate failed (mysql): {}", e);
        }
    }

    result
}

async fn run_migrate(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    history::create_history_table_db(client, &schema, table).await?;

    if config.migrations.validate_on_migrate {
        if let Err(e) = crate::commands::validate::execute_db(client, config).await {
            match &e {
                WaypointError::ValidationFailed(_) => return Err(e),
                _ => log::debug!("Validation skipped: {}", e),
            }
        }
    }

    if config.preflight.enabled {
        let report = crate::preflight::run_preflight_db(client, &config.preflight).await?;
        if !report.passed {
            let failed_checks: Vec<String> = report
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
    let applied = history::get_applied_migrations_db(client, &schema, table).await?;

    let mut all_hooks: Vec<ResolvedHook> = hooks::scan_hooks(&config.migrations.locations)?;
    let config_hooks = hooks::load_config_hooks(&config.hooks)?;
    all_hooks.extend(config_hooks);

    let db_user = client
        .current_user()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let db_name = client
        .current_database()
        .await
        .unwrap_or_else(|_| "unknown".into());
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

    let pending_versioned: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| {
            if m.is_undo() {
                return false;
            }
            let v = match m.version() {
                Some(v) => v,
                None => return false,
            };
            if !m.is_versioned() {
                return false;
            }
            if effective_versions.contains(&v.raw) {
                return false;
            }
            if let Some(ref bl) = baseline_version {
                if v <= bl {
                    return false;
                }
            }
            if let Some(ref t) = target {
                if v > t {
                    return false;
                }
            }
            if !config.migrations.out_of_order {
                if let Some(ref hi) = highest_applied {
                    if v < hi {
                        return false;
                    }
                }
            }
            if !should_run_in_environment(&m.directives, current_env) {
                return false;
            }
            true
        })
        .collect();

    let pending_repeatables: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| {
            if m.version().is_some() || m.is_undo() {
                return false;
            }
            if !should_run_in_environment(&m.directives, current_env) {
                return false;
            }
            match applied_scripts.get(&m.script) {
                None => true,
                Some(prev) => prev != &Some(m.checksum),
            }
        })
        .collect();

    let mut report = MigrateReport {
        migrations_applied: 0,
        total_time_ms: 0,
        details: Vec::new(),
        hooks_executed: 0,
        hooks_time_ms: 0,
    };

    let mut sorted_versioned = pending_versioned.clone();
    sorted_versioned.sort_by(|a, b| a.version().unwrap().cmp(b.version().unwrap()));

    let has_pending = !sorted_versioned.is_empty() || !pending_repeatables.is_empty();

    if has_pending {
        let placeholders = build_placeholders(
            &config.placeholders,
            &schema,
            &db_user,
            &db_name,
            "beforeMigrate",
        );
        fire_hooks(
            client,
            &all_hooks,
            &HookType::BeforeMigrate,
            &placeholders,
            &mut report,
        )
        .await?;
    }

    for m in sorted_versioned {
        let placeholders =
            build_placeholders(&config.placeholders, &schema, &db_user, &db_name, &m.script);

        match evaluate_require_guards_db(client, &schema, m, config).await? {
            GuardAction::Continue => {}
            GuardAction::Skip => continue,
            GuardAction::Error(e) => return Err(e),
        }

        fire_hooks(
            client,
            &all_hooks,
            &HookType::BeforeEachMigrate,
            &placeholders,
            &mut report,
        )
        .await?;

        let before_snapshot = if config.reversals.enabled && m.is_versioned() {
            Some(crate::reversal::capture_before_db(client, &schema).await?)
        } else {
            None
        };

        let elapsed = apply_one(client, m, &schema, table, &installed_by, &placeholders).await?;
        report.migrations_applied += 1;
        report.total_time_ms += elapsed;
        report.details.push(MigrateDetail {
            version: m.version().map(|v| v.raw.clone()),
            description: m.description.clone(),
            script: m.script.clone(),
            execution_time_ms: elapsed,
        });

        // ensure guards run AFTER the migration. On MySQL DDL has already
        // auto-committed, so an ensure-failure does NOT roll back the
        // migration — it surfaces as a hard error and leaves the schema in
        // the post-migration state. This is the documented MySQL caveat.
        evaluate_ensure_guards_db(client, &schema, m).await?;

        if let (Some(before), Some(ver)) = (before_snapshot.as_ref(), m.version()) {
            match crate::reversal::generate_reversal_db(
                client,
                &schema,
                before,
                config.reversals.warn_data_loss,
            )
            .await
            {
                Ok(result) => {
                    if let Some(ref reversal_sql) = result.reversal_sql {
                        if let Err(e) = crate::reversal::store_reversal_db(
                            client,
                            &schema,
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
                        for w in &result.warnings {
                            log::warn!("Reversal warning for version {}: {}", ver.raw, w);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Failed to generate reversal for version {}: {}", ver.raw, e);
                }
            }
        }

        fire_hooks(
            client,
            &all_hooks,
            &HookType::AfterEachMigrate,
            &placeholders,
            &mut report,
        )
        .await?;
    }

    for m in pending_repeatables {
        let placeholders =
            build_placeholders(&config.placeholders, &schema, &db_user, &db_name, &m.script);

        match evaluate_require_guards_db(client, &schema, m, config).await? {
            GuardAction::Continue => {}
            GuardAction::Skip => continue,
            GuardAction::Error(e) => return Err(e),
        }

        fire_hooks(
            client,
            &all_hooks,
            &HookType::BeforeEachMigrate,
            &placeholders,
            &mut report,
        )
        .await?;

        let elapsed = apply_one(client, m, &schema, table, &installed_by, &placeholders).await?;
        report.migrations_applied += 1;
        report.total_time_ms += elapsed;
        report.details.push(MigrateDetail {
            version: None,
            description: m.description.clone(),
            script: m.script.clone(),
            execution_time_ms: elapsed,
        });

        evaluate_ensure_guards_db(client, &schema, m).await?;

        fire_hooks(
            client,
            &all_hooks,
            &HookType::AfterEachMigrate,
            &placeholders,
            &mut report,
        )
        .await?;
    }

    if has_pending {
        let placeholders = build_placeholders(
            &config.placeholders,
            &schema,
            &db_user,
            &db_name,
            "afterMigrate",
        );
        fire_hooks(
            client,
            &all_hooks,
            &HookType::AfterMigrate,
            &placeholders,
            &mut report,
        )
        .await?;
    }

    Ok(report)
}

/// Run all hooks of `phase` and fold the result into `report`.
async fn fire_hooks(
    client: &DbClient,
    all_hooks: &[ResolvedHook],
    phase: &HookType,
    placeholders: &HashMap<String, String>,
    report: &mut MigrateReport,
) -> Result<()> {
    let (count, ms) = hooks::run_hooks_db(client, all_hooks, phase, placeholders).await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;
    Ok(())
}

async fn apply_one(
    client: &DbClient,
    m: &ResolvedMigration,
    schema: &str,
    table: &str,
    installed_by: &str,
    placeholders: &HashMap<String, String>,
) -> Result<i32> {
    let sql = replace_placeholders(&m.sql, placeholders)?;
    log::info!("Applying migration; script={}", m.script);
    let elapsed = client
        .execute_raw(&sql)
        .await
        .map_err(|e| WaypointError::MigrationFailed {
            script: m.script.clone(),
            reason: e.to_string(),
        })?;

    let migration_type = if m.version().is_some() {
        "SQL"
    } else {
        "SQL_REPEATABLE"
    };
    history::insert_applied_migration_db(
        client,
        schema,
        table,
        m.version().map(|v| v.raw.as_str()),
        &m.description,
        migration_type,
        &m.script,
        Some(m.checksum),
        installed_by,
        elapsed,
        true,
    )
    .await?;

    Ok(elapsed)
}
