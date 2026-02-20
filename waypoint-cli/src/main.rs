//! CLI entry point for the waypoint migration tool.
//! Provides clap-based command routing for 16 subcommands, exit code mapping
//! based on error type, and multi-database dispatch.

mod output;
mod self_update;

use std::process;

use clap::{Parser, Subcommand};
use colored::Colorize;

use waypoint_core::config::{normalize_location, CliOverrides, WaypointConfig};
use waypoint_core::error::WaypointError;
use waypoint_core::migration::MigrationVersion;
use waypoint_core::{UndoTarget, Waypoint};

/// Top-level CLI definition with global flags and subcommand dispatch.
#[derive(Parser)]
#[command(
    name = "waypoint",
    about = "Lightweight SQL migration tool",
    version = concat!(
        env!("CARGO_PKG_VERSION"),
        " (", env!("GIT_HASH"), " ", env!("BUILD_TIME"), ")"
    ),
    propagate_version = true
)]
struct Cli {
    /// Config file path
    #[arg(short, long, value_name = "PATH")]
    config: Option<String>,

    /// Database URL (overrides config)
    #[arg(long, value_name = "URL")]
    url: Option<String>,

    /// Target schema (overrides config)
    #[arg(long, value_name = "SCHEMA")]
    schema: Option<String>,

    /// History table name (overrides config)
    #[arg(long, value_name = "TABLE")]
    table: Option<String>,

    /// Migration locations, comma-separated (overrides config)
    #[arg(long, value_name = "PATHS")]
    locations: Option<String>,

    /// Number of retries when connecting to the database
    #[arg(long, value_name = "N")]
    connect_retries: Option<u32>,

    /// SSL/TLS mode: disable, prefer, require
    #[arg(long, value_name = "MODE")]
    ssl_mode: Option<String>,

    /// Connection timeout in seconds (default: 30, 0 = no timeout)
    #[arg(long, value_name = "SECS")]
    connect_timeout: Option<u32>,

    /// Statement timeout in seconds (default: 0 = no limit)
    #[arg(long, value_name = "SECS")]
    statement_timeout: Option<u32>,

    /// Allow out-of-order migrations
    #[arg(long, overrides_with = "no_out_of_order")]
    out_of_order: bool,

    /// Disallow out-of-order migrations (overrides --out-of-order)
    #[arg(long = "no-out-of-order", hide = true)]
    no_out_of_order: bool,

    /// Validate before migrating (default: true)
    #[arg(long, overrides_with = "no_validate_on_migrate")]
    validate_on_migrate: Option<bool>,

    /// Disable validate-on-migrate
    #[arg(long = "no-validate-on-migrate", hide = true)]
    no_validate_on_migrate: bool,

    /// Output results as JSON
    #[arg(long, global = true)]
    json: bool,

    /// Preview what would be done without making changes
    #[arg(long, global = true)]
    dry_run: bool,

    /// Suppress non-essential output
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Enable verbose/debug output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Environment for environment-scoped migrations
    #[arg(long, value_name = "ENV", global = true)]
    environment: Option<String>,

    /// Enable dependency-based migration ordering
    #[arg(long, global = true)]
    dependency_ordering: bool,

    /// Skip pre-flight health checks
    #[arg(long, global = true)]
    skip_preflight: bool,

    /// Filter to a specific database (multi-db mode)
    #[arg(long, value_name = "NAME", global = true)]
    database: Option<String>,

    /// Stop on first failure (multi-db mode)
    #[arg(long, global = true)]
    fail_fast: bool,

    #[command(subcommand)]
    command: Commands,
}

/// All available waypoint subcommands.
#[derive(Subcommand)]
enum Commands {
    /// Apply pending migrations
    Migrate {
        /// Migrate up to this version only
        #[arg(long, value_name = "VERSION")]
        target: Option<String>,
    },

    /// Show migration status
    Info,

    /// Validate applied migrations
    Validate,

    /// Repair the schema history table
    Repair,

    /// Baseline an existing database
    Baseline {
        /// Version to baseline at
        #[arg(long, value_name = "VER")]
        baseline_version: Option<String>,

        /// Description for baseline entry
        #[arg(long, value_name = "DESC")]
        baseline_description: Option<String>,
    },

    /// Undo applied migration(s)
    Undo {
        /// Undo all versions above this version (exclusive)
        #[arg(long, value_name = "VERSION", conflicts_with = "count")]
        target: Option<String>,

        /// Number of migrations to undo
        #[arg(long, value_name = "N", conflicts_with = "target")]
        count: Option<usize>,
    },

    /// Drop all objects in managed schemas
    Clean {
        /// Required flag to actually run clean
        #[arg(long)]
        allow_clean: bool,
    },

    /// Static analysis of migration SQL files
    Lint {
        /// Disable specific rules (comma-separated)
        #[arg(long, value_name = "RULES", value_delimiter = ',')]
        disable: Vec<String>,
        /// Exit code 1 if any errors found
        #[arg(long)]
        strict: bool,
    },

    /// Auto-generate changelog from migration DDL
    Changelog {
        /// Start from this version
        #[arg(long, value_name = "VERSION")]
        from: Option<String>,
        /// End at this version
        #[arg(long, value_name = "VERSION")]
        to: Option<String>,
        /// Output format: plain, markdown, json
        #[arg(long, default_value = "plain")]
        format: String,
    },

    /// Compare database schema against a target
    Diff {
        /// Compare against another database URL
        #[arg(long, value_name = "URL")]
        target_url: Option<String>,
        /// Write output SQL to file
        #[arg(long)]
        output: Option<String>,
        /// Auto-generate versioned migration file (V{next}__Auto_generated.sql)
        #[arg(long)]
        auto_version: bool,
    },

    /// Detect manual schema changes that bypassed migrations
    Drift,

    /// Take a schema snapshot
    Snapshot,

    /// Restore from a schema snapshot
    Restore {
        /// Snapshot ID to restore (omit to list available)
        #[arg(value_name = "ID")]
        snapshot_id: Option<String>,
    },

    /// Run pre-flight health checks
    Preflight,

    /// Detect migration conflicts between git branches
    CheckConflicts {
        /// Base branch to compare against
        #[arg(long, default_value = "main")]
        base: String,
        /// Minimal output for git hooks
        #[arg(long)]
        git_hook: bool,
    },

    /// Update waypoint to the latest version
    SelfUpdate {
        /// Check for updates without installing
        #[arg(long)]
        check: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Set up logging (suppress when JSON output is requested)
    let filter = if cli.json {
        "error"
    } else if cli.verbose {
        "debug"
    } else if cli.quiet {
        "error"
    } else {
        "info"
    };

    env_logger::Builder::new()
        .parse_env(env_logger::Env::default().default_filter_or(filter))
        .format_target(false)
        .format_timestamp(None)
        .init();

    if let Err(e) = run(cli).await {
        print_error(&e);
        process::exit(exit_code(&e));
    }
}

/// Map error types to differentiated exit codes.
fn exit_code(error: &WaypointError) -> i32 {
    match error {
        WaypointError::ConfigError(_) => 2,
        WaypointError::ValidationFailed(_) => 3,
        WaypointError::DatabaseError(_) => 4,
        WaypointError::MigrationFailed { .. } => 5,
        WaypointError::HookFailed { .. } => 5,
        WaypointError::UndoFailed { .. } => 5,
        WaypointError::UndoMissing { .. } => 5,
        WaypointError::LockError(_) => 6,
        WaypointError::CleanDisabled => 7,
        WaypointError::UpdateError(_) => 8,
        WaypointError::LintFailed { .. } => 9,
        WaypointError::DriftDetected { .. } => 10,
        WaypointError::ConflictsDetected { .. } => 11,
        WaypointError::PreflightFailed { .. } => 12,
        _ => 1,
    }
}

/// Build configuration, resolve multi-database mode, and dispatch the chosen subcommand.
async fn run(cli: Cli) -> Result<(), WaypointError> {
    let json_output = cli.json;
    let dry_run = cli.dry_run;
    let skip_preflight = cli.skip_preflight;

    // Handle self-update before config/DB setup (no database needed)
    if let Commands::SelfUpdate { check } = &cli.command {
        return self_update::self_update(*check, json_output);
    }

    // Build CLI overrides with negation flag support
    let out_of_order = if cli.out_of_order {
        Some(true)
    } else if cli.no_out_of_order {
        Some(false)
    } else {
        None
    };

    let validate_on_migrate = if cli.no_validate_on_migrate {
        Some(false)
    } else {
        cli.validate_on_migrate
    };

    let overrides = CliOverrides {
        url: cli.url,
        schema: cli.schema,
        table: cli.table,
        locations: cli
            .locations
            .map(|l| l.split(',').map(|s| normalize_location(s.trim())).collect()),
        out_of_order,
        validate_on_migrate,
        baseline_version: match &cli.command {
            Commands::Baseline {
                baseline_version, ..
            } => baseline_version.clone(),
            _ => None,
        },
        connect_retries: cli.connect_retries,
        ssl_mode: cli.ssl_mode,
        connect_timeout: cli.connect_timeout,
        statement_timeout: cli.statement_timeout,
        environment: cli.environment,
        dependency_ordering: if cli.dependency_ordering {
            Some(true)
        } else {
            None
        },
    };

    // Load config
    let mut config = WaypointConfig::load(cli.config.as_deref(), &overrides)?;

    // Override preflight if --skip-preflight
    if skip_preflight {
        config.preflight.enabled = false;
    }

    // === Commands that don't need a DB connection ===

    match &cli.command {
        Commands::Lint { disable, strict } => {
            let mut disabled = config.lint.disabled_rules.clone();
            disabled.extend(disable.iter().cloned());
            let report =
                waypoint_core::commands::lint::execute(&config.migrations.locations, &disabled)?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_lint_report(&report);
            }
            if *strict && report.error_count > 0 {
                return Err(WaypointError::LintFailed {
                    error_count: report.error_count,
                    details: format!("{} warning(s)", report.warning_count),
                });
            }
            return Ok(());
        }
        Commands::Changelog { from, to, format } => {
            let report = waypoint_core::commands::changelog::execute(
                &config.migrations.locations,
                from.as_deref(),
                to.as_deref(),
            )?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                let fmt =
                    waypoint_core::commands::changelog::ChangelogFormat::parse(format);
                match fmt {
                    waypoint_core::commands::changelog::ChangelogFormat::Markdown => {
                        print!(
                            "{}",
                            waypoint_core::commands::changelog::render_markdown(&report)
                        );
                    }
                    waypoint_core::commands::changelog::ChangelogFormat::Json => {
                        println!("{}", serde_json::to_string_pretty(&report).unwrap());
                    }
                    waypoint_core::commands::changelog::ChangelogFormat::PlainText => {
                        print!(
                            "{}",
                            waypoint_core::commands::changelog::render_plain(&report)
                        );
                    }
                }
            }
            return Ok(());
        }
        Commands::CheckConflicts { base, git_hook } => {
            let report = waypoint_core::commands::check_conflicts::execute(
                &config.migrations.locations,
                base,
            )?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else if *git_hook {
                if report.has_conflicts {
                    eprintln!(
                        "Migration conflicts detected: {} conflict(s)",
                        report.conflicts.len()
                    );
                }
            } else {
                output::print_conflict_report(&report);
            }
            if report.has_conflicts {
                return Err(WaypointError::ConflictsDetected {
                    count: report.conflicts.len(),
                    details: report
                        .conflicts
                        .iter()
                        .map(|c| c.description.clone())
                        .collect::<Vec<_>>()
                        .join("; "),
                });
            }
            return Ok(());
        }
        _ => {}
    }

    // === Multi-database mode ===
    if let Some(ref databases) = config.multi_database {
        let order = waypoint_core::MultiWaypoint::execution_order(databases)?;
        let clients = waypoint_core::MultiWaypoint::connect(
            databases,
            cli.database.as_deref(),
        )
        .await?;

        match &cli.command {
            Commands::Migrate { target } => {
                let result = waypoint_core::MultiWaypoint::migrate(
                    databases,
                    &clients,
                    &order,
                    target.as_deref(),
                    cli.fail_fast,
                )
                .await?;
                if json_output {
                    println!("{}", serde_json::to_string_pretty(&result).unwrap());
                } else {
                    output::print_multi_result(&result);
                }
                if !result.all_succeeded {
                    return Err(WaypointError::MultiDbError {
                        name: "multi".to_string(),
                        reason: "One or more databases failed".to_string(),
                    });
                }
            }
            Commands::Info => {
                let all_info = waypoint_core::MultiWaypoint::info(
                    databases,
                    &clients,
                    &order,
                )
                .await?;
                if json_output {
                    println!("{}", serde_json::to_string_pretty(&all_info).unwrap());
                } else {
                    output::print_multi_info(&all_info);
                }
            }
            _ => {
                // For other commands, run on filtered single DB
                if let Some(ref db_name) = cli.database {
                    if let Some(db) = databases.iter().find(|d| &d.name == db_name) {
                        let single_config = db.to_waypoint_config();
                        let wp = Waypoint::new(single_config).await?;
                        return run_single_db_command(&cli.command, &wp, json_output, dry_run)
                            .await;
                    }
                }
                return Err(WaypointError::ConfigError(
                    "Multi-database mode: use --database to select a database for this command"
                        .to_string(),
                ));
            }
        }
        return Ok(());
    }

    // === Single database mode ===

    // Dry-run mode: show what would be applied using info/explain
    if dry_run {
        if let Commands::Migrate { .. } = &cli.command {
            let wp = Waypoint::new(config).await?;
            let report = waypoint_core::commands::explain::execute(wp.client(), &wp.config).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_explain_report(&report);
            }
            return Ok(());
        }
    }

    // Create waypoint instance
    let wp = Waypoint::new(config).await?;

    run_single_db_command(&cli.command, &wp, json_output, dry_run).await
}

/// Execute a subcommand against a single database instance.
async fn run_single_db_command(
    command: &Commands,
    wp: &Waypoint,
    json_output: bool,
    _dry_run: bool,
) -> Result<(), WaypointError> {
    match command {
        Commands::Migrate { target, .. } => {
            let report = wp.migrate(target.as_deref()).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_migrate_summary(&report);
            }
        }
        Commands::Info => {
            let infos = wp.info().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&infos).unwrap());
            } else {
                output::print_info_table(&infos);
            }
        }
        Commands::Validate => {
            let report = wp.validate().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_validate_result(&report);
            }
        }
        Commands::Repair => {
            let report = wp.repair().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_repair_result(&report);
            }
        }
        Commands::Baseline {
            baseline_version,
            baseline_description,
        } => {
            wp.baseline(
                baseline_version.as_deref(),
                baseline_description.as_deref(),
            )
            .await?;
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({"success": true, "message": "Successfully baselined schema."})
                );
            } else {
                println!("{}", "Successfully baselined schema.".green().bold());
            }
        }
        Commands::Undo { target, count } => {
            let undo_target = if let Some(ver) = target {
                UndoTarget::Version(MigrationVersion::parse(ver)?)
            } else if let Some(n) = count {
                UndoTarget::Count(*n)
            } else {
                UndoTarget::Last
            };
            let report = wp.undo(undo_target).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_undo_summary(&report);
            }
        }
        Commands::Clean { allow_clean } => {
            let dropped = wp.clean(*allow_clean).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&dropped).unwrap());
            } else {
                output::print_clean_result(&dropped);
            }
        }
        Commands::Diff {
            target_url,
            output: output_file,
            auto_version,
        } => {
            let target = match target_url {
                Some(url) => waypoint_core::commands::diff::DiffTarget::Database(url.clone()),
                None => {
                    return Err(WaypointError::ConfigError(
                        "Diff requires --target-url".to_string(),
                    ));
                }
            };
            let report = wp.diff(target).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_diff_report(&report);
            }
            if report.has_changes {
                let output_path = if *auto_version {
                    // Determine next version from existing migrations
                    let infos = wp.info().await?;
                    let max_version = infos
                        .iter()
                        .filter_map(|i| i.version.as_ref())
                        .filter_map(|v| v.parse::<u64>().ok())
                        .max()
                        .unwrap_or(0);
                    let next_version = max_version + 1;
                    let dir = &wp.config.migrations.locations[0];
                    let filename = format!("V{}__Auto_generated.sql", next_version);
                    Some(dir.join(filename).display().to_string())
                } else {
                    output_file.clone()
                };
                if let Some(path) = output_path {
                    std::fs::write(&path, &report.generated_sql).map_err(|e| {
                        WaypointError::IoError(e)
                    })?;
                    println!(
                        "{}",
                        format!("Generated SQL written to {}", path).green()
                    );
                }
            }
        }
        Commands::Drift => {
            let report = wp.drift().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_drift_report(&report);
            }
            if report.has_drift {
                return Err(WaypointError::DriftDetected {
                    count: report.drifts.len(),
                    details: report
                        .drifts
                        .iter()
                        .map(|d| d.object.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                });
            }
        }
        Commands::Snapshot => {
            let report = wp.snapshot(&wp.config.snapshots).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_snapshot_report(&report);
            }
        }
        Commands::Restore { snapshot_id } => {
            match snapshot_id {
                Some(id) => {
                    let report = wp.restore(&wp.config.snapshots, id).await?;
                    if json_output {
                        println!("{}", serde_json::to_string_pretty(&report).unwrap());
                    } else {
                        output::print_restore_report(&report);
                    }
                }
                None => {
                    let snapshots =
                        waypoint_core::commands::snapshot::list_snapshots(&wp.config.snapshots)?;
                    if json_output {
                        println!("{}", serde_json::to_string_pretty(&snapshots).unwrap());
                    } else {
                        output::print_snapshot_list(&snapshots);
                    }
                }
            }
        }
        Commands::Preflight => {
            let report = wp.preflight().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_preflight_report(&report);
            }
        }
        // No-DB commands handled earlier
        Commands::Lint { .. }
        | Commands::Changelog { .. }
        | Commands::CheckConflicts { .. }
        | Commands::SelfUpdate { .. } => {
            unreachable!("handled before DB setup")
        }
    }

    Ok(())
}

/// Print a formatted error message with actionable hints to stderr.
fn print_error(error: &WaypointError) {
    eprintln!("{} {}", "ERROR:".red().bold(), error);

    // Provide actionable guidance
    match error {
        WaypointError::ConfigError(_) => {
            eprintln!(
                "{}",
                "Hint: Check your waypoint.toml or set WAYPOINT_DATABASE_URL environment variable."
                    .dimmed()
            );
        }
        WaypointError::DatabaseError(_) => {
            eprintln!(
                "{}",
                "Hint: Verify database is running and connection details are correct.".dimmed()
            );
        }
        WaypointError::CleanDisabled => {
            eprintln!(
                "{}",
                "Hint: Pass --allow-clean flag or set clean_enabled = true in waypoint.toml."
                    .dimmed()
            );
        }
        WaypointError::ChecksumMismatch { .. } => {
            eprintln!(
                "{}",
                "Hint: Run 'waypoint repair' to update checksums, or restore the original migration file."
                    .dimmed()
            );
        }
        WaypointError::OutOfOrder { .. } => {
            eprintln!(
                "{}",
                "Hint: Use --out-of-order flag to allow out-of-order migrations.".dimmed()
            );
        }
        WaypointError::UndoMissing { version } => {
            eprintln!(
                "{}",
                format!(
                    "Hint: Create a U{version}__<description>.sql file in your migrations directory."
                )
                .dimmed()
            );
        }
        WaypointError::DriftDetected { .. } => {
            eprintln!(
                "{}",
                "Hint: Run 'waypoint diff' to generate a migration that resolves this drift."
                    .dimmed()
            );
        }
        WaypointError::LintFailed { .. } => {
            eprintln!(
                "{}",
                "Hint: Fix the issues or add rule IDs to [lint] disabled_rules in waypoint.toml."
                    .dimmed()
            );
        }
        _ => {}
    }
}
