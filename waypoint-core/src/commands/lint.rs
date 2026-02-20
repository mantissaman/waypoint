//! Static analysis of migration SQL files.
//!
//! Checks for common anti-patterns and dangerous operations
//! without requiring a database connection.

use std::path::PathBuf;

use serde::Serialize;

use crate::error::Result;
use crate::migration::scan_migrations;
use crate::sql_parser::{extract_ddl_operations, split_statements, DdlOperation};

/// Severity level for a lint issue.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum LintSeverity {
    /// A critical issue that will likely cause migration failure.
    Error,
    /// A potential problem or anti-pattern that deserves attention.
    Warning,
    /// An informational observation about the migration.
    Info,
}

impl std::fmt::Display for LintSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LintSeverity::Error => write!(f, "error"),
            LintSeverity::Warning => write!(f, "warning"),
            LintSeverity::Info => write!(f, "info"),
        }
    }
}

/// A single lint finding.
#[derive(Debug, Clone, Serialize)]
pub struct LintIssue {
    /// Unique identifier of the lint rule (e.g. "W001", "E001").
    pub rule_id: String,
    /// Severity level of this issue.
    pub severity: LintSeverity,
    /// Human-readable description of the issue.
    pub message: String,
    /// Filename of the migration script where the issue was found.
    pub script: String,
    /// Approximate line number of the issue, if determinable.
    pub line: Option<usize>,
    /// Suggested fix or remediation for the issue.
    pub suggestion: Option<String>,
}

/// Aggregate lint report.
#[derive(Debug, Serialize)]
pub struct LintReport {
    /// All lint issues found across checked files.
    pub issues: Vec<LintIssue>,
    /// Total number of migration files that were checked.
    pub files_checked: usize,
    /// Number of issues with Error severity.
    pub error_count: usize,
    /// Number of issues with Warning severity.
    pub warning_count: usize,
    /// Number of issues with Info severity.
    pub info_count: usize,
}

/// Execute the lint command.
pub fn execute(locations: &[PathBuf], disabled_rules: &[String]) -> Result<LintReport> {
    let migrations = scan_migrations(locations)?;
    let mut issues = Vec::new();
    let disabled: std::collections::HashSet<&str> =
        disabled_rules.iter().map(|s| s.as_str()).collect();

    let files_checked = migrations.len();

    for migration in &migrations {
        // Skip undo migrations for linting
        if migration.is_undo() {
            continue;
        }

        let sql = &migration.sql;
        let script = &migration.script;

        // I001: File contains only comments or whitespace
        if !disabled.contains("I001") {
            let meaningful = sql.lines().any(|l| {
                let t = l.trim();
                !t.is_empty() && !t.starts_with("--")
            });
            if !meaningful {
                issues.push(LintIssue {
                    rule_id: "I001".to_string(),
                    severity: LintSeverity::Info,
                    message: "File contains only comments or whitespace".to_string(),
                    script: script.clone(),
                    line: None,
                    suggestion: None,
                });
                continue;
            }
        }

        let ops = extract_ddl_operations(sql);
        let statements = split_statements(sql);

        // Pre-compute uppercase SQL once per migration for case-insensitive checks
        let upper = sql.to_uppercase();

        for op in &ops {
            match op {
                // W001: CREATE TABLE without IF NOT EXISTS
                DdlOperation::CreateTable {
                    table,
                    if_not_exists,
                } if !if_not_exists => {
                    if !disabled.contains("W001") {
                        issues.push(LintIssue {
                            rule_id: "W001".to_string(),
                            severity: LintSeverity::Warning,
                            message: format!("CREATE TABLE {} without IF NOT EXISTS", table),
                            script: script.clone(),
                            line: find_line(sql, &upper, "CREATE TABLE"),
                            suggestion: Some(
                                "Use CREATE TABLE IF NOT EXISTS to make migration re-runnable"
                                    .to_string(),
                            ),
                        });
                    }
                }

                // W002: CREATE INDEX without CONCURRENTLY
                DdlOperation::CreateIndex {
                    name,
                    is_concurrent,
                    ..
                } if !is_concurrent => {
                    if !disabled.contains("W002") {
                        issues.push(LintIssue {
                            rule_id: "W002".to_string(),
                            severity: LintSeverity::Warning,
                            message: format!(
                                "CREATE INDEX {} without CONCURRENTLY (blocks writes during creation)",
                                name
                            ),
                            script: script.clone(),
                            line: find_line(sql, &upper, "CREATE INDEX"),
                            suggestion: Some("Use CREATE INDEX CONCURRENTLY to avoid blocking writes".to_string()),
                        });
                    }
                }

                // E001: ADD COLUMN NOT NULL without DEFAULT
                DdlOperation::AlterTableAddColumn {
                    table,
                    column,
                    is_not_null,
                    has_default,
                    ..
                } if *is_not_null && !has_default => {
                    if !disabled.contains("E001") {
                        issues.push(LintIssue {
                            rule_id: "E001".to_string(),
                            severity: LintSeverity::Error,
                            message: format!(
                                "ADD COLUMN {}.{} is NOT NULL without DEFAULT (will fail if table has rows)",
                                table, column
                            ),
                            script: script.clone(),
                            line: find_line(sql, &upper, "ADD"),
                            suggestion: Some("Add a DEFAULT value or make the column nullable".to_string()),
                        });
                    }
                }

                // W003: ALTER COLUMN TYPE (full table rewrite + lock)
                DdlOperation::AlterTableAlterColumn { table, column } => {
                    if !disabled.contains("W003") {
                        // Check if it's a TYPE change (uses pre-computed uppercase)
                        if upper.contains("TYPE") {
                            issues.push(LintIssue {
                                rule_id: "W003".to_string(),
                                severity: LintSeverity::Warning,
                                message: format!(
                                    "ALTER COLUMN {}.{} TYPE causes full table rewrite and exclusive lock",
                                    table, column
                                ),
                                script: script.clone(),
                                line: find_line(sql, &upper, "ALTER COLUMN"),
                                suggestion: Some("Consider a multi-step approach: add new column, backfill, swap".to_string()),
                            });
                        }
                    }
                }

                // W004: DROP TABLE / DROP COLUMN (destructive)
                DdlOperation::DropTable { table } => {
                    if !disabled.contains("W004") {
                        issues.push(LintIssue {
                            rule_id: "W004".to_string(),
                            severity: LintSeverity::Warning,
                            message: format!(
                                "DROP TABLE {} is destructive and irreversible",
                                table
                            ),
                            script: script.clone(),
                            line: find_line(sql, &upper, "DROP TABLE"),
                            suggestion: Some(
                                "Ensure you have a backup or undo migration".to_string(),
                            ),
                        });
                    }
                }
                DdlOperation::AlterTableDropColumn { table, column } => {
                    if !disabled.contains("W004") {
                        issues.push(LintIssue {
                            rule_id: "W004".to_string(),
                            severity: LintSeverity::Warning,
                            message: format!(
                                "DROP COLUMN {}.{} is destructive and irreversible",
                                table, column
                            ),
                            script: script.clone(),
                            line: find_line(sql, &upper, "DROP COLUMN"),
                            suggestion: Some(
                                "Ensure you have a backup or undo migration".to_string(),
                            ),
                        });
                    }
                }

                // W006: Large DEFAULT expression on ADD COLUMN
                DdlOperation::AlterTableAddColumn {
                    table,
                    column,
                    has_default,
                    ..
                } if *has_default => {
                    if !disabled.contains("W006") {
                        // Heuristic: check for function calls in DEFAULT (uses pre-computed uppercase)
                        if upper.contains("DEFAULT")
                            && (upper.contains("RANDOM()")
                                || upper.contains("GEN_RANDOM_UUID()")
                                || upper.contains("NOW()"))
                        {
                            issues.push(LintIssue {
                                rule_id: "W006".to_string(),
                                severity: LintSeverity::Warning,
                                message: format!(
                                    "ADD COLUMN {}.{} with volatile DEFAULT expression (pre-PG11: table rewrite)",
                                    table, column
                                ),
                                script: script.clone(),
                                line: find_line(sql, &upper, "DEFAULT"),
                                suggestion: Some("On PostgreSQL < 11, volatile defaults cause a full table rewrite".to_string()),
                            });
                        }
                    }
                }

                // W007: TRUNCATE TABLE
                DdlOperation::TruncateTable { table } => {
                    if !disabled.contains("W007") {
                        issues.push(LintIssue {
                            rule_id: "W007".to_string(),
                            severity: LintSeverity::Warning,
                            message: format!(
                                "TRUNCATE TABLE {} is destructive and acquires ACCESS EXCLUSIVE lock",
                                table
                            ),
                            script: script.clone(),
                            line: find_line(sql, &upper, "TRUNCATE"),
                            suggestion: Some("Ensure this is intentional and the table can be locked exclusively".to_string()),
                        });
                    }
                }

                _ => {}
            }
        }

        // E002: Multiple DDL statements without explicit transaction control
        if !disabled.contains("E002") {
            let ddl_count = ops
                .iter()
                .filter(|op| !matches!(op, DdlOperation::Other { .. }))
                .count();
            let has_begin = statements.iter().any(|s| {
                s.trim()
                    .get(..5)
                    .is_some_and(|w| w.eq_ignore_ascii_case("BEGIN"))
            });
            if ddl_count > 1 && !has_begin {
                // This is a warning because waypoint wraps in a transaction by default
                issues.push(LintIssue {
                    rule_id: "E002".to_string(),
                    severity: LintSeverity::Error,
                    message: format!(
                        "{} DDL statements without explicit BEGIN/COMMIT (relies on tool-level transaction)",
                        ddl_count
                    ),
                    script: script.clone(),
                    line: None,
                    suggestion: Some("Consider adding explicit BEGIN/COMMIT for clarity, or split into separate migrations".to_string()),
                });
            }
        }
    }

    let error_count = issues
        .iter()
        .filter(|i| i.severity == LintSeverity::Error)
        .count();
    let warning_count = issues
        .iter()
        .filter(|i| i.severity == LintSeverity::Warning)
        .count();
    let info_count = issues
        .iter()
        .filter(|i| i.severity == LintSeverity::Info)
        .count();

    Ok(LintReport {
        issues,
        files_checked,
        error_count,
        warning_count,
        info_count,
    })
}

/// Find the approximate line number of a pattern in SQL content.
///
/// Accepts the pre-computed uppercase SQL to avoid re-allocating.
fn find_line(sql: &str, upper_sql: &str, pattern: &str) -> Option<usize> {
    upper_sql
        .find(pattern)
        .map(|offset| sql[..offset].lines().count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_migration(dir: &std::path::Path, name: &str, sql: &str) {
        fs::write(dir.join(name), sql).unwrap();
    }

    #[test]
    fn test_lint_create_table_without_if_not_exists() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Create_users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W001"));
    }

    #[test]
    fn test_lint_create_table_with_if_not_exists_passes() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Create_users.sql",
            "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY);",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(!report.issues.iter().any(|i| i.rule_id == "W001"));
    }

    #[test]
    fn test_lint_add_column_not_null_without_default() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Add_email.sql",
            "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL;",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "E001"));
        assert!(report.error_count > 0);
    }

    #[test]
    fn test_lint_index_without_concurrently() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Add_index.sql",
            "CREATE INDEX idx_users_email ON users (email);",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W002"));
    }

    #[test]
    fn test_lint_disabled_rules() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Create_users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        );

        let report = execute(&[dir.path().to_path_buf()], &["W001".to_string()]).unwrap();
        assert!(!report.issues.iter().any(|i| i.rule_id == "W001"));
    }

    #[test]
    fn test_lint_drop_table() {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Drop_old.sql", "DROP TABLE old_table;");

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W004"));
    }

    #[test]
    fn test_lint_empty_file() {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Empty.sql", "-- Just a comment\n");

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "I001"));
    }

    #[test]
    fn test_lint_truncate() {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Truncate.sql", "TRUNCATE TABLE users;");

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W007"));
    }
}
