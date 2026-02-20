//! Migration safety analysis: lock levels, impact estimation, and verdicts.

use serde::Serialize;

use crate::error::{Result, WaypointError};
use crate::sql_parser::DdlOperation;

/// PostgreSQL lock levels, ordered from least to most restrictive.
///
/// The ordering matches PostgreSQL's internal lock hierarchy so that
/// comparisons (e.g. `lock > LockLevel::ShareLock`) work correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum LockLevel {
    /// No lock acquired (new objects, functions, enums).
    None,
    /// ACCESS SHARE — acquired by SELECT.
    AccessShareLock,
    /// ROW SHARE — acquired by SELECT FOR UPDATE/SHARE.
    RowShareLock,
    /// ROW EXCLUSIVE — acquired by INSERT/UPDATE/DELETE.
    RowExclusiveLock,
    /// SHARE UPDATE EXCLUSIVE — acquired by VACUUM, CREATE INDEX CONCURRENTLY.
    ShareUpdateExclusiveLock,
    /// SHARE — acquired by CREATE INDEX (non-concurrent).
    ShareLock,
    /// SHARE ROW EXCLUSIVE — acquired by some constraint triggers.
    ShareRowExclusiveLock,
    /// EXCLUSIVE — blocks all reads/writes except ACCESS SHARE.
    ExclusiveLock,
    /// ACCESS EXCLUSIVE — the strongest lock; blocks everything.
    AccessExclusiveLock,
}

impl std::fmt::Display for LockLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockLevel::None => write!(f, "None"),
            LockLevel::AccessShareLock => write!(f, "ACCESS SHARE"),
            LockLevel::RowShareLock => write!(f, "ROW SHARE"),
            LockLevel::RowExclusiveLock => write!(f, "ROW EXCLUSIVE"),
            LockLevel::ShareUpdateExclusiveLock => write!(f, "SHARE UPDATE EXCLUSIVE"),
            LockLevel::ShareLock => write!(f, "SHARE"),
            LockLevel::ShareRowExclusiveLock => write!(f, "SHARE ROW EXCLUSIVE"),
            LockLevel::ExclusiveLock => write!(f, "EXCLUSIVE"),
            LockLevel::AccessExclusiveLock => write!(f, "ACCESS EXCLUSIVE"),
        }
    }
}

/// Rough classification of table size based on estimated row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TableSize {
    /// Fewer than 10,000 rows.
    Small,
    /// 10,000 to 1,000,000 rows.
    Medium,
    /// 1,000,000 to 100,000,000 rows.
    Large,
    /// More than 100,000,000 rows.
    Huge,
}

impl std::fmt::Display for TableSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableSize::Small => write!(f, "Small (<10k rows)"),
            TableSize::Medium => write!(f, "Medium (10k-1M rows)"),
            TableSize::Large => write!(f, "Large (1M-100M rows)"),
            TableSize::Huge => write!(f, "Huge (>100M rows)"),
        }
    }
}

/// Overall safety verdict for a migration statement or script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum SafetyVerdict {
    /// No significant risk detected.
    Safe,
    /// Moderate risk — review recommended.
    Caution,
    /// High risk — may cause downtime or data loss.
    Danger,
}

impl std::fmt::Display for SafetyVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SafetyVerdict::Safe => write!(f, "SAFE"),
            SafetyVerdict::Caution => write!(f, "CAUTION"),
            SafetyVerdict::Danger => write!(f, "DANGER"),
        }
    }
}

/// Safety analysis for a single SQL statement within a migration.
#[derive(Debug, Clone, Serialize)]
pub struct StatementAnalysis {
    /// A short preview of the analyzed statement.
    pub statement_preview: String,
    /// The PostgreSQL lock level this statement acquires.
    pub lock_level: LockLevel,
    /// The table affected by this statement, if identifiable.
    pub affected_table: Option<String>,
    /// Estimated table size classification, if known.
    pub table_size: Option<TableSize>,
    /// Estimated live row count, if available from statistics.
    pub estimated_rows: Option<i64>,
    /// The safety verdict for this statement.
    pub verdict: SafetyVerdict,
    /// Actionable suggestions for reducing risk.
    pub suggestions: Vec<String>,
    /// Whether this statement causes irreversible data loss.
    pub data_loss: bool,
}

/// Full safety report for a migration script.
#[derive(Debug, Clone, Serialize)]
pub struct SafetyReport {
    /// The migration script filename or identifier.
    pub script: String,
    /// The worst-case verdict across all statements.
    pub overall_verdict: SafetyVerdict,
    /// Per-statement analysis results.
    pub statements: Vec<StatementAnalysis>,
    /// Aggregated suggestions across all statements.
    pub suggestions: Vec<String>,
}

/// Configuration for safety analysis.
#[derive(Debug, Clone)]
pub struct SafetyConfig {
    /// Whether safety analysis is enabled.
    pub enabled: bool,
    /// Whether to block migrations that receive a DANGER verdict.
    pub block_on_danger: bool,
    /// Row count threshold for classifying a table as Large.
    pub large_table_threshold: i64,
    /// Row count threshold for classifying a table as Huge.
    pub huge_table_threshold: i64,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            block_on_danger: false,
            large_table_threshold: 1_000_000,
            huge_table_threshold: 100_000_000,
        }
    }
}

/// Determine the PostgreSQL lock level required by a DDL operation.
pub fn lock_level_for_ddl(op: &DdlOperation) -> LockLevel {
    match op {
        DdlOperation::CreateTable { .. } => LockLevel::None,
        DdlOperation::AlterTableAddColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::AlterTableDropColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::AlterTableAlterColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateIndex { is_concurrent, .. } => {
            if *is_concurrent {
                LockLevel::ShareUpdateExclusiveLock
            } else {
                LockLevel::ShareLock
            }
        }
        DdlOperation::DropTable { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::DropIndex { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateView { .. } => LockLevel::None,
        DdlOperation::DropView { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateFunction { .. } => LockLevel::None,
        DdlOperation::DropFunction { .. } => LockLevel::None,
        DdlOperation::AddConstraint { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::DropConstraint { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateEnum { .. } => LockLevel::None,
        DdlOperation::TruncateTable { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::Other { .. } => LockLevel::None,
    }
}

/// Classify a table's size by querying PostgreSQL statistics.
///
/// Returns the classification and the estimated row count from
/// `pg_stat_user_tables.n_live_tup`.
pub async fn classify_table_size(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    large_threshold: i64,
    huge_threshold: i64,
) -> Result<(TableSize, i64)> {
    let row = client
        .query_opt(
            "SELECT n_live_tup FROM pg_stat_user_tables \
             WHERE schemaname = $1 AND relname = $2",
            &[&schema, &table],
        )
        .await
        .map_err(WaypointError::DatabaseError)?;

    let estimated_rows: i64 = match row {
        Some(r) => r.get::<_, i64>(0),
        None => 0,
    };

    let size = classify_row_count(estimated_rows, large_threshold, huge_threshold);
    Ok((size, estimated_rows))
}

/// Classify a row count into a [`TableSize`] using the given thresholds.
fn classify_row_count(rows: i64, large_threshold: i64, huge_threshold: i64) -> TableSize {
    if rows > huge_threshold {
        TableSize::Huge
    } else if rows > large_threshold {
        TableSize::Large
    } else if rows >= 10_000 {
        TableSize::Medium
    } else {
        TableSize::Small
    }
}

/// Determine the safety verdict for a statement given its lock level,
/// affected table size, and whether it causes data loss.
fn compute_verdict(lock: LockLevel, size: TableSize, data_loss: bool) -> SafetyVerdict {
    // AccessExclusiveLock on Large/Huge tables is always dangerous
    if lock == LockLevel::AccessExclusiveLock
        && (size == TableSize::Large || size == TableSize::Huge)
    {
        return SafetyVerdict::Danger;
    }

    // Data loss operations on Large/Huge tables are dangerous
    if data_loss && (size == TableSize::Large || size == TableSize::Huge) {
        return SafetyVerdict::Danger;
    }

    // AccessExclusiveLock on Small/Medium tables warrants caution
    if lock == LockLevel::AccessExclusiveLock {
        return SafetyVerdict::Caution;
    }

    // ShareLock (non-concurrent index) on Large/Huge warrants caution
    if lock == LockLevel::ShareLock && (size == TableSize::Large || size == TableSize::Huge) {
        return SafetyVerdict::Caution;
    }

    SafetyVerdict::Safe
}

/// Generate actionable suggestions for a DDL operation based on table size.
fn generate_suggestions(op: &DdlOperation, size: TableSize) -> Vec<String> {
    let mut suggestions = Vec::new();

    match op {
        DdlOperation::CreateIndex {
            is_concurrent: false,
            ..
        } if size == TableSize::Large || size == TableSize::Huge => {
            suggestions.push("Use CREATE INDEX CONCURRENTLY".to_string());
        }
        DdlOperation::AlterTableAddColumn {
            is_not_null: true,
            has_default: true,
            ..
        } if size == TableSize::Large || size == TableSize::Huge => {
            suggestions.push("Split into: add nullable column, backfill, set NOT NULL".to_string());
        }
        DdlOperation::AlterTableAlterColumn { .. }
            if size == TableSize::Large || size == TableSize::Huge =>
        {
            suggestions.push("Use add-column + backfill + swap pattern".to_string());
        }
        DdlOperation::DropTable { .. } | DdlOperation::AlterTableDropColumn { .. } => {
            suggestions.push("Consider soft-delete pattern for reversibility".to_string());
        }
        DdlOperation::TruncateTable { .. } => {
            suggestions.push("Consider DELETE with batching for large tables".to_string());
        }
        _ => {}
    }

    suggestions
}

/// Check whether a DDL operation causes irreversible data loss.
fn is_data_loss(op: &DdlOperation) -> bool {
    matches!(
        op,
        DdlOperation::DropTable { .. }
            | DdlOperation::AlterTableDropColumn { .. }
            | DdlOperation::TruncateTable { .. }
    )
}

/// Extract the affected table name from a DDL operation, if applicable.
fn affected_table(op: &DdlOperation) -> Option<String> {
    match op {
        DdlOperation::CreateTable { table, .. }
        | DdlOperation::DropTable { table }
        | DdlOperation::AlterTableAddColumn { table, .. }
        | DdlOperation::AlterTableDropColumn { table, .. }
        | DdlOperation::AlterTableAlterColumn { table, .. }
        | DdlOperation::CreateIndex { table, .. }
        | DdlOperation::AddConstraint { table, .. }
        | DdlOperation::DropConstraint { table, .. }
        | DdlOperation::TruncateTable { table } => Some(table.clone()),
        DdlOperation::DropIndex { .. }
        | DdlOperation::CreateView { .. }
        | DdlOperation::DropView { .. }
        | DdlOperation::CreateFunction { .. }
        | DdlOperation::DropFunction { .. }
        | DdlOperation::CreateEnum { .. }
        | DdlOperation::Other { .. } => None,
    }
}

/// Analyze a migration script for safety concerns.
///
/// Parses the SQL into individual DDL operations, queries the database
/// for table size statistics, and produces a [`SafetyReport`] with
/// per-statement verdicts and suggestions.
pub async fn analyze_migration(
    client: &tokio_postgres::Client,
    schema: &str,
    sql: &str,
    script: &str,
    config: &SafetyConfig,
) -> Result<SafetyReport> {
    let ops = crate::sql_parser::extract_ddl_operations(sql);
    let mut statements = Vec::new();
    let mut all_suggestions = Vec::new();
    let mut worst_verdict = SafetyVerdict::Safe;

    for op in &ops {
        let lock = lock_level_for_ddl(op);
        let table = affected_table(op);
        let data_loss = is_data_loss(op);

        let (table_size, estimated_rows) = if let Some(ref t) = table {
            match classify_table_size(
                client,
                schema,
                t,
                config.large_table_threshold,
                config.huge_table_threshold,
            )
            .await
            {
                Ok((size, rows)) => (Some(size), Some(rows)),
                // Table may not exist yet (CREATE TABLE) — treat as Small
                Err(_) => (Some(TableSize::Small), None),
            }
        } else {
            (None, None)
        };

        let size_for_verdict = table_size.unwrap_or(TableSize::Small);
        let verdict = compute_verdict(lock, size_for_verdict, data_loss);

        let suggestions = generate_suggestions(op, size_for_verdict);
        all_suggestions.extend(suggestions.clone());

        // Track the worst verdict
        if verdict == SafetyVerdict::Danger
            || (verdict == SafetyVerdict::Caution && worst_verdict == SafetyVerdict::Safe)
        {
            worst_verdict = verdict;
        }

        let preview: String = op.to_string().chars().take(120).collect();

        statements.push(StatementAnalysis {
            statement_preview: preview,
            lock_level: lock,
            affected_table: table,
            table_size,
            estimated_rows,
            verdict,
            suggestions,
            data_loss,
        });
    }

    // De-duplicate suggestions
    all_suggestions.sort();
    all_suggestions.dedup();

    Ok(SafetyReport {
        script: script.to_string(),
        overall_verdict: worst_verdict,
        statements,
        suggestions: all_suggestions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Lock level mapping tests ──────────────────────────────────────

    #[test]
    fn test_lock_create_table_is_none() {
        let op = DdlOperation::CreateTable {
            table: "users".into(),
            if_not_exists: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_alter_table_add_column() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "email".into(),
            data_type: "text".into(),
            has_default: false,
            is_not_null: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_alter_table_drop_column() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "users".into(),
            column: "email".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_alter_table_alter_column() {
        let op = DdlOperation::AlterTableAlterColumn {
            table: "users".into(),
            column: "name".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_index_concurrent() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: true,
            is_unique: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::ShareUpdateExclusiveLock);
    }

    #[test]
    fn test_lock_create_index_non_concurrent() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::ShareLock);
    }

    #[test]
    fn test_lock_drop_table() {
        let op = DdlOperation::DropTable {
            table: "users".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_drop_index() {
        let op = DdlOperation::DropIndex {
            name: "idx_email".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_view() {
        let op = DdlOperation::CreateView {
            name: "user_stats".into(),
            is_materialized: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_drop_view() {
        let op = DdlOperation::DropView {
            name: "user_stats".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_function() {
        let op = DdlOperation::CreateFunction {
            name: "my_func".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_drop_function() {
        let op = DdlOperation::DropFunction {
            name: "my_func".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_add_constraint() {
        let op = DdlOperation::AddConstraint {
            table: "users".into(),
            constraint_type: "FOREIGN KEY".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_drop_constraint() {
        let op = DdlOperation::DropConstraint {
            table: "users".into(),
            name: "fk_user_org".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_enum() {
        let op = DdlOperation::CreateEnum {
            name: "mood".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_truncate_table() {
        let op = DdlOperation::TruncateTable {
            table: "logs".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_other_is_none() {
        let op = DdlOperation::Other {
            statement_preview: "INSERT INTO ...".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    // ── Lock level ordering ───────────────────────────────────────────

    #[test]
    fn test_lock_level_ordering() {
        assert!(LockLevel::None < LockLevel::AccessShareLock);
        assert!(LockLevel::AccessShareLock < LockLevel::RowShareLock);
        assert!(LockLevel::RowShareLock < LockLevel::RowExclusiveLock);
        assert!(LockLevel::RowExclusiveLock < LockLevel::ShareUpdateExclusiveLock);
        assert!(LockLevel::ShareUpdateExclusiveLock < LockLevel::ShareLock);
        assert!(LockLevel::ShareLock < LockLevel::ShareRowExclusiveLock);
        assert!(LockLevel::ShareRowExclusiveLock < LockLevel::ExclusiveLock);
        assert!(LockLevel::ExclusiveLock < LockLevel::AccessExclusiveLock);
    }

    // ── Verdict computation tests ─────────────────────────────────────

    #[test]
    fn test_verdict_access_exclusive_large_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Large, false),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_access_exclusive_huge_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Huge, false),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_data_loss_on_large_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Large, true),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_data_loss_on_huge_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::None, TableSize::Huge, true),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_access_exclusive_small_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Small, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_access_exclusive_medium_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Medium, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_share_lock_large_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::ShareLock, TableSize::Large, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_share_lock_huge_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::ShareLock, TableSize::Huge, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_share_lock_small_is_safe() {
        assert_eq!(
            compute_verdict(LockLevel::ShareLock, TableSize::Small, false),
            SafetyVerdict::Safe
        );
    }

    #[test]
    fn test_verdict_none_lock_small_is_safe() {
        assert_eq!(
            compute_verdict(LockLevel::None, TableSize::Small, false),
            SafetyVerdict::Safe
        );
    }

    #[test]
    fn test_verdict_concurrent_index_large_is_safe() {
        // ShareUpdateExclusiveLock should be safe even on large tables
        assert_eq!(
            compute_verdict(LockLevel::ShareUpdateExclusiveLock, TableSize::Large, false),
            SafetyVerdict::Safe
        );
    }

    // ── Data loss detection ───────────────────────────────────────────

    #[test]
    fn test_data_loss_drop_table() {
        let op = DdlOperation::DropTable {
            table: "users".into(),
        };
        assert!(is_data_loss(&op));
    }

    #[test]
    fn test_data_loss_drop_column() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "users".into(),
            column: "email".into(),
        };
        assert!(is_data_loss(&op));
    }

    #[test]
    fn test_data_loss_truncate() {
        let op = DdlOperation::TruncateTable {
            table: "logs".into(),
        };
        assert!(is_data_loss(&op));
    }

    #[test]
    fn test_no_data_loss_create_table() {
        let op = DdlOperation::CreateTable {
            table: "users".into(),
            if_not_exists: false,
        };
        assert!(!is_data_loss(&op));
    }

    #[test]
    fn test_no_data_loss_add_column() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "email".into(),
            data_type: "text".into(),
            has_default: false,
            is_not_null: false,
        };
        assert!(!is_data_loss(&op));
    }

    #[test]
    fn test_no_data_loss_create_index() {
        let op = DdlOperation::CreateIndex {
            name: "idx".into(),
            table: "users".into(),
            is_concurrent: true,
            is_unique: false,
        };
        assert!(!is_data_loss(&op));
    }

    // ── Suggestion generation tests ───────────────────────────────────

    #[test]
    fn test_suggestion_non_concurrent_index_large() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("CONCURRENTLY"));
    }

    #[test]
    fn test_suggestion_non_concurrent_index_huge() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Huge);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("CONCURRENTLY"));
    }

    #[test]
    fn test_suggestion_non_concurrent_index_small_no_suggestion() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Small);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_concurrent_index_large_no_suggestion() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: true,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_add_not_null_default_large() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "status".into(),
            data_type: "text".into(),
            has_default: true,
            is_not_null: true,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("nullable column"));
    }

    #[test]
    fn test_suggestion_add_nullable_column_large_no_suggestion() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "bio".into(),
            data_type: "text".into(),
            has_default: false,
            is_not_null: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_alter_column_type_huge() {
        let op = DdlOperation::AlterTableAlterColumn {
            table: "users".into(),
            column: "name".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Huge);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("backfill"));
    }

    #[test]
    fn test_suggestion_alter_column_type_small_no_suggestion() {
        let op = DdlOperation::AlterTableAlterColumn {
            table: "users".into(),
            column: "name".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Small);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_drop_table() {
        let op = DdlOperation::DropTable {
            table: "users".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Small);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("soft-delete"));
    }

    #[test]
    fn test_suggestion_drop_column() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "users".into(),
            column: "email".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Medium);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("soft-delete"));
    }

    #[test]
    fn test_suggestion_truncate() {
        let op = DdlOperation::TruncateTable {
            table: "logs".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Huge);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("DELETE with batching"));
    }

    // ── Affected table extraction ─────────────────────────────────────

    #[test]
    fn test_affected_table_create_table() {
        let op = DdlOperation::CreateTable {
            table: "orders".into(),
            if_not_exists: false,
        };
        assert_eq!(affected_table(&op), Some("orders".into()));
    }

    #[test]
    fn test_affected_table_create_view_is_none() {
        let op = DdlOperation::CreateView {
            name: "v_stats".into(),
            is_materialized: false,
        };
        assert_eq!(affected_table(&op), None);
    }

    #[test]
    fn test_affected_table_create_function_is_none() {
        let op = DdlOperation::CreateFunction {
            name: "my_func".into(),
        };
        assert_eq!(affected_table(&op), None);
    }

    #[test]
    fn test_affected_table_other_is_none() {
        let op = DdlOperation::Other {
            statement_preview: "GRANT SELECT ON ...".into(),
        };
        assert_eq!(affected_table(&op), None);
    }

    // ── Display impls ─────────────────────────────────────────────────

    #[test]
    fn test_lock_level_display() {
        assert_eq!(LockLevel::None.to_string(), "None");
        assert_eq!(LockLevel::AccessShareLock.to_string(), "ACCESS SHARE");
        assert_eq!(LockLevel::RowShareLock.to_string(), "ROW SHARE");
        assert_eq!(LockLevel::RowExclusiveLock.to_string(), "ROW EXCLUSIVE");
        assert_eq!(
            LockLevel::ShareUpdateExclusiveLock.to_string(),
            "SHARE UPDATE EXCLUSIVE"
        );
        assert_eq!(LockLevel::ShareLock.to_string(), "SHARE");
        assert_eq!(
            LockLevel::ShareRowExclusiveLock.to_string(),
            "SHARE ROW EXCLUSIVE"
        );
        assert_eq!(LockLevel::ExclusiveLock.to_string(), "EXCLUSIVE");
        assert_eq!(
            LockLevel::AccessExclusiveLock.to_string(),
            "ACCESS EXCLUSIVE"
        );
    }

    #[test]
    fn test_safety_verdict_display() {
        assert_eq!(SafetyVerdict::Safe.to_string(), "SAFE");
        assert_eq!(SafetyVerdict::Caution.to_string(), "CAUTION");
        assert_eq!(SafetyVerdict::Danger.to_string(), "DANGER");
    }

    #[test]
    fn test_table_size_display() {
        assert_eq!(TableSize::Small.to_string(), "Small (<10k rows)");
        assert_eq!(TableSize::Medium.to_string(), "Medium (10k-1M rows)");
        assert_eq!(TableSize::Large.to_string(), "Large (1M-100M rows)");
        assert_eq!(TableSize::Huge.to_string(), "Huge (>100M rows)");
    }

    // ── Row count classification ──────────────────────────────────────

    #[test]
    fn test_classify_row_count_small() {
        assert_eq!(
            classify_row_count(0, 1_000_000, 100_000_000),
            TableSize::Small
        );
        assert_eq!(
            classify_row_count(9_999, 1_000_000, 100_000_000),
            TableSize::Small
        );
    }

    #[test]
    fn test_classify_row_count_medium() {
        assert_eq!(
            classify_row_count(10_000, 1_000_000, 100_000_000),
            TableSize::Medium
        );
        assert_eq!(
            classify_row_count(500_000, 1_000_000, 100_000_000),
            TableSize::Medium
        );
        assert_eq!(
            classify_row_count(1_000_000, 1_000_000, 100_000_000),
            TableSize::Medium
        );
    }

    #[test]
    fn test_classify_row_count_large() {
        assert_eq!(
            classify_row_count(1_000_001, 1_000_000, 100_000_000),
            TableSize::Large
        );
        assert_eq!(
            classify_row_count(50_000_000, 1_000_000, 100_000_000),
            TableSize::Large
        );
        assert_eq!(
            classify_row_count(100_000_000, 1_000_000, 100_000_000),
            TableSize::Large
        );
    }

    #[test]
    fn test_classify_row_count_huge() {
        assert_eq!(
            classify_row_count(100_000_001, 1_000_000, 100_000_000),
            TableSize::Huge
        );
        assert_eq!(
            classify_row_count(1_000_000_000, 1_000_000, 100_000_000),
            TableSize::Huge
        );
    }

    #[test]
    fn test_classify_custom_thresholds() {
        // With lower thresholds: large=1_000, huge=10_000
        assert_eq!(classify_row_count(500, 1_000, 10_000), TableSize::Small);
        assert_eq!(classify_row_count(1_001, 1_000, 10_000), TableSize::Large);
        assert_eq!(classify_row_count(10_000, 1_000, 10_000), TableSize::Large);
        assert_eq!(classify_row_count(10_001, 1_000, 10_000), TableSize::Huge);
    }

    // ── SafetyConfig defaults ─────────────────────────────────────────

    #[test]
    fn test_safety_config_defaults() {
        let config = SafetyConfig::default();
        assert!(config.enabled);
        assert!(!config.block_on_danger);
        assert_eq!(config.large_table_threshold, 1_000_000);
        assert_eq!(config.huge_table_threshold, 100_000_000);
    }
}
