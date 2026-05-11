//! Schema advisor: proactive suggestions for schema improvements.
//!
//! Analyzes the live database schema and produces actionable advisories
//! with generated fix SQL. Rule IDs are namespaced per engine: `A001`-`A010`
//! are PostgreSQL rules, `M001`-`M005` are MySQL rules.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::db::{quote_ident, DbClient};
use crate::dialect::DialectKind;
use crate::error::Result;

/// Configuration for the schema advisor.
#[derive(Debug, Clone, Default)]
pub struct AdvisorConfig {
    /// Whether to run the advisor after migrations.
    pub run_after_migrate: bool,
    /// List of rule IDs to disable (e.g., ["A003", "A006"]).
    pub disabled_rules: Vec<String>,
}

/// Severity of an advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AdvisorySeverity {
    Info,
    Suggestion,
    Warning,
}

impl std::fmt::Display for AdvisorySeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Suggestion => write!(f, "suggestion"),
            Self::Warning => write!(f, "warning"),
        }
    }
}

/// A single advisory finding.
#[derive(Debug, Clone, Serialize)]
pub struct Advisory {
    /// Rule ID (e.g., "A001").
    pub rule_id: String,
    /// Category of the advisory.
    pub category: String,
    /// Severity level.
    pub severity: AdvisorySeverity,
    /// Affected database object (e.g., "users.email", "idx_name").
    pub object: String,
    /// Human-readable explanation of the issue.
    pub explanation: String,
    /// Generated SQL to fix the issue.
    pub fix_sql: Option<String>,
}

/// Report from the schema advisor.
#[derive(Debug, Clone, Serialize)]
pub struct AdvisorReport {
    /// Schema that was analyzed.
    pub schema: String,
    /// All advisory findings.
    pub advisories: Vec<Advisory>,
    /// Count of warnings.
    pub warning_count: usize,
    /// Count of suggestions.
    pub suggestion_count: usize,
    /// Count of info items.
    pub info_count: usize,
}

/// Run all advisory rules against the database schema (dialect-aware entry).
pub async fn analyze_db(
    client: &DbClient,
    schema: &str,
    config: &AdvisorConfig,
) -> Result<AdvisorReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => analyze(client.as_postgres()?, schema, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(crate::error::WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => analyze_mysql(client, schema, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(crate::error::WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}

/// Run all advisory rules against the database schema (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn analyze(
    client: &Client,
    schema: &str,
    config: &AdvisorConfig,
) -> Result<AdvisorReport> {
    let mut advisories = Vec::new();

    if !config.disabled_rules.contains(&"A001".to_string()) {
        advisories.extend(check_a001_fk_without_index(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A002".to_string()) {
        advisories.extend(check_a002_unused_indexes(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A003".to_string()) {
        advisories.extend(check_a003_timestamp_without_tz(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A004".to_string()) {
        advisories.extend(check_a004_table_without_pk(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A005".to_string()) {
        advisories.extend(check_a005_nullable_all_nonnull(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A006".to_string()) {
        advisories.extend(check_a006_varchar_without_limit(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A007".to_string()) {
        advisories.extend(check_a007_duplicate_indexes(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A008".to_string()) {
        advisories.extend(check_a008_seq_scan_large_table(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A009".to_string()) {
        advisories.extend(check_a009_large_enum(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A010".to_string()) {
        advisories.extend(check_a010_orphaned_sequences(client, schema).await?);
    }

    let warning_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Warning)
        .count();
    let suggestion_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Suggestion)
        .count();
    let info_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Info)
        .count();

    Ok(AdvisorReport {
        schema: schema.to_string(),
        advisories,
        warning_count,
        suggestion_count,
        info_count,
    })
}

/// Generate combined fix SQL from all advisories.
pub fn generate_fix_sql(report: &AdvisorReport) -> String {
    let fixes: Vec<String> = report
        .advisories
        .iter()
        .filter_map(|a| {
            a.fix_sql.as_ref().map(|sql| {
                format!(
                    "-- {} [{}]: {}\n{}",
                    a.rule_id, a.severity, a.explanation, sql
                )
            })
        })
        .collect();
    fixes.join("\n\n")
}

// ── A001: Foreign key column missing index ──

#[cfg(feature = "postgres")]
async fn check_a001_fk_without_index(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            tc.table_name,
            kcu.column_name,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1
            AND NOT EXISTS (
                SELECT 1 FROM pg_indexes pi
                WHERE pi.schemaname = $1
                    AND pi.tablename = tc.table_name
                    AND pi.indexdef LIKE '%' || kcu.column_name || '%'
            )
        ORDER BY tc.table_name, kcu.column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let column: String = r.get(1);
            Advisory {
                rule_id: "A001".to_string(),
                category: "Performance".to_string(),
                severity: AdvisorySeverity::Warning,
                object: format!("{}.{}", table, column),
                explanation: format!(
                    "Foreign key column {}.{} has no index, which can cause slow joins and constraint checks",
                    table, column
                ),
                fix_sql: Some(format!(
                    "CREATE INDEX idx_{}_{} ON {} ({});",
                    table, column,
                    quote_ident(&table),
                    quote_ident(&column)
                )),
            }
        })
        .collect())
}

// ── A002: Unused indexes ──

#[cfg(feature = "postgres")]
async fn check_a002_unused_indexes(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            s.indexrelname AS index_name,
            s.relname AS table_name,
            s.idx_scan AS scans
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        WHERE s.schemaname = $1
            AND s.idx_scan = 0
            AND NOT i.indisprimary
            AND NOT i.indisunique
        ORDER BY s.relname, s.indexrelname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let index_name: String = r.get(0);
            let table_name: String = r.get(1);
            Advisory {
                rule_id: "A002".to_string(),
                category: "Performance".to_string(),
                severity: AdvisorySeverity::Suggestion,
                object: index_name.clone(),
                explanation: format!(
                    "Index {} on {} has never been used (0 scans). Consider removing it to reduce write overhead",
                    index_name, table_name
                ),
                fix_sql: Some(format!("DROP INDEX {};", quote_ident(&index_name))),
            }
        })
        .collect())
}

// ── A003: TIMESTAMP without timezone ──

#[cfg(feature = "postgres")]
async fn check_a003_timestamp_without_tz(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = $1
            AND data_type = 'timestamp without time zone'
        ORDER BY table_name, column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let column: String = r.get(1);
            Advisory {
                rule_id: "A003".to_string(),
                category: "Correctness".to_string(),
                severity: AdvisorySeverity::Warning,
                object: format!("{}.{}", table, column),
                explanation: format!(
                    "Column {}.{} uses TIMESTAMP WITHOUT TIME ZONE. Use TIMESTAMPTZ to avoid timezone ambiguity",
                    table, column
                ),
                fix_sql: Some(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE TIMESTAMPTZ;",
                    quote_ident(&table),
                    quote_ident(&column)
                )),
            }
        })
        .collect())
}

// ── A004: Table without primary key ──

#[cfg(feature = "postgres")]
async fn check_a004_table_without_pk(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
            AND NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints tc
                WHERE tc.table_schema = $1
                    AND tc.table_name = t.table_name
                    AND tc.constraint_type = 'PRIMARY KEY'
            )
        ORDER BY t.table_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            Advisory {
                rule_id: "A004".to_string(),
                category: "Correctness".to_string(),
                severity: AdvisorySeverity::Warning,
                object: table.clone(),
                explanation: format!(
                    "Table {} has no primary key. This prevents logical replication and makes row identification unreliable",
                    table
                ),
                fix_sql: None, // Can't auto-generate a PK without knowing the table
            }
        })
        .collect())
}

// ── A005: Nullable column where all values are non-null ──

#[cfg(feature = "postgres")]
async fn check_a005_nullable_all_nonnull(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    // Only check tables with at least 100 rows to avoid false positives on empty tables
    let sql = r#"
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN pg_stat_user_tables s
            ON c.table_name = s.relname AND s.schemaname = $1
        WHERE c.table_schema = $1
            AND c.is_nullable = 'YES'
            AND s.n_live_tup > 100
            AND c.column_default IS NULL
        ORDER BY c.table_name, c.column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    let mut advisories = Vec::new();

    for row in &rows {
        let table: String = row.get(0);
        let column: String = row.get(1);

        // Check if any nulls actually exist
        let null_check = format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1)",
            quote_ident(&table),
            quote_ident(&column)
        );
        if let Ok(result) = client.query_one(&null_check, &[]).await {
            let has_nulls: bool = result.get(0);
            if !has_nulls {
                advisories.push(Advisory {
                    rule_id: "A005".to_string(),
                    category: "Correctness".to_string(),
                    severity: AdvisorySeverity::Info,
                    object: format!("{}.{}", table, column),
                    explanation: format!(
                        "Column {}.{} is nullable but contains no NULL values. Consider adding NOT NULL constraint",
                        table, column
                    ),
                    fix_sql: Some(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        quote_ident(&table),
                        quote_ident(&column)
                    )),
                });
            }
        }
    }

    Ok(advisories)
}

// ── A006: VARCHAR without length limit ──

#[cfg(feature = "postgres")]
async fn check_a006_varchar_without_limit(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = $1
            AND data_type = 'character varying'
            AND character_maximum_length IS NULL
        ORDER BY table_name, column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let column: String = r.get(1);
            Advisory {
                rule_id: "A006".to_string(),
                category: "Design".to_string(),
                severity: AdvisorySeverity::Info,
                object: format!("{}.{}", table, column),
                explanation: format!(
                    "Column {}.{} is VARCHAR without length limit. Consider using TEXT or adding a length constraint",
                    table, column
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A007: Duplicate indexes ──

#[cfg(feature = "postgres")]
async fn check_a007_duplicate_indexes(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            a.indexname AS index_a,
            b.indexname AS index_b,
            a.tablename
        FROM pg_indexes a
        JOIN pg_indexes b
            ON a.tablename = b.tablename
            AND a.schemaname = b.schemaname
            AND a.indexname < b.indexname
            AND a.indexdef = b.indexdef
        WHERE a.schemaname = $1
        ORDER BY a.tablename, a.indexname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let index_a: String = r.get(0);
            let index_b: String = r.get(1);
            let table: String = r.get(2);
            Advisory {
                rule_id: "A007".to_string(),
                category: "Design".to_string(),
                severity: AdvisorySeverity::Warning,
                object: format!("{}, {}", index_a, index_b),
                explanation: format!(
                    "Indexes {} and {} on table {} have identical definitions. Remove the duplicate",
                    index_a, index_b, table
                ),
                fix_sql: Some(format!("DROP INDEX {};", quote_ident(&index_b))),
            }
        })
        .collect())
}

// ── A008: Sequential scan on large table ──

#[cfg(feature = "postgres")]
async fn check_a008_seq_scan_large_table(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            relname,
            seq_scan,
            n_live_tup
        FROM pg_stat_user_tables
        WHERE schemaname = $1
            AND n_live_tup > 100000
            AND seq_scan > 0
            AND seq_scan > idx_scan
        ORDER BY seq_scan DESC
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let seq_scans: i64 = r.get(1);
            let row_count: i64 = r.get(2);
            Advisory {
                rule_id: "A008".to_string(),
                category: "Performance".to_string(),
                severity: AdvisorySeverity::Warning,
                object: table.clone(),
                explanation: format!(
                    "Table {} (~{} rows) has {} sequential scans exceeding index scans. Consider adding indexes",
                    table, row_count, seq_scans
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A009: Enum with >20 values ──

#[cfg(feature = "postgres")]
async fn check_a009_large_enum(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT t.typname, count(e.enumlabel)::int AS label_count
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = $1
        GROUP BY t.typname
        HAVING count(e.enumlabel) > 20
        ORDER BY t.typname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            let count: i32 = r.get(1);
            Advisory {
                rule_id: "A009".to_string(),
                category: "Design".to_string(),
                severity: AdvisorySeverity::Suggestion,
                object: name.clone(),
                explanation: format!(
                    "Enum type {} has {} values. Enums with many values are hard to maintain; consider a lookup table",
                    name, count
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A010: Orphaned sequences ──

#[cfg(feature = "postgres")]
async fn check_a010_orphaned_sequences(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT s.relname
        FROM pg_class s
        JOIN pg_namespace n ON n.oid = s.relnamespace
        WHERE s.relkind = 'S'
            AND n.nspname = $1
            AND NOT EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = s.oid
                    AND d.deptype IN ('a', 'i')
            )
        ORDER BY s.relname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            Advisory {
                rule_id: "A010".to_string(),
                category: "Correctness".to_string(),
                severity: AdvisorySeverity::Suggestion,
                object: name.clone(),
                explanation: format!(
                    "Sequence {} is not attached to any column. It may be orphaned",
                    name
                ),
                fix_sql: Some(format!("DROP SEQUENCE IF EXISTS {};", quote_ident(&name))),
            }
        })
        .collect())
}

// ── MySQL rules ─────────────────────────────────────────────────────────────
//
// Rule numbering is namespaced: A001-A010 are PostgreSQL rules above, M001-M005
// are MySQL rules below. They share the same Advisory / AdvisorReport types so
// JSON consumers can ignore the dialect.

/// Run all advisory rules against a MySQL schema.
///
/// Current rule set:
/// - M001: foreign key column without an index (analog of A001)
/// - M002: table without a primary key (analog of A004)
/// - M003: non-utf8mb4 charset in use (MySQL-specific)
/// - M004: non-InnoDB storage engine (MyISAM etc.) — risky for transactions
/// - M005: duplicate indexes on the same columns (analog of A007)
#[cfg(feature = "mysql")]
pub async fn analyze_mysql(
    client: &DbClient,
    schema: &str,
    config: &AdvisorConfig,
) -> Result<AdvisorReport> {
    let mut advisories = Vec::new();
    if !config.disabled_rules.contains(&"M001".to_string()) {
        advisories.extend(check_m001_fk_without_index_mysql(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M002".to_string()) {
        advisories.extend(check_m002_table_without_pk_mysql(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M003".to_string()) {
        advisories.extend(check_m003_non_utf8mb4_charset(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M004".to_string()) {
        advisories.extend(check_m004_non_innodb_engine(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M005".to_string()) {
        advisories.extend(check_m005_duplicate_indexes_mysql(client, schema).await?);
    }

    let warning_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Warning)
        .count();
    let suggestion_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Suggestion)
        .count();
    let info_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Info)
        .count();

    Ok(AdvisorReport {
        schema: schema.to_string(),
        advisories,
        warning_count,
        suggestion_count,
        info_count,
    })
}

// ── M001: Foreign key column missing index ──
#[cfg(feature = "mysql")]
async fn check_m001_fk_without_index_mysql(
    client: &DbClient,
    schema: &str,
) -> Result<Vec<Advisory>> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    // Foreign keys whose first column has no covering index. We approximate
    // by joining KEY_COLUMN_USAGE (FK columns) against STATISTICS (indexed
    // columns), filtering FKs whose column doesn't appear as the FIRST column
    // of any index. MySQL automatically creates an index on FK columns at FK
    // creation time — but a later DROP INDEX can leave the FK without one.
    let rows: Vec<(String, String, String)> = conn
        .exec(
            "SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.CONSTRAINT_NAME \
             FROM information_schema.KEY_COLUMN_USAGE kcu \
             WHERE kcu.TABLE_SCHEMA = ? \
               AND kcu.REFERENCED_TABLE_NAME IS NOT NULL \
               AND NOT EXISTS ( \
                 SELECT 1 FROM information_schema.STATISTICS s \
                 WHERE s.TABLE_SCHEMA = kcu.TABLE_SCHEMA \
                   AND s.TABLE_NAME = kcu.TABLE_NAME \
                   AND s.COLUMN_NAME = kcu.COLUMN_NAME \
                   AND s.SEQ_IN_INDEX = 1 \
               ) \
             ORDER BY kcu.TABLE_NAME, kcu.COLUMN_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, column, _constraint)| Advisory {
            rule_id: "M001".to_string(),
            category: "Performance".to_string(),
            severity: AdvisorySeverity::Warning,
            object: format!("{}.{}", table, column),
            explanation: format!(
                "Foreign key column {}.{} has no covering index — joins and FK \
                 constraint checks will perform a full table scan",
                table, column
            ),
            fix_sql: Some(format!(
                "CREATE INDEX `idx_{table}_{column}` ON `{table}` (`{column}`);"
            )),
        })
        .collect())
}

// ── M002: Table without primary key ──
#[cfg(feature = "mysql")]
async fn check_m002_table_without_pk_mysql(
    client: &DbClient,
    schema: &str,
) -> Result<Vec<Advisory>> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    let rows: Vec<String> = conn
        .exec(
            "SELECT t.TABLE_NAME FROM information_schema.TABLES t \
             WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM information_schema.TABLE_CONSTRAINTS tc \
                 WHERE tc.TABLE_SCHEMA = t.TABLE_SCHEMA \
                   AND tc.TABLE_NAME = t.TABLE_NAME \
                   AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
               ) \
             ORDER BY t.TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|table| Advisory {
            rule_id: "M002".to_string(),
            category: "Correctness".to_string(),
            severity: AdvisorySeverity::Warning,
            object: table.clone(),
            explanation: format!(
                "Table {} has no primary key — InnoDB will create a hidden \
                 6-byte rowid index, replication and crash recovery suffer",
                table
            ),
            fix_sql: None, // can't auto-fix without knowing the right column
        })
        .collect())
}

// ── M003: Non-utf8mb4 charset ──
#[cfg(feature = "mysql")]
async fn check_m003_non_utf8mb4_charset(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    // Tables whose default charset isn't utf8mb4. utf8 (3-byte) is a frequent
    // legacy footgun that can't store 4-byte characters (emoji, some Asian
    // scripts) and may surface as silent data corruption.
    let rows: Vec<(String, String)> = conn
        .exec(
            "SELECT t.TABLE_NAME, ccsa.CHARACTER_SET_NAME \
             FROM information_schema.TABLES t \
             JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa \
               ON ccsa.COLLATION_NAME = t.TABLE_COLLATION \
             WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' \
               AND ccsa.CHARACTER_SET_NAME <> 'utf8mb4' \
             ORDER BY t.TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, charset)| Advisory {
            rule_id: "M003".to_string(),
            category: "Correctness".to_string(),
            severity: AdvisorySeverity::Suggestion,
            object: table.clone(),
            explanation: format!(
                "Table {} uses charset '{}' — utf8mb4 is the modern default \
                 and supports the full Unicode range (4-byte chars)",
                table, charset
            ),
            fix_sql: Some(format!(
                "ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 \
                 COLLATE utf8mb4_0900_ai_ci;"
            )),
        })
        .collect())
}

// ── M004: Non-InnoDB storage engine ──
#[cfg(feature = "mysql")]
async fn check_m004_non_innodb_engine(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
               AND ENGINE IS NOT NULL AND ENGINE <> 'InnoDB' \
             ORDER BY TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, engine)| Advisory {
            rule_id: "M004".to_string(),
            category: "Correctness".to_string(),
            severity: AdvisorySeverity::Warning,
            object: table.clone(),
            explanation: format!(
                "Table {} uses storage engine '{}' — InnoDB is the modern \
                 default and the only engine with full transaction + crash-\
                 recovery support",
                table, engine
            ),
            fix_sql: Some(format!("ALTER TABLE `{table}` ENGINE = InnoDB;")),
        })
        .collect())
}

// ── M005: Duplicate indexes ──
#[cfg(feature = "mysql")]
async fn check_m005_duplicate_indexes_mysql(
    client: &DbClient,
    schema: &str,
) -> Result<Vec<Advisory>> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    // Indexes that index exactly the same column sequence on the same table.
    // We group by (table, column-sequence) and emit an advisory when more
    // than one index name appears in a group. Concatenating column names with
    // a delimiter is a coarse fingerprint; for the same physical leaf it's
    // sufficient.
    let rows: Vec<(String, String, i64)> = conn
        .exec(
            "SELECT TABLE_NAME, GROUP_CONCAT(INDEX_NAME ORDER BY INDEX_NAME), COUNT(*) \
             FROM ( \
                 SELECT TABLE_NAME, INDEX_NAME, \
                        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = ? \
                 GROUP BY TABLE_NAME, INDEX_NAME \
             ) g \
             GROUP BY TABLE_NAME, cols \
             HAVING COUNT(*) > 1 \
             ORDER BY TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, names, count)| Advisory {
            rule_id: "M005".to_string(),
            category: "Performance".to_string(),
            severity: AdvisorySeverity::Suggestion,
            object: format!("{}: {}", table, names),
            explanation: format!(
                "Table {} has {} indexes ({}) covering the same columns — \
                 drop the redundant ones to reduce write amplification and \
                 storage",
                table, count, names
            ),
            fix_sql: None,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_advisor_config_default() {
        let config = AdvisorConfig::default();
        assert!(!config.run_after_migrate);
        assert!(config.disabled_rules.is_empty());
    }

    #[test]
    fn test_generate_fix_sql_empty() {
        let report = AdvisorReport {
            schema: "public".to_string(),
            advisories: vec![],
            warning_count: 0,
            suggestion_count: 0,
            info_count: 0,
        };
        assert!(generate_fix_sql(&report).is_empty());
    }

    #[test]
    fn test_generate_fix_sql_with_advisories() {
        let report = AdvisorReport {
            schema: "public".to_string(),
            advisories: vec![
                Advisory {
                    rule_id: "A001".to_string(),
                    category: "Performance".to_string(),
                    severity: AdvisorySeverity::Warning,
                    object: "orders.user_id".to_string(),
                    explanation: "FK without index".to_string(),
                    fix_sql: Some(
                        "CREATE INDEX idx_orders_user_id ON \"orders\" (\"user_id\");".to_string(),
                    ),
                },
                Advisory {
                    rule_id: "A004".to_string(),
                    category: "Correctness".to_string(),
                    severity: AdvisorySeverity::Warning,
                    object: "logs".to_string(),
                    explanation: "No primary key".to_string(),
                    fix_sql: None,
                },
            ],
            warning_count: 2,
            suggestion_count: 0,
            info_count: 0,
        };
        let sql = generate_fix_sql(&report);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("A001"));
        assert!(!sql.contains("A004")); // No fix SQL for A004
    }

    #[test]
    fn test_advisory_severity_display() {
        assert_eq!(AdvisorySeverity::Info.to_string(), "info");
        assert_eq!(AdvisorySeverity::Suggestion.to_string(), "suggestion");
        assert_eq!(AdvisorySeverity::Warning.to_string(), "warning");
    }
}
