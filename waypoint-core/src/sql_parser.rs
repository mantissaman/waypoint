//! Lightweight regex-based DDL extraction from SQL content.
//!
//! Used by lint, changelog, and conflict detection features.

use std::sync::LazyLock;

use regex_lite::Regex;
use serde::Serialize;

/// A DDL operation extracted from SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum DdlOperation {
    /// A CREATE TABLE statement.
    CreateTable {
        /// Name of the table being created.
        table: String,
        /// Whether the statement includes IF NOT EXISTS.
        if_not_exists: bool,
    },
    /// A DROP TABLE statement.
    DropTable {
        /// Name of the table being dropped.
        table: String,
    },
    /// An ALTER TABLE ... ADD COLUMN statement.
    AlterTableAddColumn {
        /// Name of the table being altered.
        table: String,
        /// Name of the column being added.
        column: String,
        /// Data type of the new column.
        data_type: String,
        /// Whether the column has a DEFAULT expression.
        has_default: bool,
        /// Whether the column has a NOT NULL constraint.
        is_not_null: bool,
    },
    /// An ALTER TABLE ... DROP COLUMN statement.
    AlterTableDropColumn {
        /// Name of the table being altered.
        table: String,
        /// Name of the column being dropped.
        column: String,
    },
    /// An ALTER TABLE ... ALTER COLUMN statement.
    AlterTableAlterColumn {
        /// Name of the table being altered.
        table: String,
        /// Name of the column being modified.
        column: String,
    },
    /// A CREATE INDEX statement.
    CreateIndex {
        /// Name of the index being created.
        name: String,
        /// Name of the table the index is on.
        table: String,
        /// Whether the index is created CONCURRENTLY.
        is_concurrent: bool,
        /// Whether this is a UNIQUE index.
        is_unique: bool,
    },
    /// A DROP INDEX statement.
    DropIndex {
        /// Name of the index being dropped.
        name: String,
    },
    /// A CREATE VIEW or CREATE MATERIALIZED VIEW statement.
    CreateView {
        /// Name of the view being created.
        name: String,
        /// Whether this is a materialized view.
        is_materialized: bool,
    },
    /// A DROP VIEW statement.
    DropView {
        /// Name of the view being dropped.
        name: String,
    },
    /// A CREATE FUNCTION statement.
    CreateFunction {
        /// Name of the function being created.
        name: String,
    },
    /// A DROP FUNCTION statement.
    DropFunction {
        /// Name of the function being dropped.
        name: String,
    },
    /// An ALTER TABLE ... ADD CONSTRAINT statement.
    AddConstraint {
        /// Name of the table the constraint is added to.
        table: String,
        /// Type of constraint (e.g. PRIMARY KEY, UNIQUE, FOREIGN KEY).
        constraint_type: String,
    },
    /// An ALTER TABLE ... DROP CONSTRAINT statement.
    DropConstraint {
        /// Name of the table the constraint is dropped from.
        table: String,
        /// Name of the constraint being dropped.
        name: String,
    },
    /// A CREATE TYPE ... AS ENUM statement.
    CreateEnum {
        /// Name of the enum type being created.
        name: String,
    },
    /// A TRUNCATE TABLE statement.
    TruncateTable {
        /// Name of the table being truncated.
        table: String,
    },
    /// Any other SQL statement that does not match known DDL patterns.
    Other {
        /// Truncated preview of the unrecognized statement.
        statement_preview: String,
    },
}

impl std::fmt::Display for DdlOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                if *if_not_exists {
                    write!(f, "CREATE TABLE IF NOT EXISTS {}", table)
                } else {
                    write!(f, "CREATE TABLE {}", table)
                }
            }
            DdlOperation::DropTable { table } => write!(f, "DROP TABLE {}", table),
            DdlOperation::AlterTableAddColumn {
                table,
                column,
                data_type,
                ..
            } => {
                write!(
                    f,
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    table, column, data_type
                )
            }
            DdlOperation::AlterTableDropColumn { table, column } => {
                write!(f, "ALTER TABLE {} DROP COLUMN {}", table, column)
            }
            DdlOperation::AlterTableAlterColumn { table, column } => {
                write!(f, "ALTER TABLE {} ALTER COLUMN {}", table, column)
            }
            DdlOperation::CreateIndex {
                name,
                table,
                is_unique,
                is_concurrent,
            } => {
                let unique = if *is_unique { "UNIQUE " } else { "" };
                let concurrent = if *is_concurrent { "CONCURRENTLY " } else { "" };
                write!(
                    f,
                    "CREATE {}{}INDEX {} ON {}",
                    unique, concurrent, name, table
                )
            }
            DdlOperation::DropIndex { name } => write!(f, "DROP INDEX {}", name),
            DdlOperation::CreateView {
                name,
                is_materialized,
            } => {
                if *is_materialized {
                    write!(f, "CREATE MATERIALIZED VIEW {}", name)
                } else {
                    write!(f, "CREATE VIEW {}", name)
                }
            }
            DdlOperation::DropView { name } => write!(f, "DROP VIEW {}", name),
            DdlOperation::CreateFunction { name } => write!(f, "CREATE FUNCTION {}", name),
            DdlOperation::DropFunction { name } => write!(f, "DROP FUNCTION {}", name),
            DdlOperation::AddConstraint {
                table,
                constraint_type,
            } => {
                write!(
                    f,
                    "ALTER TABLE {} ADD {} CONSTRAINT",
                    table, constraint_type
                )
            }
            DdlOperation::DropConstraint { table, name } => {
                write!(f, "ALTER TABLE {} DROP CONSTRAINT {}", table, name)
            }
            DdlOperation::CreateEnum { name } => write!(f, "CREATE TYPE {} AS ENUM", name),
            DdlOperation::TruncateTable { table } => write!(f, "TRUNCATE TABLE {}", table),
            DdlOperation::Other { statement_preview } => write!(f, "{}", statement_preview),
        }
    }
}

// Regex patterns for DDL extraction
static CREATE_TABLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static DROP_TABLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static ALTER_TABLE_ADD_COLUMN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ADD\s+(?:COLUMN\s+)?(\w+)\s+(\w[\w\s\(\),]*)",
    )
    .unwrap()
});

static ALTER_TABLE_DROP_COLUMN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+DROP\s+(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?(\w+)",
    )
    .unwrap()
});

static ALTER_TABLE_ALTER_COLUMN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ALTER\s+(?:COLUMN\s+)?(\w+)").unwrap()
});

static CREATE_INDEX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+(UNIQUE\s+)?INDEX\s+(CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(?:(\w+)\.)?(\w+)").unwrap()
});

static DROP_INDEX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)")
        .unwrap()
});

static CREATE_VIEW_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(MATERIALIZED\s+)?VIEW\s+(?:(\w+)\.)?(\w+)")
        .unwrap()
});

static DROP_VIEW_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+(MATERIALIZED\s+)?VIEW\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static CREATE_FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:(\w+)\.)?(\w+)").unwrap()
});

static DROP_FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)").unwrap()
});

static ADD_CONSTRAINT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ADD\s+(?:CONSTRAINT\s+\w+\s+)?(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|EXCLUDE)").unwrap()
});

static DROP_CONSTRAINT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?(\w+)",
    )
    .unwrap()
});

static CREATE_ENUM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)CREATE\s+TYPE\s+(?:(\w+)\.)?(\w+)\s+AS\s+ENUM").unwrap());

static TRUNCATE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)TRUNCATE\s+(?:TABLE\s+)?(?:(\w+)\.)?(\w+)").unwrap());

/// Extract DDL operations from SQL content.
pub fn extract_ddl_operations(sql: &str) -> Vec<DdlOperation> {
    let statements = split_statements(sql);
    let mut ops = Vec::new();

    for stmt in statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(op) = parse_statement(trimmed) {
            ops.push(op);
        }
    }

    ops
}

fn parse_statement(stmt: &str) -> Option<DdlOperation> {
    // Order matters — more specific patterns first

    // ALTER TABLE ... ADD CONSTRAINT (before ADD COLUMN)
    if let Some(caps) = ADD_CONSTRAINT_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let constraint_type = caps.get(3).unwrap().as_str().to_uppercase();
        return Some(DdlOperation::AddConstraint {
            table,
            constraint_type,
        });
    }

    // ALTER TABLE ... DROP CONSTRAINT (before DROP COLUMN)
    if let Some(caps) = DROP_CONSTRAINT_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let name = caps.get(3).unwrap().as_str().to_string();
        return Some(DdlOperation::DropConstraint { table, name });
    }

    // ALTER TABLE ... ALTER COLUMN (before ADD/DROP COLUMN)
    if ALTER_TABLE_ALTER_COLUMN_RE.is_match(stmt) {
        if let Some(caps) = ALTER_TABLE_ALTER_COLUMN_RE.captures(stmt) {
            let table = caps.get(2).unwrap().as_str().to_string();
            let column = caps.get(3).unwrap().as_str().to_string();
            return Some(DdlOperation::AlterTableAlterColumn { table, column });
        }
    }

    // ALTER TABLE ... DROP COLUMN
    if let Some(caps) = ALTER_TABLE_DROP_COLUMN_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let column = caps.get(3).unwrap().as_str().to_string();
        return Some(DdlOperation::AlterTableDropColumn { table, column });
    }

    // ALTER TABLE ... ADD COLUMN
    if let Some(caps) = ALTER_TABLE_ADD_COLUMN_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        let column = caps.get(3).unwrap().as_str().to_string();
        let rest = caps.get(4).unwrap().as_str();
        // Extract data type (first word)
        let data_type = rest
            .split_whitespace()
            .next()
            .unwrap_or("unknown")
            .to_string();
        let upper = stmt.to_uppercase();
        let has_default = upper.contains("DEFAULT");
        let is_not_null = upper.contains("NOT NULL");
        return Some(DdlOperation::AlterTableAddColumn {
            table,
            column,
            data_type,
            has_default,
            is_not_null,
        });
    }

    // CREATE TABLE
    if let Some(caps) = CREATE_TABLE_RE.captures(stmt) {
        let if_not_exists = caps.get(1).is_some();
        let table = caps.get(3).unwrap().as_str().to_string();
        return Some(DdlOperation::CreateTable {
            table,
            if_not_exists,
        });
    }

    // DROP TABLE
    if let Some(caps) = DROP_TABLE_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        return Some(DdlOperation::DropTable { table });
    }

    // CREATE INDEX
    if let Some(caps) = CREATE_INDEX_RE.captures(stmt) {
        let is_unique = caps.get(1).is_some();
        let is_concurrent = caps.get(2).is_some();
        let name = caps.get(3).unwrap().as_str().to_string();
        let table = caps.get(5).unwrap().as_str().to_string();
        return Some(DdlOperation::CreateIndex {
            name,
            table,
            is_concurrent,
            is_unique,
        });
    }

    // DROP INDEX
    if let Some(caps) = DROP_INDEX_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return Some(DdlOperation::DropIndex { name });
    }

    // CREATE [MATERIALIZED] VIEW
    if let Some(caps) = CREATE_VIEW_RE.captures(stmt) {
        let is_materialized = caps.get(1).is_some();
        let name = caps.get(3).unwrap().as_str().to_string();
        return Some(DdlOperation::CreateView {
            name,
            is_materialized,
        });
    }

    // DROP VIEW
    if let Some(caps) = DROP_VIEW_RE.captures(stmt) {
        let name = caps.get(3).unwrap().as_str().to_string();
        return Some(DdlOperation::DropView { name });
    }

    // CREATE FUNCTION
    if let Some(caps) = CREATE_FUNCTION_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return Some(DdlOperation::CreateFunction { name });
    }

    // DROP FUNCTION
    if let Some(caps) = DROP_FUNCTION_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return Some(DdlOperation::DropFunction { name });
    }

    // CREATE TYPE ... AS ENUM
    if let Some(caps) = CREATE_ENUM_RE.captures(stmt) {
        let name = caps.get(2).unwrap().as_str().to_string();
        return Some(DdlOperation::CreateEnum { name });
    }

    // TRUNCATE
    if let Some(caps) = TRUNCATE_RE.captures(stmt) {
        let table = caps.get(2).unwrap().as_str().to_string();
        return Some(DdlOperation::TruncateTable { table });
    }

    // Check if it looks like a DDL/DML statement (not just a comment)
    let upper = stmt.trim_start().to_uppercase();
    if upper.starts_with("--") || upper.is_empty() {
        return None;
    }

    // Produce an "Other" for non-trivial statements
    let preview: String = stmt.chars().take(80).collect();
    let preview = if stmt.len() > 80 {
        format!("{}...", preview)
    } else {
        preview
    };
    Some(DdlOperation::Other {
        statement_preview: preview,
    })
}

/// Split SQL into individual statements, respecting dollar-quoted blocks,
/// string literals, and comments.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut i = 0;

    while i < len {
        match bytes[i] {
            // Single-line comment
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                // Skip to end of line
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            // Block comment
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                let mut depth = 1;
                while i < len && depth > 0 {
                    if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                        depth += 1;
                        i += 2;
                    } else if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            // String literal (standard or E'...' escape string)
            b'\'' => {
                // Check if this is an E'...' escape string
                let is_escape_string = i > 0
                    && (bytes[i - 1] == b'E' || bytes[i - 1] == b'e')
                    && (i < 2 || !(bytes[i - 2].is_ascii_alphanumeric() || bytes[i - 2] == b'_'));
                i += 1;
                while i < len {
                    if is_escape_string && bytes[i] == b'\\' {
                        i += 2; // Skip escaped character in E-string
                        continue;
                    }
                    if bytes[i] == b'\'' {
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2; // doubled-quote escape
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            // Dollar-quoted string ($$...$$, $tag$...$tag$)
            b'$' => {
                // Find the tag
                let tag_start = i;
                i += 1;
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if i < len && bytes[i] == b'$' {
                    let tag = &sql[tag_start..=i];
                    i += 1;
                    // Find closing tag
                    loop {
                        if i >= len {
                            break;
                        }
                        if bytes[i] == b'$' {
                            let remaining = &sql[i..];
                            if remaining.starts_with(tag) {
                                i += tag.len();
                                break;
                            }
                        }
                        i += 1;
                    }
                }
                continue;
            }
            // Statement separator
            b';' => {
                let stmt = &sql[start..i];
                let trimmed = stmt.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed);
                }
                i += 1;
                start = i;
                continue;
            }
            _ => {}
        }
        i += 1;
    }

    // Remainder after last semicolon
    let remainder = sql[start..].trim();
    if !remainder.is_empty() {
        statements.push(remainder);
    }

    statements
}

/// Count the approximate line number for a byte offset.
pub fn line_number_at(sql: &str, offset: usize) -> usize {
    sql[..offset.min(sql.len())].lines().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_simple_statements() {
        let sql = "SELECT 1; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_split_respects_string_literals() {
        let sql = "SELECT 'hello;world'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts, vec!["SELECT 'hello;world'", "SELECT 2"]);
    }

    #[test]
    fn test_split_respects_dollar_quoting() {
        let sql =
            "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE plpgsql; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN; END;"));
    }

    #[test]
    fn test_split_respects_tagged_dollar_quoting() {
        let sql = "CREATE FUNCTION foo() RETURNS void AS $body$ BEGIN; END; $body$ LANGUAGE plpgsql; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN; END;"));
    }

    #[test]
    fn test_split_respects_comments() {
        let sql = "-- This is a comment with ; semicolon\nSELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_split_no_trailing_semicolon() {
        let sql = "SELECT 1";
        let stmts = split_statements(sql);
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn test_extract_create_table() {
        let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY);";
        let ops = extract_ddl_operations(sql);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                assert_eq!(table, "users");
                assert!(!if_not_exists);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_extract_create_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS users (id SERIAL);";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                assert_eq!(table, "users");
                assert!(if_not_exists);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_extract_add_column() {
        let sql = "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT '';";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::AlterTableAddColumn {
                table,
                column,
                is_not_null,
                has_default,
                ..
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
                assert!(is_not_null);
                assert!(has_default);
            }
            _ => panic!("Expected AlterTableAddColumn"),
        }
    }

    #[test]
    fn test_extract_create_index() {
        let sql = "CREATE UNIQUE INDEX CONCURRENTLY idx_users_email ON users (email);";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateIndex {
                name,
                table,
                is_concurrent,
                is_unique,
            } => {
                assert_eq!(name, "idx_users_email");
                assert_eq!(table, "users");
                assert!(is_concurrent);
                assert!(is_unique);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_extract_create_function() {
        let sql = "CREATE OR REPLACE FUNCTION my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateFunction { name } => {
                assert_eq!(name, "my_func");
            }
            _ => panic!("Expected CreateFunction, got {:?}", ops[0]),
        }
    }

    #[test]
    fn test_extract_create_enum() {
        let sql = "CREATE TYPE mood AS ENUM ('happy', 'sad');";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateEnum { name } => {
                assert_eq!(name, "mood");
            }
            _ => panic!("Expected CreateEnum"),
        }
    }

    #[test]
    fn test_extract_multiple() {
        let sql = "CREATE TABLE users (id SERIAL); CREATE INDEX idx_users ON users (id); DROP TABLE old_table;";
        let ops = extract_ddl_operations(sql);
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_extract_truncate() {
        let sql = "TRUNCATE TABLE users;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::TruncateTable { table } => assert_eq!(table, "users"),
            _ => panic!("Expected TruncateTable"),
        }
    }

    #[test]
    fn test_extract_drop_column() {
        let sql = "ALTER TABLE users DROP COLUMN email;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::AlterTableDropColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            _ => panic!("Expected AlterTableDropColumn"),
        }
    }

    #[test]
    fn test_extract_alter_column() {
        let sql = "ALTER TABLE users ALTER COLUMN name TYPE text;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::AlterTableAlterColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "name");
            }
            _ => panic!("Expected AlterTableAlterColumn"),
        }
    }

    #[test]
    fn test_extract_materialized_view() {
        let sql = "CREATE MATERIALIZED VIEW user_stats AS SELECT count(*) FROM users;";
        let ops = extract_ddl_operations(sql);
        match &ops[0] {
            DdlOperation::CreateView {
                name,
                is_materialized,
            } => {
                assert_eq!(name, "user_stats");
                assert!(is_materialized);
            }
            _ => panic!("Expected CreateView"),
        }
    }

    #[test]
    fn test_block_comment_with_semicolons() {
        let sql = "/* comment; with; semicolons */ SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_escaped_string_quotes() {
        let sql = "SELECT 'it''s; here'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_respects_e_escape_strings() {
        let sql = r"SELECT E'hello\';world'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains(r"E'hello\';world'"));
    }

    #[test]
    fn test_split_e_string_with_backslash() {
        let sql = r"SELECT E'it\'s a test; really'; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_nested_block_comments() {
        let sql = "SELECT /* outer /* inner */ outer */ 1; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_split_whitespace_only() {
        let stmts = split_statements("   \n\t  ");
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_split_comment_only() {
        let stmts = split_statements("-- just a comment\n");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "-- just a comment");
    }

    #[test]
    fn test_split_mixed_e_and_regular_strings() {
        let sql = r"SELECT 'normal;string', E'escape\';string'; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }
}
