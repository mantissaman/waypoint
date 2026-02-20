//! Parse `-- waypoint:*` comment directives from SQL file headers.
//!
//! Directives appear as SQL comments at the top of migration files:
//! ```sql
//! -- waypoint:env dev,staging
//! -- waypoint:depends V3,V5
//! CREATE TABLE ...
//! ```

/// Parsed directives from a migration file header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationDirectives {
    /// Dependencies: `-- waypoint:depends V3,V5` (V prefix is stripped)
    pub depends: Vec<String>,
    /// Environment tags: `-- waypoint:env dev,staging`
    pub env: Vec<String>,
}

/// Parse `-- waypoint:*` directives from SQL content.
///
/// Only parses comment lines (`--`) at the top of the file.
/// Stops at the first non-empty, non-comment line.
pub fn parse_directives(sql: &str) -> MigrationDirectives {
    let mut directives = MigrationDirectives::default();

    for line in sql.lines() {
        let trimmed = line.trim();

        // Skip empty lines at the top
        if trimmed.is_empty() {
            continue;
        }

        // Only process SQL comment lines
        if !trimmed.starts_with("--") {
            break;
        }

        let comment_body = trimmed.strip_prefix("--").unwrap().trim();

        if let Some(value) = comment_body.strip_prefix("waypoint:depends") {
            let value = value.trim();
            for item in value.split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    // Strip optional V prefix
                    let version = item.strip_prefix('V').unwrap_or(item);
                    directives.depends.push(version.to_string());
                }
            }
        } else if let Some(value) = comment_body.strip_prefix("waypoint:env") {
            let value = value.trim();
            for item in value.split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    directives.env.push(item.to_string());
                }
            }
        }
    }

    directives
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_env_directive() {
        let sql = "-- waypoint:env dev,staging\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev", "staging"]);
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_parse_depends_directive() {
        let sql = "-- waypoint:depends V3,V5\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.depends, vec!["3", "5"]);
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_depends_without_v_prefix() {
        let sql = "-- waypoint:depends 3,5\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.depends, vec!["3", "5"]);
    }

    #[test]
    fn test_parse_multiple_directives() {
        let sql = "-- waypoint:env dev\n-- waypoint:depends V1,V2\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev"]);
        assert_eq!(d.depends, vec!["1", "2"]);
    }

    #[test]
    fn test_stops_at_non_comment_line() {
        let sql = "-- waypoint:env dev\nCREATE TABLE foo();\n-- waypoint:env prod\n";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev"]);
    }

    #[test]
    fn test_empty_sql() {
        let d = parse_directives("");
        assert!(d.env.is_empty());
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_no_directives() {
        let sql = "-- Regular comment\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(d.env.is_empty());
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_skips_leading_blank_lines() {
        let sql = "\n\n-- waypoint:env prod\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["prod"]);
    }

    #[test]
    fn test_whitespace_in_values() {
        let sql = "-- waypoint:env  dev , staging , prod \nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev", "staging", "prod"]);
    }

    #[test]
    fn test_no_env_runs_everywhere() {
        let d = MigrationDirectives::default();
        assert!(d.env.is_empty());
    }
}
