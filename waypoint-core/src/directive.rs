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
    /// Preconditions: `-- waypoint:require table_exists("users")`
    pub require: Vec<String>,
    /// Postconditions: `-- waypoint:ensure column_exists("users", "email")`
    pub ensure: Vec<String>,
    /// Safety override: `-- waypoint:safety-override` bypasses DANGER blocks
    pub safety_override: bool,
}

/// Strip a directive prefix, ensuring the prefix is followed by whitespace or end of string.
/// This prevents prefix collisions like "waypoint:env" matching "waypoint:environment".
fn strip_directive_prefix<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    if let Some(rest) = line.strip_prefix(prefix) {
        if rest.is_empty() || rest.starts_with(char::is_whitespace) {
            Some(rest.trim())
        } else {
            None
        }
    } else {
        None
    }
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

        if let Some(value) = strip_directive_prefix(comment_body, "waypoint:depends") {
            for item in value.split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    // Strip optional V prefix
                    let version = item.strip_prefix('V').unwrap_or(item);
                    directives.depends.push(version.to_string());
                }
            }
        } else if let Some(value) = strip_directive_prefix(comment_body, "waypoint:env") {
            for item in value.split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    directives.env.push(item.to_string());
                }
            }
        } else if let Some(value) = strip_directive_prefix(comment_body, "waypoint:require") {
            if !value.is_empty() {
                directives.require.push(value.to_string());
            }
        } else if let Some(value) = strip_directive_prefix(comment_body, "waypoint:ensure") {
            if !value.is_empty() {
                directives.ensure.push(value.to_string());
            }
        } else if comment_body.trim() == "waypoint:safety-override" {
            directives.safety_override = true;
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

    #[test]
    fn test_parse_require_directive() {
        let sql = "-- waypoint:require table_exists(\"users\")\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.require, vec!["table_exists(\"users\")"]);
    }

    #[test]
    fn test_parse_ensure_directive() {
        let sql = "-- waypoint:ensure column_exists(\"users\", \"email\")\nALTER TABLE users ADD COLUMN email TEXT;";
        let d = parse_directives(sql);
        assert_eq!(d.ensure, vec!["column_exists(\"users\", \"email\")"]);
    }

    #[test]
    fn test_parse_multiple_guards() {
        let sql = "-- waypoint:require table_exists(\"users\")\n-- waypoint:require NOT column_exists(\"users\", \"email\")\n-- waypoint:ensure column_exists(\"users\", \"email\")\nALTER TABLE users ADD COLUMN email TEXT;";
        let d = parse_directives(sql);
        assert_eq!(d.require.len(), 2);
        assert_eq!(d.ensure.len(), 1);
    }

    #[test]
    fn test_parse_safety_override() {
        let sql = "-- waypoint:safety-override\nALTER TABLE large_table ADD COLUMN foo TEXT;";
        let d = parse_directives(sql);
        assert!(d.safety_override);
    }

    #[test]
    fn test_safety_override_default_false() {
        let sql = "CREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(!d.safety_override);
    }

    #[test]
    fn test_env_prefix_does_not_match_ensure() {
        let sql = "-- waypoint:ensure column_exists(\"users\", \"email\")\nALTER TABLE users ADD COLUMN email TEXT;";
        let d = parse_directives(sql);
        // Should be parsed as ensure, not env
        assert!(d.env.is_empty());
        assert_eq!(d.ensure.len(), 1);
    }

    #[test]
    fn test_directive_prefix_boundary() {
        // "waypoint:environment" should NOT match "waypoint:env"
        let sql = "-- waypoint:environment prod\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        // Should NOT be parsed as env directive since "waypoint:environment" != "waypoint:env"
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_empty_depends() {
        let sql = "-- waypoint:depends\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_parse_empty_env() {
        let sql = "-- waypoint:env\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_require_with_special_chars() {
        let sql = "-- waypoint:require table_exists(\"my-table\")\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.require, vec!["table_exists(\"my-table\")"]);
    }
}
