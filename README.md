# Waypoint

[![CI](https://github.com/mantissaman/waypoint/actions/workflows/ci.yml/badge.svg)](https://github.com/mantissaman/waypoint/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/waypoint-core.svg)](https://crates.io/crates/waypoint-core)
[![docs.rs](https://docs.rs/waypoint-core/badge.svg)](https://docs.rs/waypoint-core)
[![Downloads](https://img.shields.io/crates/d/waypoint-core.svg)](https://crates.io/crates/waypoint-core)
[![Docker Hub](https://img.shields.io/docker/v/mantissaman/waypoint?label=docker&sort=semver)](https://hub.docker.com/r/mantissaman/waypoint)
[![Docker Pulls](https://img.shields.io/docker/pulls/mantissaman/waypoint)](https://hub.docker.com/r/mantissaman/waypoint)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Lightweight, Flyway-compatible PostgreSQL migration tool built in Rust.

- **Fast** — single static binary, ~30MB Docker image
- **Flyway-compatible** — same migration naming, CRC32 checksums, JDBC URL support
- **Production-ready** — TLS via rustls, advisory locking, structured logging, retry with backoff
- **Schema intelligence** — diff, drift detection, snapshots, EXPLAIN dry-run
- **Team-friendly** — lint, changelog, branch conflict detection, multi-database orchestration
- **Drop-in Docker replacement** — same env vars as Flyway containers

## Install

### Quick install (Linux / macOS)

```bash
curl -sSf https://raw.githubusercontent.com/mantissaman/waypoint/main/install.sh | sh
```

Pin a specific version:

```bash
curl -sSf https://raw.githubusercontent.com/mantissaman/waypoint/main/install.sh | WAYPOINT_VERSION=v0.3.0 sh
```

### Self-update

```bash
waypoint self-update          # Update to latest
waypoint self-update --check  # Check without installing
```

### From crates.io

```bash
cargo install waypoint-cli
```

### From source

```bash
cargo install --path waypoint-cli
```

### Library

```toml
[dependencies]
waypoint-core = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

### Docker

```bash
docker pull mantissaman/waypoint:latest
```

## Quick Start

```bash
# Apply migrations
waypoint --url "postgres://user:pass@localhost:5432/mydb" migrate

# Show migration status
waypoint --url "postgres://user:pass@localhost:5432/mydb" info

# Lint migration files (no DB needed)
waypoint lint

# Preview what would be applied
waypoint --url "postgres://user:pass@localhost:5432/mydb" migrate --dry-run
```

## Migration Files

Place SQL files in your migrations directory (default: `db/migrations/`):

```
db/migrations/
  V1__Create_users.sql
  V1.1__Add_email_column.sql
  V2__Create_orders.sql
  R__Create_user_view.sql
  U1__Create_users.sql        # Undo for V1
```

- **Versioned** — `V{version}__{description}.sql` — applied once, in order
- **Repeatable** — `R__{description}.sql` — re-applied when checksum changes
- **Undo** — `U{version}__{description}.sql` — reverses a versioned migration

### Directives

Add `-- waypoint:*` comment directives to the top of migration files:

```sql
-- waypoint:env dev,staging
-- waypoint:depends V1,V3
CREATE TABLE users (id SERIAL PRIMARY KEY);
```

| Directive | Description |
|---|---|
| `-- waypoint:env dev,staging` | Only run in the specified environments |
| `-- waypoint:depends V1,V3` | Declare explicit version dependencies |

## Commands

### Core Commands

| Command | Description | Needs DB |
|---|---|---|
| `migrate` | Apply pending migrations | Yes |
| `info` | Show migration status | Yes |
| `validate` | Verify applied migrations match local files | Yes |
| `repair` | Remove failed entries, update checksums | Yes |
| `baseline` | Mark an existing database at a version | Yes |
| `undo` | Undo applied migrations using `U{version}` files | Yes |
| `clean` | Drop all objects in managed schemas (requires `--allow-clean`) | Yes |

### Schema Intelligence

| Command | Description | Needs DB |
|---|---|---|
| `diff` | Compare schema against another database, generate migration SQL | Yes |
| `drift` | Detect manual schema changes that bypassed migrations | Yes |
| `snapshot` | Save current schema as DDL to a file | Yes |
| `restore` | Restore schema from a snapshot | Yes |
| `preflight` | Run pre-migration health checks | Yes |

### Developer Tools

| Command | Description | Needs DB |
|---|---|---|
| `lint` | Static analysis of migration SQL files | No |
| `changelog` | Auto-generate changelog from migration DDL | No |
| `check-conflicts` | Detect migration conflicts between git branches | No |
| `self-update` | Update waypoint to the latest version | No |

### Command Examples

```bash
# Undo the last migration
waypoint undo

# Undo to a specific version
waypoint undo --target 3

# Undo last N migrations
waypoint undo --count 2

# Lint with specific rules disabled
waypoint lint --disable W001,W002

# Lint in CI (exit code 1 on errors)
waypoint lint --strict

# Generate markdown changelog
waypoint changelog --format markdown

# Changelog for a version range
waypoint changelog --from 1 --to 5

# Diff against another database
waypoint diff --target-url "postgres://user:pass@localhost/staging_db"

# Diff and write migration file
waypoint diff --target-url "postgres://..." --output V5__Sync_schema.sql

# Diff and auto-generate versioned file
waypoint diff --target-url "postgres://..." --auto-version

# Detect schema drift
waypoint drift

# Take a snapshot
waypoint snapshot

# List available snapshots
waypoint restore

# Restore from a specific snapshot
waypoint restore 20260220_143022

# Run pre-flight checks
waypoint preflight

# Check for branch conflicts
waypoint check-conflicts --base main

# Check for conflicts in a git hook (minimal output)
waypoint check-conflicts --git-hook

# Migrate with environment scoping
waypoint migrate --environment production

# Migrate with dependency ordering
waypoint migrate --dependency-ordering

# Migrate and skip preflight checks
waypoint migrate --skip-preflight

# Enhanced dry-run with EXPLAIN output
waypoint migrate --dry-run
```

### Lint Rules

| Rule | Severity | Description |
|---|---|---|
| `E001` | error | `ADD COLUMN ... NOT NULL` without `DEFAULT` |
| `E002` | error | Multiple DDL statements without explicit transaction control |
| `W001` | warning | `CREATE TABLE` without `IF NOT EXISTS` |
| `W002` | warning | `CREATE INDEX` without `CONCURRENTLY` |
| `W003` | warning | `ALTER COLUMN TYPE` (full table rewrite + lock) |
| `W004` | warning | `DROP TABLE` / `DROP COLUMN` (destructive) |
| `W006` | warning | Volatile `DEFAULT` on `ADD COLUMN` (pre-PG11 rewrite) |
| `W007` | warning | `TRUNCATE TABLE` (destructive, locks) |
| `I001` | info | File contains only comments or whitespace |

## Configuration

Config is resolved in priority order (highest wins):

1. CLI arguments
2. Environment variables (`WAYPOINT_DATABASE_URL`, etc.)
3. `waypoint.toml` (override path with `-c`)
4. Built-in defaults

### waypoint.toml

```toml
[database]
url = "postgres://user:pass@localhost:5432/mydb"
connect_retries = 5
ssl_mode = "prefer"          # disable | prefer | require
connect_timeout = 30         # seconds
statement_timeout = 0        # seconds, 0 = no limit

[migrations]
locations = ["db/migrations"]
schema = "public"
table = "waypoint_schema_history"
out_of_order = false
validate_on_migrate = true
baseline_version = "1"
environment = "production"       # only run migrations tagged for this env
dependency_ordering = false      # use -- waypoint:depends for ordering
show_progress = true             # per-statement progress output

[lint]
disabled_rules = ["W001", "W006"]

[snapshots]
directory = ".waypoint/snapshots"
auto_snapshot_on_migrate = false
max_snapshots = 10

[preflight]
enabled = true
max_replication_lag_mb = 100
long_query_threshold_secs = 300

[hooks]
before_migrate = ["hooks/before.sql"]
after_migrate = ["hooks/after.sql"]

[placeholders]
env = "production"
app_name = "myapp"
```

### Multi-Database Configuration

Manage migrations across multiple databases with dependency ordering:

```toml
[[databases]]
name = "auth_db"
url = "postgres://localhost/auth"
depends_on = []

[databases.migrations]
locations = ["db/auth"]

[[databases]]
name = "app_db"
url = "postgres://localhost/app"
depends_on = ["auth_db"]

[databases.migrations]
locations = ["db/app"]
```

```bash
# Migrate all databases in dependency order
waypoint migrate

# Migrate a specific database
waypoint migrate --database auth_db

# Stop on first failure
waypoint migrate --fail-fast
```

Per-database env vars: `WAYPOINT_DB_{NAME}_URL` (e.g., `WAYPOINT_DB_AUTH_DB_URL`).

### Environment Variables

| Variable | Description |
|---|---|
| `WAYPOINT_DATABASE_URL` | Database connection URL |
| `WAYPOINT_SSL_MODE` | TLS mode: `disable`, `prefer`, `require` |
| `WAYPOINT_CONNECT_TIMEOUT` | Connection timeout in seconds |
| `WAYPOINT_STATEMENT_TIMEOUT` | Statement timeout in seconds |
| `WAYPOINT_CONNECT_RETRIES` | Number of connection retry attempts |
| `WAYPOINT_MIGRATIONS_LOCATIONS` | Comma-separated migration paths |
| `WAYPOINT_MIGRATIONS_SCHEMA` | Target schema |
| `WAYPOINT_MIGRATIONS_TABLE` | History table name |
| `WAYPOINT_ENVIRONMENT` | Environment for scoped migrations |
| `WAYPOINT_PLACEHOLDER_{KEY}` | Set placeholder value |
| `WAYPOINT_DB_{NAME}_URL` | Per-database URL (multi-db mode) |

### CLI Flags

```
waypoint [OPTIONS] <COMMAND>

Global options (can be placed before or after the subcommand):
  -c, --config <PATH>            Config file path
      --url <URL>                Database URL
      --schema <SCHEMA>          Target schema
      --table <TABLE>            History table name
      --locations <PATHS>        Migration locations (comma-separated)
      --connect-retries <N>      Connection retry attempts
      --ssl-mode <MODE>          TLS mode: disable, prefer, require
      --connect-timeout <SECS>   Connection timeout (default: 30)
      --statement-timeout <SECS> Statement timeout (default: 0)
      --out-of-order             Allow out-of-order migrations
      --json                     Output as JSON
      --dry-run                  Preview without applying changes
  -q, --quiet                    Suppress non-essential output
  -v, --verbose                  Enable debug output
      --environment <ENV>        Environment for scoped migrations
      --dependency-ordering      Enable dependency-based ordering
      --skip-preflight           Skip pre-flight health checks
      --database <NAME>          Filter to specific database (multi-db)
      --fail-fast                Stop on first failure (multi-db)
```

## Docker

Drop-in replacement for Flyway containers. Same environment variables work:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=host.docker.internal \
  -e DB_NAME=mydb \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=secret \
  mantissaman/waypoint
```

### Docker Compose

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: myapp
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d myapp"]
      interval: 5s
      timeout: 5s
      retries: 5

  migrate:
    image: mantissaman/waypoint:latest
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./db/migrations:/waypoint/sql
    environment:
      DB_HOST: db
      DB_NAME: myapp
      DB_USERNAME: app
      DB_PASSWORD: secret
```

### Migrating from Flyway

```dockerfile
# Before
FROM flyway/flyway
COPY migrations /flyway/sql

# After
FROM mantissaman/waypoint
COPY migrations /waypoint/sql
```

See [DOCKER.md](DOCKER.md) for full Docker documentation.

## Placeholders

Use `${key}` syntax in SQL files:

```sql
CREATE TABLE ${schema}.users (
    id SERIAL PRIMARY KEY,
    env VARCHAR(20) DEFAULT '${env}'
);
```

Set values via config, env vars (`WAYPOINT_PLACEHOLDER_ENV=production`), or CLI.

Built-in placeholders: `${schema}`, `${user}`, `${database}`, `${filename}`.

## Hooks

SQL callback hooks run before/after migrations (Flyway-compatible):

```
db/migrations/
  beforeMigrate.sql
  afterMigrate.sql
  beforeEachMigrate.sql
  afterEachMigrate__Refresh_views.sql
  V1__Create_users.sql
```

Or configure in `waypoint.toml`:

```toml
[hooks]
before_migrate = ["hooks/before.sql"]
after_migrate = ["hooks/after.sql"]
before_each_migrate = ["hooks/before_each.sql"]
after_each_migrate = ["hooks/after_each.sql"]
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Validation failed |
| 4 | Database error |
| 5 | Migration, hook, or undo failed |
| 6 | Lock error |
| 7 | Clean disabled |
| 8 | Self-update error |
| 9 | Lint errors found (with `--strict`) |
| 10 | Schema drift detected |
| 11 | Branch conflicts detected |
| 12 | Pre-flight checks failed |

## Using as a Library

Add `waypoint-core` to embed migrations in your Rust application:

```rust
use waypoint_core::config::WaypointConfig;
use waypoint_core::Waypoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config from waypoint.toml + env vars
    let config = WaypointConfig::load(None, &Default::default())?;
    let wp = Waypoint::new(config).await?;

    // Apply pending migrations
    let report = wp.migrate(None).await?;
    println!("Applied {} migrations", report.migrations_applied);

    Ok(())
}
```

### Build config programmatically

```rust
use std::path::PathBuf;
use waypoint_core::config::{DatabaseConfig, MigrationSettings, WaypointConfig};
use waypoint_core::Waypoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WaypointConfig {
        database: DatabaseConfig {
            url: Some("postgres://user:pass@localhost:5432/mydb".to_string()),
            ..Default::default()
        },
        migrations: MigrationSettings {
            locations: vec![PathBuf::from("db/migrations")],
            ..Default::default()
        },
        ..Default::default()
    };

    let wp = Waypoint::new(config).await?;

    // Check migration status
    let infos = wp.info().await?;
    for info in &infos {
        println!("{:?} - {} - {}",
            info.state,
            info.version.as_deref().unwrap_or("R"),
            info.description);
    }

    // Apply migrations
    let report = wp.migrate(None).await?;
    println!("Applied {} migrations in {}ms",
        report.migrations_applied, report.total_time_ms);

    // Validate
    let validation = wp.validate().await?;
    println!("Valid: {}", validation.valid);

    Ok(())
}
```

### Use with an existing connection

```rust
use waypoint_core::config::WaypointConfig;
use waypoint_core::db;
use waypoint_core::Waypoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WaypointConfig::load(None, &Default::default())?;
    let client = db::connect("postgres://user:pass@localhost:5432/mydb").await?;

    let wp = Waypoint::with_client(config, client);
    wp.migrate(None).await?;

    Ok(())
}
```

### Available methods

| Method | Returns | Description |
|---|---|---|
| `Waypoint::new(config)` | `Waypoint` | Connect and create instance |
| `Waypoint::with_client(config, client)` | `Waypoint` | Use existing connection |
| `wp.migrate(target)` | `MigrateReport` | Apply pending migrations |
| `wp.info()` | `Vec<MigrationInfo>` | Get migration status |
| `wp.validate()` | `ValidateReport` | Validate applied migrations |
| `wp.repair()` | `RepairReport` | Fix history table |
| `wp.baseline(version, desc)` | `()` | Baseline existing database |
| `wp.undo(target)` | `UndoReport` | Undo applied migrations |
| `wp.clean(allow)` | `Vec<String>` | Drop all managed objects |
| `wp.lint(locations, disabled)` | `LintReport` | Static analysis (no DB) |
| `wp.changelog(locations, from, to)` | `ChangelogReport` | Generate changelog (no DB) |
| `wp.diff(target)` | `DiffReport` | Compare schemas |
| `wp.drift()` | `DriftReport` | Detect schema drift |
| `wp.snapshot(config)` | `SnapshotReport` | Take schema snapshot |
| `wp.restore(config, id)` | `RestoreReport` | Restore from snapshot |
| `wp.explain()` | `ExplainReport` | Dry-run with EXPLAIN |
| `wp.preflight()` | `PreflightReport` | Health checks |
| `wp.check_conflicts(locations, base)` | `ConflictReport` | Branch conflict check (no DB) |

## Development

### Prerequisites

- Rust (latest stable)
- PostgreSQL (for integration tests)

### Build & Test

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo test --lib               # Unit tests (no DB required)
cargo clippy -- -D warnings    # Lint
cargo fmt --check              # Format check
```

### Integration Tests

```bash
# Start PostgreSQL, then:
export TEST_DATABASE_URL="postgres://user:pass@localhost:5432/waypoint_test"
cargo test --test integration_test
```

### Project Structure

```
waypoint/
  waypoint-core/               # Library crate
    src/
      commands/                # Command implementations
        migrate.rs             #   Apply pending migrations
        info.rs                #   Migration status
        validate.rs            #   Checksum validation
        repair.rs              #   Fix history table
        baseline.rs            #   Baseline existing DB
        clean.rs               #   Drop all objects
        undo.rs                #   Undo migrations
        lint.rs                #   Static SQL analysis
        changelog.rs           #   Auto-generate changelog
        diff.rs                #   Schema diff
        drift.rs               #   Drift detection
        snapshot.rs            #   Schema snapshots
        explain.rs             #   EXPLAIN dry-run
        check_conflicts.rs     #   Branch conflict detection
        preflight.rs           #   Pre-flight checks (wrapper)
      config.rs                # Config loading (TOML + env + CLI)
      db.rs                    # Connection, TLS, advisory locks
      history.rs               # Schema history table CRUD
      migration.rs             # File parsing and scanning
      checksum.rs              # CRC32 checksums (Flyway-compatible)
      placeholder.rs           # ${key} replacement
      hooks.rs                 # SQL callback hooks
      directive.rs             # -- waypoint:* directive parsing
      sql_parser.rs            # Regex-based DDL extraction
      schema.rs                # Schema introspection + diff + DDL gen
      dependency.rs            # Migration dependency graph (Kahn's)
      preflight.rs             # Pre-migration health checks
      multi.rs                 # Multi-database orchestration
      error.rs                 # Error types
      lib.rs                   # Public API (Waypoint struct)
    tests/
      integration_test.rs      # DB integration tests
  waypoint-cli/                # Binary crate
    src/
      main.rs                  # clap CLI, subcommand routing
      output.rs                # Terminal formatting (tables, colors)
      self_update.rs           # GitHub release updater
    build.rs                   # Git hash + build timestamp
```

## License

MIT License

Copyright (c) 2025 mantissaman

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
