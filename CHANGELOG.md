# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-02-20

### Added

**New commands (12):**
- `undo` — Undo applied migrations using manual U files or auto-generated reversals
- `lint` — Static analysis of migration SQL (no DB required), 8 rules (E001-E002, W001-W007, I001)
- `changelog` — Auto-generate changelog from migration DDL (no DB required)
- `diff` — Compare schemas between databases, generate migration SQL
- `drift` — Detect manual schema changes that bypassed migrations
- `snapshot` / `restore` — Save and restore schema snapshots as DDL
- `preflight` — Pre-migration health checks (recovery mode, replication lag, locks, connections)
- `check-conflicts` — Detect migration version conflicts between git branches (no DB required)
- `safety` — Analyze migrations for PostgreSQL lock levels, row-count impact, and safety verdicts (Safe/Caution/Danger)
- `advise` — Schema advisory rules (A001-A010) with auto-generated fix SQL
- `simulate` — Run pending migrations in a throwaway schema to verify correctness
- `self-update` — Update waypoint binary from GitHub Releases

**New core modules:**
- `directive.rs` — Parse `-- waypoint:env`, `-- waypoint:depends`, `-- waypoint:require`, `-- waypoint:ensure`, `-- waypoint:safety-override` directives from SQL file headers
- `guard.rs` — Recursive descent expression parser and evaluator with 10 built-in assertion functions (`table_exists`, `column_exists`, `row_count`, `sql`, etc.)
- `reversal.rs` — Auto-generate reverse DDL from before/after schema snapshots, store in history table
- `safety.rs` — Map DDL operations to PostgreSQL lock levels, estimate impact from `pg_stat_user_tables`
- `advisor.rs` — 10 schema advisory rules with severity levels and fix SQL generation
- `sql_parser.rs` — Regex-based DDL extraction (`DdlOperation` enum) and `split_statements()` with dollar-quote, string, and comment awareness
- `schema.rs` — PostgreSQL introspection via `information_schema`/`pg_catalog`, schema diff, DDL generation
- `dependency.rs` — Migration dependency graph with topological sort (Kahn's algorithm)
- `preflight.rs` — Pre-migration health checks against PostgreSQL system catalogs
- `multi.rs` — Multi-database orchestration with dependency ordering

**New features:**
- Undo migrations (`U{version}__desc.sql`) with automatic fallback to auto-generated reversals
- Environment-scoped migrations (`-- waypoint:env dev,staging`)
- Migration dependency ordering (`-- waypoint:depends V1,V3`) with cycle detection
- Guard expressions — preconditions (`-- waypoint:require`) and postconditions (`-- waypoint:ensure`) evaluated against live DB
- Safety analysis with DANGER migration blocking (`--force` to override)
- Batch transaction mode (`--transaction`) — wrap all pending migrations in a single atomic transaction
- Multi-database configuration (`[[databases]]` TOML array) with dependency ordering
- Enhanced dry-run with EXPLAIN output
- TCP keepalive support (`--keepalive`, `keepalive_secs` config)
- Connection retry with exponential backoff and jitter
- Transient error detection and automatic reconnection (max 3 retries)

**New CLI flags:**
- `--environment` — Filter migrations by environment
- `--dependency-ordering` — Enable topological sort for migration ordering
- `--skip-preflight` — Skip pre-flight health checks
- `--database` — Target a specific database in multi-db mode
- `--fail-fast` — Stop on first failure in multi-db mode
- `--force` — Override DANGER safety blocks
- `--simulate` — Run simulation before applying migrations
- `--transaction` — Batch transaction mode
- `--keepalive` — TCP keepalive interval

**New library API methods on `Waypoint`:**
- `undo()`, `lint()`, `changelog()`, `diff()`, `drift()`, `snapshot()`, `restore()`, `explain()`, `preflight()`, `check_conflicts()`, `client()`

**New public re-exports:**
- `ChangelogReport`, `ConflictReport`, `DiffReport`, `DriftReport`, `ExplainReport`, `LintReport`, `SnapshotReport`, `RestoreReport`, `UndoReport`, `UndoTarget`, `MultiWaypoint`, `PreflightReport`

**Infrastructure:**
- `install.sh` shell installer for Linux/macOS
- GitHub Actions release workflow for cross-platform binaries
- `self-update` command with GitHub Releases API
- docs.rs metadata and module-level documentation
- Test fixtures for all command types (`docs/fixtures/`)

### Changed

- Replaced `regex` crate with `regex-lite` (smaller binary, no Unicode tables needed for SQL patterns)
- Replaced `tracing`/`tracing-subscriber` with `log`/`env_logger` (simpler, fewer dependencies)
- Replaced `rand` with `fastrand` (smaller, no crypto overhead for jitter)
- `connect_with_config()` now injects TCP keepalive parameters
- `ResolvedMigration` now includes a `directives` field for parsed `-- waypoint:*` comments
- `MigrationKind` enum now includes `Undo(MigrationVersion)` variant
- `MigrationType` enum now includes `Undo` variant
- `WaypointConfig` now includes `lint`, `snapshots`, `preflight`, `multi_database` fields
- `MigrationSettings` now includes `environment`, `dependency_ordering`, `show_progress` fields
- `CliOverrides` now includes `environment`, `dependency_ordering` fields
- `WaypointError` enum expanded from 12 to 28 variants
- History table now tracks `reversal_sql` column for auto-generated undo SQL
- `migrate` command now runs safety analysis, preflight checks, guard evaluation, and auto-reversal generation
- `rustls` configuration now explicitly selects the `ring` crypto provider

### Performance

- Static `LazyLock` regex compilation for placeholder, batch validation, and migration filename patterns
- Pre-computed uppercase SQL in lint (avoids redundant `to_uppercase()` per rule)
- Zero-allocation case-insensitive comparison in guard tokenizer (`eq_ignore_ascii_case`)
- Borrowed `&str` references in dependency graph and multi-db topological sort (avoids intermediate `String` cloning)
- Parallel schema introspection queries in `schema.rs`

### Fixed

- E-string support in SQL statement splitter (`E'...\'..'`)
- Nested block comment support (`/* outer /* inner */ outer */`)
- Dollar-quote-aware placeholder replacement (skips `${key}` inside `$$...$$`)
- Duplicate migration version detection across files
- Graceful handling of malformed migration filenames (warns and skips instead of aborting)

## [0.2.0] - 2026-02-20

### Added

- README.md with full documentation
- MIT LICENSE
- crates.io metadata for `waypoint-core` and `waypoint-cli`
- Library usage documentation and examples
- GitHub Actions workflow for publishing to crates.io

### Changed

- Version bump from 0.1.0 to 0.2.0

## [0.1.1] - 2026-02-20

### Fixed

- Docker build: touch sources to invalidate cargo cache after dummy build
- Use latest stable Rust image for Docker builds
- Bump Rust Docker image to 1.87 for let-chains support
- Fix TIMESTAMPTZ type mismatch in history table reads
- Fix all clippy warnings and formatting issues

## [0.1.0] - 2026-02-20

### Added

- Initial release
- Core migration engine: versioned (`V`) and repeatable (`R`) migrations
- Flyway-compatible CRC32 checksums and migration naming
- Commands: `migrate`, `info`, `validate`, `repair`, `baseline`, `clean`
- TOML configuration with environment variable overrides
- TLS support via rustls with Mozilla CA bundle
- PostgreSQL advisory locking for concurrent safety
- `${key}` placeholder replacement in SQL
- SQL callback hooks (beforeMigrate, afterMigrate, beforeEachMigrate, afterEachMigrate)
- Docker image with Flyway-compatible environment variables
- CI/CD with GitHub Actions
- Colored table output with `comfy-table`

[0.3.0]: https://github.com/mantissaman/waypoint/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mantissaman/waypoint/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mantissaman/waypoint/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mantissaman/waypoint/releases/tag/v0.1.0
