# Testing Guide

Step-by-step instructions to manually test every waypoint feature using the provided fixtures.

## Prerequisites

```bash
# Build waypoint
cargo build

# Alias for convenience (or use cargo run --)
alias wp="cargo run --quiet --"
```

For features that need a database, start PostgreSQL and create a test database:

```bash
# Start PostgreSQL (adjust for your setup)
# Option A: Docker
docker run -d --name wp-test-pg \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=waypoint_test \
  -p 5432:5432 \
  postgres:16

# Option B: Use an existing PostgreSQL instance
# Just create the test database:
createdb waypoint_test

# Set the URL (used throughout this guide)
export DB_URL="postgres://postgres:postgres@localhost:5432/waypoint_test"
```

---

## Feature 1: Lint (no database needed)

Static analysis of migration SQL. Detects common anti-patterns.

**Fixtures:** `docs/fixtures/lint/`

### Step 1: Run lint on the fixtures

```bash
wp lint --locations docs/fixtures/lint
```

**Expected output:** Should report issues for each migration:

| File | Rule | Issue |
|---|---|---|
| V1 | W001 | CREATE TABLE without IF NOT EXISTS |
| V2 | E001 | ADD COLUMN NOT NULL without DEFAULT |
| V3 | W002 | CREATE INDEX without CONCURRENTLY (x2) |
| V3 | E002 | Multiple DDL without BEGIN/COMMIT |
| V4 | W004 | DROP COLUMN + DROP TABLE (destructive) |
| V5 | W003 | ALTER COLUMN TYPE (table rewrite) |
| V6 | W007 | TRUNCATE TABLE |
| V7 | W006 | Volatile DEFAULT (gen_random_uuid) |
| V8 | I001 | Empty file (only comments) |
| V9 | (none) | Clean migration, no issues |

### Step 2: Test disabled rules

```bash
wp lint --locations docs/fixtures/lint --disable W001,W002
```

**Expected:** W001 and W002 should be absent from the output.

### Step 3: Test strict mode

```bash
wp lint --locations docs/fixtures/lint --strict
echo "Exit code: $?"
```

**Expected:** Exit code 9 (lint errors found since E001 and E002 are errors).

### Step 4: Test JSON output

```bash
wp lint --locations docs/fixtures/lint --json | python3 -m json.tool
```

**Expected:** Well-formed JSON with `issues`, `files_checked`, `error_count`, `warning_count`, `info_count` fields.

---

## Feature 2: Changelog (no database needed)

Auto-generate a changelog from migration DDL operations.

**Fixtures:** `docs/fixtures/changelog/`

### Step 1: Plain text changelog

```bash
wp changelog --locations docs/fixtures/changelog
```

**Expected:** Grouped by version, showing DDL operations like CREATE TABLE, ALTER TABLE ADD COLUMN, CREATE INDEX, CREATE VIEW.

### Step 2: Markdown format

```bash
wp changelog --locations docs/fixtures/changelog --format markdown
```

**Expected:** Markdown-formatted changelog with headers (`## V1`, `## V2`, etc.).

### Step 3: JSON format

```bash
wp changelog --locations docs/fixtures/changelog --format json | python3 -m json.tool
```

**Expected:** JSON with `versions` array, each containing `version`, `description`, `script`, `changes`.

### Step 4: Version range filter

```bash
wp changelog --locations docs/fixtures/changelog --from 2 --to 3
```

**Expected:** Only shows V2 and V3, omits V1 and V4.

---

## Feature 3: Environment Scoping (needs database)

Migrations tagged with `-- waypoint:env` only run in matching environments.

**Fixtures:** `docs/fixtures/env-scoping/`

### Step 1: Migrate as "dev" environment

```bash
wp --url "$DB_URL" --locations docs/fixtures/env-scoping \
  migrate --environment dev
```

**Expected:** Applies V1 (no tag = runs everywhere), V2 (tagged dev,test), and V4 (tagged dev). Skips V3 (tagged production,staging).

### Step 2: Check info

```bash
wp --url "$DB_URL" --locations docs/fixtures/env-scoping info
```

**Expected:** V1, V2, V4 = Applied. V3 = Pending.

### Step 3: Clean up and test with "production"

```bash
wp --url "$DB_URL" --locations docs/fixtures/env-scoping clean --allow-clean

wp --url "$DB_URL" --locations docs/fixtures/env-scoping \
  migrate --environment production
```

**Expected:** Applies V1 (no tag) and V3 (tagged production,staging). Skips V2 and V4.

### Step 4: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/env-scoping clean --allow-clean
```

---

## Feature 4: Dependency Ordering (needs database)

Migrations with `-- waypoint:depends` run in topological order instead of linear version order.

**Fixtures:** `docs/fixtures/dependency/`

### Step 1: View the dependency structure

Look at the files:
- V1: Create users (no deps)
- V2: Create products (no deps)
- V3: Create orders (depends V1, V2)
- V4: Create reviews (depends V3)
- V5: Add user email (depends V1)

### Step 2: Migrate with dependency ordering

```bash
wp --url "$DB_URL" --locations docs/fixtures/dependency \
  migrate --dependency-ordering
```

**Expected:** All 5 migrations applied. V1 and V2 run before V3. V3 runs before V4. V5 runs after V1 (but could be before or after V2/V3).

### Step 3: Check info

```bash
wp --url "$DB_URL" --locations docs/fixtures/dependency info
```

**Expected:** All 5 migrations show as Applied.

### Step 4: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/dependency clean --allow-clean
```

---

## Feature 5: Undo (needs database)

Undo applied migrations using `U{version}__*.sql` files.

**Fixtures:** `docs/fixtures/undo/`

### Step 1: Apply all migrations

```bash
wp --url "$DB_URL" --locations docs/fixtures/undo migrate
```

**Expected:** 3 migrations applied (V1, V2, V3).

### Step 2: Undo the last migration

```bash
wp --url "$DB_URL" --locations docs/fixtures/undo undo
```

**Expected:** Undoes V3 (drops display_name and bio columns from users).

### Step 3: Undo by count

```bash
wp --url "$DB_URL" --locations docs/fixtures/undo undo --count 1
```

**Expected:** Undoes V2 (drops orders table).

### Step 4: Undo to a target version

```bash
# Re-apply first
wp --url "$DB_URL" --locations docs/fixtures/undo migrate

# Undo everything above V1
wp --url "$DB_URL" --locations docs/fixtures/undo undo --target 1
```

**Expected:** Undoes V3 and V2, keeping only V1 applied.

### Step 5: Check info

```bash
wp --url "$DB_URL" --locations docs/fixtures/undo info
```

**Expected:** V1 = Applied, V2 = Undone, V3 = Undone (or Pending if undone entries are cleaned).

### Step 6: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/undo clean --allow-clean
```

---

## Feature 6: Diff (needs two databases)

Compare schema between two databases and generate migration SQL.

### Step 1: Create two test databases

```bash
createdb waypoint_test_source
createdb waypoint_test_target
```

### Step 2: Apply migrations to the source DB

```bash
wp --url "postgres://postgres:postgres@localhost:5432/waypoint_test_source" \
  --locations docs/fixtures/basic migrate
```

### Step 3: Apply only V1 to the target DB

```bash
wp --url "postgres://postgres:postgres@localhost:5432/waypoint_test_target" \
  --locations docs/fixtures/basic migrate --target 1
```

### Step 4: Diff source against target

```bash
wp --url "postgres://postgres:postgres@localhost:5432/waypoint_test_source" \
  diff --target-url "postgres://postgres:postgres@localhost:5432/waypoint_test_target"
```

**Expected:** Shows differences — the target is missing orders table, user_stats view, indexes, and user profile columns.

### Step 5: Diff with SQL output

```bash
wp --url "postgres://postgres:postgres@localhost:5432/waypoint_test_source" \
  diff --target-url "postgres://postgres:postgres@localhost:5432/waypoint_test_target" \
  --output /tmp/diff_migration.sql

cat /tmp/diff_migration.sql
```

**Expected:** Generated SQL that would bring target in sync with source.

### Step 6: JSON output

```bash
wp --url "postgres://postgres:postgres@localhost:5432/waypoint_test_source" \
  diff --target-url "postgres://postgres:postgres@localhost:5432/waypoint_test_target" \
  --json | python3 -m json.tool
```

### Step 7: Clean up

```bash
dropdb waypoint_test_source
dropdb waypoint_test_target
```

---

## Feature 7: Drift Detection (needs database)

Detect manual schema changes that bypassed migrations.

### Step 1: Apply migrations

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic migrate
```

### Step 2: Verify no drift

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic drift
```

**Expected:** "No drift detected."

### Step 3: Make a manual change

```bash
psql "$DB_URL" -c "ALTER TABLE users ADD COLUMN manually_added TEXT;"
```

### Step 4: Detect drift

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic drift
echo "Exit code: $?"
```

**Expected:** Reports drift — "manually_added" column exists in DB but not in migrations. Exit code 10.

### Step 5: JSON output

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic drift --json | python3 -m json.tool
```

### Step 6: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic clean --allow-clean
```

---

## Feature 8: Snapshot & Restore (needs database)

Take a DDL snapshot of the current schema and restore from it later.

### Step 1: Apply migrations

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic migrate
```

### Step 2: Take a snapshot

```bash
wp --url "$DB_URL" snapshot
```

**Expected:** "Snapshot '20XXXXXX_XXXXXX' created (N objects captured)" and a file path.

### Step 3: List snapshots

```bash
wp --url "$DB_URL" restore
```

**Expected:** Table showing the snapshot ID, creation time, and file size.

### Step 4: Make destructive changes

```bash
psql "$DB_URL" -c "DROP TABLE orders CASCADE; DROP VIEW user_stats;"
```

### Step 5: Restore from snapshot

Use the snapshot ID from step 2:

```bash
wp --url "$DB_URL" restore <SNAPSHOT_ID>
```

**Expected:** Schema restored — tables and views recreated from snapshot DDL.

### Step 6: Verify restoration

```bash
psql "$DB_URL" -c "\dt"
psql "$DB_URL" -c "\dv"
```

**Expected:** `users`, `orders` tables and `user_stats` view should all exist again.

### Step 7: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic clean --allow-clean
rm -rf .waypoint/snapshots
```

---

## Feature 9: Pre-flight Checks (needs database)

Health checks run before migration.

### Step 1: Run preflight standalone

```bash
wp --url "$DB_URL" preflight
```

**Expected:** Shows pass/warn/fail for each check:
- Recovery mode
- Active connections
- Long-running queries
- Replication lag
- Database size
- Lock contention

### Step 2: JSON output

```bash
wp --url "$DB_URL" preflight --json | python3 -m json.tool
```

### Step 3: Skip preflight during migrate

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic \
  migrate --skip-preflight
```

**Expected:** Migrations applied without running preflight checks.

### Step 4: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic clean --allow-clean
```

---

## Feature 10: Enhanced Dry-Run / Explain (needs database)

Preview what migrations would do, with EXPLAIN output for DML.

### Step 1: Apply V1 only

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic migrate --target 1
```

### Step 2: Dry-run remaining migrations

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic migrate --dry-run
```

**Expected:** Shows pending migrations (V2, V3, R__Create_user_stats_view) with per-statement breakdown. DDL statements show "(DDL)" marker. No changes are actually applied.

### Step 3: Verify nothing was applied

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic info
```

**Expected:** V1 = Applied, V2/V3/R = still Pending.

### Step 4: JSON output

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic migrate --dry-run --json | python3 -m json.tool
```

### Step 5: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic clean --allow-clean
```

---

## Feature 11: Check Conflicts (no database needed, needs git)

Detect migration version collisions between git branches.

**Fixtures:** `docs/fixtures/conflicts-branch-a/` and `docs/fixtures/conflicts-branch-b/`

This feature requires a git repository. Here's how to test it:

### Step 1: Set up a test git repo

```bash
cd /tmp
mkdir wp-conflict-test && cd wp-conflict-test
git init
mkdir -p db/migrations

# Create base migrations
cp /path/to/waypoint/docs/fixtures/basic/V1__Create_users.sql db/migrations/
cp /path/to/waypoint/docs/fixtures/basic/V2__Create_orders.sql db/migrations/
git add . && git commit -m "Base migrations"
```

### Step 2: Create branch A with V3

```bash
git checkout -b feature-a
cp /path/to/waypoint/docs/fixtures/conflicts-branch-a/V3__Add_status_column.sql db/migrations/
git add . && git commit -m "Add status column"
```

### Step 3: Create branch B with conflicting V3

```bash
git checkout main
git checkout -b feature-b
cp /path/to/waypoint/docs/fixtures/conflicts-branch-b/V3__Add_role_column.sql db/migrations/
git add . && git commit -m "Add role column"
```

### Step 4: Check for conflicts from branch B against branch A

```bash
# From feature-b branch:
wp check-conflicts --base feature-a --locations db/migrations
echo "Exit code: $?"
```

**Expected:** Reports a version collision — V3 exists on both branches. Exit code 11.

### Step 5: Git hook mode

```bash
wp check-conflicts --base feature-a --locations db/migrations --git-hook
echo "Exit code: $?"
```

**Expected:** Minimal output (just count), same exit code 11.

### Step 6: JSON output

```bash
wp check-conflicts --base feature-a --locations db/migrations --json | python3 -m json.tool
```

### Step 7: Clean up

```bash
cd /tmp && rm -rf wp-conflict-test
```

---

## Feature 12: Multi-Database Orchestration (needs multiple databases)

Manage migrations across multiple databases with dependency ordering.

**Fixtures:** `docs/fixtures/multi-db/`

### Step 1: Create test databases

```bash
createdb waypoint_test_auth
createdb waypoint_test_app
```

### Step 2: Migrate all databases

```bash
wp -c docs/fixtures/multi-db/waypoint.toml migrate
```

**Expected:** auth_db migrated first (no dependencies), then app_db (depends on auth_db). Both succeed.

### Step 3: Check info for all databases

```bash
wp -c docs/fixtures/multi-db/waypoint.toml info
```

**Expected:** Shows migration status for both databases.

### Step 4: Migrate a specific database

```bash
wp -c docs/fixtures/multi-db/waypoint.toml migrate --database auth_db
```

**Expected:** Only runs against auth_db.

### Step 5: JSON output

```bash
wp -c docs/fixtures/multi-db/waypoint.toml info --json | python3 -m json.tool
```

### Step 6: Clean up

```bash
dropdb waypoint_test_auth
dropdb waypoint_test_app
```

---

## Feature 13: Statement-Level Progress (needs database)

Per-statement output during multi-statement migrations.

### Step 1: Migrate with verbose output

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic migrate -v
```

**Expected:** Debug-level logging showing each statement being executed with timing.

### Step 2: Clean up

```bash
wp --url "$DB_URL" --locations docs/fixtures/basic clean --allow-clean
```

---

## Global Features

### JSON Output

Every command supports `--json` for machine-readable output. The flag can go before or after the subcommand:

```bash
# Both work:
wp --json lint --locations docs/fixtures/lint
wp lint --locations docs/fixtures/lint --json
```

### Quiet Mode

Suppress non-essential output:

```bash
wp lint --locations docs/fixtures/lint -q
```

### Verbose/Debug Mode

Enable detailed logging:

```bash
wp lint --locations docs/fixtures/lint -v
```

---

## Quick Smoke Test (all no-DB features)

Run this to quickly verify all features that don't need a database:

```bash
echo "=== Lint ==="
wp lint --locations docs/fixtures/lint
echo ""

echo "=== Lint (strict, expect exit 9) ==="
wp lint --locations docs/fixtures/lint --strict; echo "Exit: $?"
echo ""

echo "=== Lint (JSON) ==="
wp lint --locations docs/fixtures/lint --json | head -5
echo "..."
echo ""

echo "=== Changelog (plain) ==="
wp changelog --locations docs/fixtures/changelog
echo ""

echo "=== Changelog (markdown) ==="
wp changelog --locations docs/fixtures/changelog --format markdown
echo ""

echo "=== Changelog (JSON) ==="
wp changelog --locations docs/fixtures/changelog --json | head -5
echo "..."
echo ""

echo "=== Changelog (range V2-V3) ==="
wp changelog --locations docs/fixtures/changelog --from 2 --to 3
echo ""

echo "=== Lint with disabled rules ==="
wp lint --locations docs/fixtures/lint --disable W001,W002,W003,W004,W006,W007,E002,I001
echo ""

echo "All no-DB tests complete!"
```

---

## Quick Smoke Test (all DB features)

Requires PostgreSQL running and `$DB_URL` set:

```bash
echo "=== Preflight ==="
wp --url "$DB_URL" preflight
echo ""

echo "=== Migrate ==="
wp --url "$DB_URL" --locations docs/fixtures/basic migrate
echo ""

echo "=== Info ==="
wp --url "$DB_URL" --locations docs/fixtures/basic info
echo ""

echo "=== Validate ==="
wp --url "$DB_URL" --locations docs/fixtures/basic validate
echo ""

echo "=== Snapshot ==="
wp --url "$DB_URL" snapshot
echo ""

echo "=== Snapshot List ==="
wp --url "$DB_URL" restore
echo ""

echo "=== Drift (expect no drift) ==="
wp --url "$DB_URL" --locations docs/fixtures/basic drift
echo ""

echo "=== Dry-Run (nothing pending) ==="
wp --url "$DB_URL" --locations docs/fixtures/basic migrate --dry-run
echo ""

echo "=== Clean ==="
wp --url "$DB_URL" --locations docs/fixtures/basic clean --allow-clean
echo ""

echo "=== Undo test ==="
wp --url "$DB_URL" --locations docs/fixtures/undo migrate
wp --url "$DB_URL" --locations docs/fixtures/undo undo
wp --url "$DB_URL" --locations docs/fixtures/undo info
wp --url "$DB_URL" --locations docs/fixtures/undo clean --allow-clean
echo ""

rm -rf .waypoint/snapshots
echo "All DB tests complete!"
```
