# Waypoint Docker Image

Lightweight PostgreSQL migration tool, distributed as a minimal Docker image (~30MB).
Drop-in replacement for Flyway containers.

## Quick Start

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=mydb \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=secret \
  tensorbeeio/waypoint
```

## Pull from Docker Hub

```bash
docker pull tensorbeeio/waypoint:latest
docker pull tensorbeeio/waypoint:0.1.0    # pinned version
```

## Migrating from Flyway

Replace your Flyway setup:

```dockerfile
# Before
FROM flyway/flyway
COPY migrations /flyway/sql

# After
FROM tensorbeeio/waypoint
COPY migrations /waypoint/sql
```

The same environment variables work:

| Env Var | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | Database host |
| `DB_PORT` | `5432` | Database port |
| `DB_NAME` | `postgres` | Database name |
| `DB_USERNAME` | `postgres` | Database user |
| `DB_PASSWORD` | (empty) | Database password |
| `CONNECT_RETRIES` | `50` | Connection retry attempts |
| `SSL_MODE` | `prefer` | TLS mode: `disable`, `prefer`, `require` |
| `LOCATIONS` | `/waypoint/sql` | Migration file directory |

## Entrypoint Behavior

The `docker-entrypoint.sh` script:

1. Builds a JDBC-style connection URL from environment variables
2. Runs `waypoint migrate` with `--out-of-order` enabled
3. Retries connection up to 50 times (configurable)
4. Prints elapsed time on completion

## Docker Compose

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
    image: tensorbeeio/waypoint:latest
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

## Advanced Usage

Override the entrypoint to use the CLI directly:

```bash
# Show help
docker run --rm --entrypoint waypoint tensorbeeio/waypoint --help

# Migration status
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  info

# Dry-run
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  --dry-run migrate

# JSON output
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  --json info

# Validate / Repair
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  validate
```

## TLS Connections

The image includes Mozilla CA certificates. Control via `SSL_MODE`:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=my-rds-instance.amazonaws.com \
  -e DB_NAME=mydb \
  -e DB_USERNAME=admin \
  -e DB_PASSWORD=secret \
  -e SSL_MODE=require \
  tensorbeeio/waypoint
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Validation failed |
| 4 | Database error |
| 5 | Migration or hook failed |
| 6 | Lock error |
| 7 | Clean disabled |
