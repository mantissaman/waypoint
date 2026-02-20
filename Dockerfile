# Stage 1: Build
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev git

WORKDIR /usr/src/waypoint

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY waypoint-core/Cargo.toml waypoint-core/Cargo.toml
COPY waypoint-cli/Cargo.toml waypoint-cli/Cargo.toml
COPY waypoint-cli/build.rs waypoint-cli/build.rs

# Create dummy source files to build dependencies
RUN mkdir -p waypoint-core/src waypoint-cli/src && \
    echo "pub fn lib() {}" > waypoint-core/src/lib.rs && \
    echo "fn main() {}" > waypoint-cli/src/main.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf waypoint-core/src waypoint-cli/src

# Copy actual source
COPY waypoint-core/ waypoint-core/
COPY waypoint-cli/ waypoint-cli/

# Copy .git if present (for build metadata), ignore failure
COPY .gi[t] .git

# Touch sources so cargo detects changes over dummy build
RUN touch waypoint-core/src/lib.rs waypoint-cli/src/main.rs

# Build release binary
RUN cargo build --release --bin waypoint

# Stage 2: Minimal runtime image
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /usr/src/waypoint/target/release/waypoint /usr/local/bin/waypoint

# Match Flyway convention: migrations go in /waypoint/sql
RUN mkdir -p /waypoint/sql
WORKDIR /waypoint

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
