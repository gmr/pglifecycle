# pglifecycle task runner. Run `just` to list recipes.
#
# The coverage, round-trip and deploy gates need PostgreSQL; they bring
# up the compose.yaml container and discover its dynamically-mapped
# port.

set shell := ["bash", "-euo", "pipefail", "-c"]

# List available recipes
default:
    @just --list

# --- Build -----------------------------------------------------------

# Debug build
build:
    cargo build

# Optimized release build
release:
    cargo build --release

# Run the binary (e.g. `just run pull --help`)
run *ARGS:
    cargo run -- {{ARGS}}

# Remove build artifacts
clean:
    cargo clean

# --- Quality ---------------------------------------------------------

# Format sources in place
fmt:
    cargo fmt

# Verify formatting (CI gate)
fmt-check:
    cargo fmt --check

# Lint with warnings denied (CI gate)
lint:
    cargo clippy --all-targets -- -D warnings

# Unit + integration tests (CI gate)
test:
    cargo test

# The full CI trio: format check, lint, test
check: fmt-check lint test

# Run all pre-commit hooks
pre-commit:
    pre-commit run --all-files

# --- Database (compose PG17) -----------------------------------------

# Start the PostgreSQL container and wait until healthy
db-up:
    docker compose up -d --wait

# Stop and remove the PostgreSQL container
db-down:
    docker compose down

# Print the host port the container's 5432 is mapped to
db-port:
    @docker compose port postgres 5432 | cut -d: -f2

# --- Gates -----------------------------------------------------------

# Coverage gate (which objects pull still cannot model)
coverage-gate: build db-up
    PGHOST=localhost \
    PGPORT="$(docker compose port postgres 5432 | cut -d: -f2)" \
    PGUSER=postgres \
    bin/coverage-gate

# Parse-coverage gate (the fixtures exercise every type pull models)
parse-coverage: db-up
    PGHOST=localhost \
    PGPORT="$(docker compose port postgres 5432 | cut -d: -f2)" \
    PGUSER=postgres \
    bin/parse-coverage

# Round-trip gate (schema → pull → build → restore → diff)
round-trip: build db-up
    PGHOST=localhost \
    PGPORT="$(docker compose port postgres 5432 | cut -d: -f2)" \
    PGUSER=postgres \
    bin/round-trip

# Deploy gates (equivalence, convergence, safety)
deploy-gates: build db-up
    PGHOST=localhost \
    PGPORT="$(docker compose port postgres 5432 | cut -d: -f2)" \
    PGUSER=postgres \
    bin/deploy-gates

# Pagila gate: the round-trip gate on the pagila sample database, a
# schema written by someone else (fixtures/pagila/README.md)
pagila-gate: build db-up
    PGHOST=localhost \
    PGPORT="$(docker compose port postgres 5432 | cut -d: -f2)" \
    PGUSER=postgres \
    NORMALIZE_BODIES=1 \
    bin/round-trip fixtures/pagila/pagila-schema.sql pagila

# Every database gate
gates: coverage-gate parse-coverage round-trip pagila-gate deploy-gates

# --- Docs ------------------------------------------------------------

# Build the documentation site (strict, as CI does)
docs:
    properdocs build --strict

# Install the documentation toolchain (properdocs and the generator plugins)
docs-deps:
    pip install -r docs/requirements.txt

# Write the generated schema reference to ./site-preview without a build
docs-schema-preview:
    python3 bin/generate-schema-docs.py --preview

# Serve the documentation locally with live reload
docs-serve:
    properdocs serve

# --- Release ---------------------------------------------------------
#
# Tagged releases are cut on GitHub: creating a release triggers the
# Release and Publish workflows, which build the cross-platform
# binaries, update the Homebrew tap, and `cargo publish` to crates.io.
# These recipes are for local verification before tagging.

# Dry-run the crates.io publish
publish-dry:
    cargo publish --dry-run

# Everything CI checks plus both database gates, before a release
pre-release: check gates docs publish-dry
