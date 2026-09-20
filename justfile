# ALAC Bot (Rust) — task recipes.
#
# Toolchains: stable for everything; nightly is ONLY needed for `fmt`,
# because import grouping/sorting (rustfmt.toml) is a nightly-only option.

# Default: list the recipes.
default:
    @just --list

# Format all crates (nightly rustfmt — groups and sorts imports).
fmt:
    cargo +nightly fmt --all

# Verify formatting without touching files (CI-style).
fmt-check:
    cargo +nightly fmt --all -- --check

# Fast type-check of the workspace.
check:
    cargo check --workspace

# Lint with warnings as errors.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Run the test suite (needs a PostgreSQL test database).
test_database_url := env_var_or_default('TEST_DATABASE_URL', 'postgres://admin:password@localhost:5432/alac_bot_test')
test:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z '{{ test_database_url }}' ]; then
        echo 'error: TEST_DATABASE_URL is required for tests' >&2
        exit 1
    fi
    TEST_DATABASE_URL='{{ test_database_url }}' cargo nextest run

# Debug build of the bot binary.
build:
    cargo build

# Optimized release build.
release:
    cargo build --release

# Build and run the bot (loads .env from the repo root).
run: build
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f .env ]; then
        echo "error: .env not found — copy .env.example and fill it in" >&2
        exit 1
    fi
    ./target/debug/bot

# Build the production container image.
docker:
    docker build -t alac-bot:latest .
