
default:
    @just --list

fmt:
    cargo +nightly fmt --all

fmt-check:
    cargo +nightly fmt --all -- --check

check:
    cargo check --workspace

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

test_database_url := env_var_or_default('TEST_DATABASE_URL', 'postgres://admin:password@localhost:5432/alac_bot_test')
test:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z '{{ test_database_url }}' ]; then
        echo 'error: TEST_DATABASE_URL is required for tests' >&2
        exit 1
    fi
    TEST_DATABASE_URL='{{ test_database_url }}' cargo nextest run --all-targets --all-features

build:
    cargo build

release:
    cargo build --release

run: build
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f .env ]; then
        echo "error: .env not found — copy .env.example and fill it in" >&2
        exit 1
    fi
    ./target/debug/bot

docker:
    docker build -t peerless:latest .
