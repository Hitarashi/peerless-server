# syntax=docker/dockerfile:1.7

# ==============================================================================
# Stage 1: Runtime with native media support and TLS certificates
# ==============================================================================
FROM docker.io/debian:bookworm-slim AS runtime

# Native media decoding, metadata, and spectrogram rendering are provided by
# the Rust media crate. Keep only the TLS root certificates required for
# outbound HTTPS connections at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Prefer IPv4 when a host publishes both A and AAAA records. Debian ships
# /etc/gai.conf with every precedence rule commented out, so glibc falls back to
# the RFC 3484 default of IPv6-first. On a host whose containers have IPv6
# addresses but no usable IPv6 route (Docker without an IPv6 gateway), the first
# address tried is the AAAA one and the connect hangs until the pool times out
# instead of falling back. That is what made DATABASE_URL (Neon, AAAA + A)
# time out in bb8 on a dual-stack Docker host. Giving IPv4-mapped addresses a
# higher precedence makes the resolver return the reachable A record first.
RUN printf 'precedence ::ffff:0:0/96 100\n' >> /etc/gai.conf

# ==============================================================================
# Stage 2: Builder
# ==============================================================================
FROM docker.io/rust:1-bookworm AS builder
WORKDIR /app

# TLS roots for crates.io / git dependencies (cargo fetches the ferogram
# git dependency over HTTPS via its bundled libgit2 — no system git needed).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates binutils \
 && rm -rf /var/lib/apt/lists/*

# Build dependencies first for layer caching: manifests + lockfile only.
# Cargo insists on a manifest for every workspace member, so all ten are
# copied and stubbed (including the bot's bin target) and the dependency
# graph compiles without the real sources.
COPY Cargo.toml Cargo.lock ./
COPY crates/music/Cargo.toml crates/music/Cargo.toml
COPY crates/lyrics/Cargo.toml crates/lyrics/Cargo.toml
COPY crates/engine/Cargo.toml crates/engine/Cargo.toml
COPY crates/apple/Cargo.toml crates/apple/Cargo.toml
COPY crates/qobuz/Cargo.toml crates/qobuz/Cargo.toml
COPY crates/db/Cargo.toml crates/db/Cargo.toml
COPY crates/media/Cargo.toml crates/media/Cargo.toml
COPY crates/stream/Cargo.toml crates/stream/Cargo.toml
COPY crates/server/Cargo.toml crates/server/Cargo.toml
COPY crates/bot/Cargo.toml crates/bot/Cargo.toml
RUN --mount=type=cache,id=peerless-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=peerless-cargo-git,target=/usr/local/cargo/git \
    mkdir -p crates/music/src crates/lyrics/src crates/engine/src crates/apple/src \
             crates/qobuz/src crates/db/src crates/media/src crates/stream/src \
             crates/server/src crates/bot/src crates/bot/src/bin \
 && echo 'pub fn _stub() {}' > crates/music/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/lyrics/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/engine/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/apple/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/qobuz/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/db/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/media/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/stream/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/server/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/bot/src/lib.rs \
 && echo 'fn main() {}' > crates/bot/src/main.rs \
 && echo 'fn main() {}' > crates/bot/src/bin/backfill_recording_mbids.rs \
 && cargo build --release --locked -p bot --bin bot --bin backfill_recording_mbids \
 && rm -rf crates

# Real sources: build the binary (dependency layers above are reused).
# The stubs above leave cargo fingerprints for the workspace members, so
# touch every stubbed target to make the real sources look newer and force a
# rebuild of the workspace crates against the cached dependency artifacts.
COPY crates ./crates
RUN --mount=type=cache,id=peerless-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=peerless-cargo-git,target=/usr/local/cargo/git \
    touch crates/music/src/lib.rs crates/lyrics/src/lib.rs crates/engine/src/lib.rs \
          crates/apple/src/lib.rs crates/qobuz/src/lib.rs crates/db/src/lib.rs \
          crates/media/src/lib.rs crates/stream/src/lib.rs crates/server/src/lib.rs \
          crates/bot/src/lib.rs crates/bot/src/main.rs \
          crates/bot/src/bin/backfill_recording_mbids.rs \
 && cargo build --release --locked -p bot --bin bot --bin backfill_recording_mbids \
 && strip --strip-unneeded target/release/bot target/release/backfill_recording_mbids

# ==============================================================================
# Stage 3: Production runner
# ==============================================================================
FROM runtime AS runner
WORKDIR /app

# Set default production environment
ENV LOG_LEVEL=info

# Copy only the stripped runtime binary; configuration is supplied at runtime.
COPY --from=builder /app/target/release/bot ./bot
COPY --from=builder /app/target/release/backfill_recording_mbids ./backfill_recording_mbids

# Liveness probe. debian:bookworm-slim has no curl or wget, so this uses bash's
# /dev/tcp and bash builtins only.
COPY healthcheck.sh /app/healthcheck.sh

# Non-root user; bot-data holds the Telegram session and download scratch.
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin peerless \
 && mkdir -p /app/bot-data/downloads \
 && chown -R peerless:peerless /app \
 && chmod 0755 /app/bot /app/backfill_recording_mbids /app/healthcheck.sh

USER peerless

# Persistent storage volume for Telegram session and downloads
VOLUME ["/app/bot-data"]

# Start the bot (migrations run automatically at startup)
CMD ["./bot"]
