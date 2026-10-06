# syntax=docker/dockerfile:1.7

FROM docker.io/debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN printf 'precedence ::ffff:0:0/96 100\n' >> /etc/gai.conf

FROM docker.io/rust:1-bookworm AS builder
WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates binutils \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/music/Cargo.toml crates/music/Cargo.toml
COPY crates/engine/Cargo.toml crates/engine/Cargo.toml
COPY crates/apple/Cargo.toml crates/apple/Cargo.toml
COPY crates/db/Cargo.toml crates/db/Cargo.toml
COPY crates/media/Cargo.toml crates/media/Cargo.toml
COPY crates/stream/Cargo.toml crates/stream/Cargo.toml
COPY crates/server/Cargo.toml crates/server/Cargo.toml
COPY crates/bot/Cargo.toml crates/bot/Cargo.toml
RUN --mount=type=cache,id=peerless-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=peerless-cargo-git,target=/usr/local/cargo/git \
    mkdir -p crates/music/src crates/engine/src crates/apple/src \
             crates/db/src crates/media/src crates/stream/src \
             crates/server/src crates/bot/src \
 && echo 'pub fn _stub() {}' > crates/music/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/engine/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/apple/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/db/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/media/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/stream/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/server/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/bot/src/lib.rs \
 && echo 'fn main() {}' > crates/bot/src/main.rs \
 && cargo build --release --locked -p bot --bin bot \
 && rm -rf crates

COPY crates ./crates
RUN --mount=type=cache,id=peerless-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=peerless-cargo-git,target=/usr/local/cargo/git \
    touch crates/music/src/lib.rs crates/engine/src/lib.rs \
          crates/apple/src/lib.rs crates/db/src/lib.rs \
          crates/media/src/lib.rs crates/stream/src/lib.rs crates/server/src/lib.rs \
          crates/bot/src/lib.rs crates/bot/src/main.rs \
 && cargo build --release --locked -p bot --bin bot \
 && strip --strip-unneeded target/release/bot

FROM runtime AS runner
WORKDIR /app

ENV LOG_LEVEL=info

COPY --from=builder /app/target/release/bot ./bot

COPY healthcheck.sh /app/healthcheck.sh

RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin peerless \
 && mkdir -p /app/bot-data/downloads \
 && chown -R peerless:peerless /app \
 && chmod 0755 /app/bot /app/healthcheck.sh

USER peerless

VOLUME ["/app/bot-data"]

CMD ["./bot"]
