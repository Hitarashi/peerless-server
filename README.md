# Peerless Server

> **High-Performance Telegram Lossless Streaming Server & Ripping Engine**  
> Direct MTProto-backed lossless audio streaming (ALAC, FLAC, Dolby Atmos) with REST API, WebSockets, and Telegram Bot
> backend in Rust 2024.

---

## Overview

**Peerless Server** is a unified dual-engine media streaming platform designed to bridge lossless music decryption
mirrors with native client applications (such as [Peerless KMP](https://github.com/Hitarashi/Peerless)):

1. **Telegram MTProto Ripping & Archiving Bot**: Downloads, tags, and stores bit-perfect Apple Music (ALAC up to
   24-bit/192kHz, Dolby Atmos EC-3) and Qobuz (FLAC up to 24-bit/192kHz) tracks directly into a private Telegram cloud
   dump channel with universal ISRC indexing.
2. **Axum HTTP Lossless Streaming Server (`/api/v1`)**: Delivers bit-perfect HTTP `206 Partial Content` Range audio
   streams directly from Telegram MTProto chunks with zero disk writes, auxiliary worker pools, in-memory LRU chunk
   caching, and full-duplex WebSocket playback synchronization.

---

## Architecture & Features

```
┌──────────────────────────────────────────────────────────┐
│                   Telegram Ecosystem                     │
│  [Private Dump Channel]  ◄───►  [Primary Bot / Ripping]   │
│            ▲                                             │
└────────────┼─────────────────────────────────────────────┘
             │ MTProto Chunks (upload.getFile)
┌────────────┴─────────────────────────────────────────────┐
│                 Peerless Server (Rust)                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │ StreamWorkerPool (Dedicated MTProto Bot Tokens)    │  │
│  │ ChunkCache (512KB Uniform Blocks, LRU Moka)        │  │
│  │ StreamPipe (Async Chunk Prefetch & Backpressure)   │  │
│  └─────────────────────────┬──────────────────────────┘  │
│                            │ HTTP 206 Partial Content    │
│  ┌─────────────────────────┴──────────────────────────┐  │
│  │ Axum Web Server & Playback Sync Hub                │  │
│  │ - /api/v1/stream (HMAC signed tickets)             │  │
│  │ - /api/v1/ws/playback (Spotify Connect WebSockets) │  │
│  │ - /api/v1/search & /catalog (ISRC Canonical DSU)   │  │
│  │ - /api/v1/assets/lyrics (Multi-Provider Aggregator)│  │
│  │ - /api/v1/integrations/lastfm (Encrypted AES-GCM)  │  │
│  │ - /api/docs (OpenAPI 3.1 Scalar UI)                │  │
│  └─────────────────────────┬──────────────────────────┘  │
└────────────────────────────┼─────────────────────────────┘
                             ▼
               [Peerless Client (Android / iOS / Desktop)]
```

### Core Subsystems

- **Direct Lossless Passthrough**: Bit-perfect streaming from Telegram MTProto to HTTP `206 Partial Content` with zero
  CPU transcoding.
- **Dedicated Stream Worker Pool**: Auxiliary bot tokens (`STREAM_WORKER_BOT_TOKENS`) exclusively fetch MTProto chunks
  (`upload.getFile`) for HTTP Range streaming, preventing FloodWait on the primary bot.
- **Worker Circuit Breaker & Least-Loaded Dispatch**: Automatically quarantines workers on FloodWait or network drops,
  distributing load across healthy tokens.
- **Uniform Block Chunk Cache**: Fixed 512KB block LRU cache (`moka`) preventing duplicate Telegram downloads on seeks
  and scrubs.
- **Spotify Connect-Style WebSockets (`/api/v1/ws/playback`)**: Full-duplex playback synchronization hub fanning out
  track state, progress, and remote playback commands (<100ms latency) across connected clients.
- **Multi-Provider Synced Lyrics Engine (`crates/lyrics`)**: Aggregates word-by-word and line-synced lyrics from Apple
  Music TTML (`amll-ttml-db`), NetEase, QQ Music, Kugou, Musixmatch, Spotify, YouTube Music, Binimum, and LRCLIB.
- **ISRC Universal Canonical Linkage**: 99.98% coverage across cached tracks, linking Apple Music and Qobuz renditions
  into unified canonical entities.
- **Telegram-Gated Authentication & Onboarding**: Single-use OTP code (`/stream`) exchanged for an opaque 256-bit
  sliding refresh token. Base64 connection payloads allow 1-tap client onboarding.
- **Interactive OpenAPI 3.1 & Scalar Documentation**: Explore and test all endpoints interactively at `/api/docs`.

---

## API Endpoints Reference (`/api/v1`)

| Group            | Method     | Endpoint                                           | Description                                             |
|:-----------------|:-----------|:---------------------------------------------------|:--------------------------------------------------------|
| **Auth**         | `POST`     | `/api/v1/auth/exchange`                            | Exchange one-time Telegram OTP for opaque session token |
|                  | `POST`     | `/api/v1/auth/refresh`                             | Slide 3-day expiration window forward                   |
|                  | `POST`     | `/api/v1/auth/logout`                              | Revoke active session token                             |
|                  | `GET`      | `/api/v1/auth/me`                                  | Fetch authenticated user profile & sessions             |
| **Streaming**    | `GET/POST` | `/api/v1/tracks/{id}/playback`                     | Acquire short-lived signed stream ticket                |
|                  | `GET/HEAD` | `/api/v1/stream?ticket=...`                        | HTTP 206 Partial Content Range streaming                |
|                  | `GET`      | `/api/v1/ws/playback`                              | Full-duplex WebSocket playback synchronization hub      |
| **Catalog**      | `GET`      | `/api/v1/search?q=...`                             | Hybrid search (cached PostgreSQL + live catalog)        |
|                  | `GET`      | `/api/v1/tracks/{id}`                              | Complete track metadata and audio specifications        |
|                  | `GET`      | `/api/v1/albums`                                   | Paginated list of cached albums                         |
|                  | `GET`      | `/api/v1/albums/{id}`                              | Album tracks with cache resolution                      |
|                  | `GET`      | `/api/v1/artists/{name}/tracks`                    | All cached tracks by an artist                          |
| **Tasks**        | `POST`     | `/api/v1/tasks/rip`                                | Enqueue an on-demand ripping task                       |
|                  | `GET`      | `/api/v1/tasks/{id}/events`                        | Server-Sent Events (SSE) live ripping progress          |
| **Assets**       | `GET`      | `/api/v1/assets/tracks/{id}/artwork`               | Track album cover art redirect                          |
|                  | `GET`      | `/api/v1/assets/providers/{p}/tracks/{id}/artwork` | Direct provider cover art proxy                         |
|                  | `GET`      | `/api/v1/assets/tracks/{id}/lyrics`                | Synced TTML/LRC word-level lyrics                       |
| **Library**      | `GET/POST` | `/api/v1/me/favorites`                             | List or bookmark favorite tracks                        |
| **Integrations** | `POST`     | `/api/v1/integrations/lastfm/login`                | Connect Last.fm account (AES-256-GCM encrypted)         |
| **Docs**         | `GET`      | `/api/docs`                                        | Interactive OpenAPI 3.1 Scalar documentation            |

---

## Prerequisites

- [Rust](https://rustup.rs) (stable 1.85+ toolchain; Rust 2024 edition)
- [just](https://github.com/casey/just) (command task runner)
- [PostgreSQL](https://www.postgresql.org/) (with `pg_trgm` extension)
- **Telegram API Credentials**: `API_ID` & `API_HASH` from [my.telegram.org](https://my.telegram.org)
- **Primary Bot Token**: `BOT_TOKEN` from [@BotFather](https://t.me/BotFather)
- **Telegram Dump Channel**: Private channel where primary bot and worker tokens are administrators

---

## Configuration (`.env`)

```env
# Telegram Core Credentials
API_ID=1234567
API_HASH=abcdef0123456789abcdef0123456789
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ
ADMIN_ID=123456789
DUMP_CHANNEL_ID=-1001234567890

# Dedicated Streaming Worker Bot Tokens (comma-separated, recommended 3-5 tokens)
STREAM_WORKER_BOT_TOKENS=bot_token_1,bot_token_2,bot_token_3

# Axum HTTP Streaming Server Port & Security
STREAM_SERVER_PORT=4444
APP_KEY=generate_a_secure_32_byte_hex_key_here

# PostgreSQL Database Connection
DATABASE_URL=postgresql://user:password@localhost:5432/peerless

# Tracing Log Level (trace | debug | info | warn | error)
LOG_LEVEL=info

# (Optional) Primary Decryption Mirror & Wrapper Overrides
ALAC_WRAPPER_URL=http://127.0.0.1:12340
```

---

## Quick Start

### 1. Build & Run

Database migrations run automatically at startup:

```bash
# Clone repository
git clone https://github.com/Hitarashi/peerless-server.git
cd peerless-server
cp .env.example .env

# Run in development mode
just run

# Or compile and run release binary
just release
./target/release/bot
```

### 2. Docker Deployment

```bash
docker build -t peerless-server .
docker run -d --name peerless-server \
  --env-file .env \
  -p 4444:4444 \
  -v peerless-server-data:/app/bot-data \
  peerless-server
```

---

## Bot Commands

| Command           | Description                                                                         |
|:------------------|:------------------------------------------------------------------------------------|
| `/stream`         | Generate a single-use OTP and Base64 Connection Payload for the Peerless client app |
| `/get <link>`     | Download and archive track or album; multi-track albums delivered as ZIPs           |
| `/search <query>` | Interactive search with inline buttons across cached and live catalogs              |
| `/info <link>`    | Display track metadata, audio codec, and cache availability                         |
| `/status`         | Active ripping downloads with live cancellation controls                            |
| `/spec`           | Generate an audiophile FFT spectrogram from replied audio                           |
| `/settings`       | Operational toggles and dynamic stream URL configuration                            |
| `/index`          | Reconcile dump channel messages, captions, and ISRCs with PostgreSQL                |
| `/export`         | Export compressed PostgreSQL backup archive                                         |

---

## Testing & Quality Assurance

```bash
# Type check workspace
just check

# Run full test suite (requires TEST_DATABASE_URL)
export TEST_DATABASE_URL=postgresql://user:password@localhost:5432/peerless_test
just test

# Lint with clippy
just clippy

# Format code (nightly toolchain)
just fmt
```

---

## License

This project is licensed under the MIT License.
