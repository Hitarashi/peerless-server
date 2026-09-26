# Peerless Server

> **High-Performance Telegram Lossless Streaming Server & Ripping Engine**  
> Direct MTProto-backed lossless audio streaming (ALAC, FLAC, Dolby Atmos) with REST API, WebSockets, and Telegram Bot
> backend in Rust 2024.

---

## Overview

**Peerless Server** is a unified dual-engine media streaming platform designed to bridge lossless music decryption
mirrors with native client applications (such as [Peerless KMP](https://github.com/Hitarashi/Peerless)):

1. **Telegram MTProto Ripping & Archiving Bot**: Downloads, tags, and stores Apple Music (including ALAC and Dolby Atmos
   EC-3) and Qobuz (FLAC) tracks in a private Telegram dump channel, retaining ISRC metadata when available.
2. **Axum HTTP Lossless Streaming Server (`/api/v1`)**: Supports HTTP `206 Partial Content` byte-range audio requests
   from Telegram MTProto chunks, using auxiliary worker pools and in-memory LRU chunk caching, with playback
   synchronization over WebSockets.

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
│  │ - /api/v1/search (ISRC canonical results)           │  │
│  │ - /api/v1/assets/tracks/{id}/lyrics (Lyrics)        │  │
│  │ - /api/v1/integrations/lastfm (Encrypted AES-GCM)  │  │
│  │ - /api/v1/docs (OpenAPI 3.1 Scalar UI)              │  │
│  └─────────────────────────┬──────────────────────────┘  │
└────────────────────────────┼─────────────────────────────┘
                             ▼
               [Peerless Client (Android / iOS / Desktop)]
```

### Core Subsystems

- **Direct Media Streaming**: Serves track media from Telegram MTProto chunks over HTTP byte-range requests.
- **Stream Worker Pool**: Configured auxiliary bot tokens (`STREAM_WORKER_BOT_TOKENS`) fetch MTProto chunks
  (`upload.getFile`) for HTTP Range streaming; the pool uses the primary bot client when no workers are configured or
  the worker pool is unavailable.
- **Worker Circuit Breaker & Least-Loaded Dispatch**: Automatically quarantines workers on FloodWait or network drops,
  distributing load across healthy tokens.
- **Uniform Block Chunk Cache**: Fixed 512KB block LRU cache (`moka`) preventing duplicate Telegram downloads on seeks
  and scrubs.
- **Spotify Connect-Style WebSockets (`/api/v1/ws/playback`)**: Full-duplex playback synchronization hub fanning out
  track state, progress, and remote playback commands across connected clients.
- **Multi-Provider Synced Lyrics Engine (`crates/lyrics`)**: Aggregates word-by-word and line-synced lyrics from Apple
  Music TTML (`amll-ttml-db`), BetterLyrics, Paxsenix, Unison, NetEase, QQ Music, Kugou, Musixmatch, Spotify, YouTube
  Music, Binimum, and LRCLIB.
- **ISRC-Based Canonical Linkage**: Groups cached Apple Music and Qobuz tracks, and live Apple Music results, when ISRCs
  or matching track metadata are available.
- **Telegram-Gated Authentication & Onboarding**: Single-use OTP code (`/stream`) exchanged for an opaque 256-bit
  sliding refresh token. Base64 connection payloads allow 1-tap client onboarding.
- **Interactive OpenAPI 3.1 & Scalar Documentation**: Explore endpoints at `/api/v1/docs`; OpenAPI documents are also
  available at `/api/v1/docs.json` and `/api/v1/docs.yaml`.

---

## Workspace Crates

| Crate    | Responsibility                                                           |
|:---------|:-------------------------------------------------------------------------|
| `music`  | Provider-neutral music domain types.                                     |
| `lyrics` | Lyrics lookup, ranking, and rendering across providers.                  |
| `engine` | Provider-neutral ripping, orchestration, streaming, and tagging logic.   |
| `apple`  | Apple Music catalog, playlist, wrapper, and audio acquisition.           |
| `qobuz`  | Qobuz catalog, hosted/native adapters, and audio acquisition.            |
| `db`     | PostgreSQL models, migrations, persistence, and repositories.            |
| `media`  | Audio inspection, tagging, and spectrogram rendering.                    |
| `stream` | Telegram MTProto worker pool, chunk cache, and byte-range stream engine. |
| `server` | Axum API, playback synchronization, and OpenAPI documentation.           |
| `bot`    | Telegram bot commands and application startup.                           |

---

## API Endpoints Reference (`/api/v1`)

| Group            | Method     | Endpoint                                                        | Description                                             |
|:-----------------|:-----------|:----------------------------------------------------------------|:--------------------------------------------------------|
| **Auth**         | `POST`     | `/api/v1/auth/exchange`                                         | Exchange one-time Telegram OTP for opaque session token |
|                  | `POST`     | `/api/v1/auth/refresh`                                          | Slide 3-day expiration window forward                   |
|                  | `POST`     | `/api/v1/auth/logout`                                           | Revoke active session token                             |
|                  | `GET`      | `/api/v1/auth/me`                                               | Fetch authenticated user profile & sessions             |
|                  | `GET`      | `/api/v1/auth/me/avatar`                                        | Fetch the authenticated user's Telegram avatar          |
| **Streaming**    | `GET/POST` | `/api/v1/tracks/{id}/playback`                                  | Acquire short-lived signed stream ticket                |
|                  | `GET/HEAD` | `/api/v1/stream?ticket=...`                                     | HTTP byte-range streaming (206 for Range requests)      |
|                  | `GET`      | `/api/v1/ws/playback`                                           | Full-duplex WebSocket playback synchronization hub      |
| **Catalog**      | `GET`      | `/api/v1/search?q=...`                                          | Hybrid search (cached PostgreSQL + live catalog)        |
|                  | `GET`      | `/api/v1/tracks/{id}`                                           | Complete track metadata and audio specifications        |
|                  | `GET`      | `/api/v1/albums`                                                | Paginated list of cached albums                         |
|                  | `GET`      | `/api/v1/albums/{id}`                                           | Album tracks with cache resolution                      |
|                  | `GET`      | `/api/v1/artists/{name}/tracks`                                 | All cached tracks by an artist                          |
| **Tasks**        | `POST`     | `/api/v1/tasks/rip`                                             | Enqueue an on-demand ripping task                       |
|                  | `GET`      | `/api/v1/tasks`                                                 | List active server-owned rip tasks                      |
|                  | `DELETE`   | `/api/v1/tasks/{id}`                                            | Cancel an active rip task                               |
| **Assets**       | `GET`      | `/api/v1/assets/tracks/{id}/artwork`                            | Track album cover art redirect                          |
|                  | `GET`      | `/api/v1/assets/providers/{provider}/tracks/{track_id}/artwork` | Direct provider cover art proxy                         |
|                  | `GET`      | `/api/v1/assets/tracks/{id}/lyrics`                             | Synced TTML/LRC word-level lyrics                       |
| **Library**      | `GET`      | `/api/v1/me/favorites`                                          | List favorite tracks                                    |
|                  | `POST`     | `/api/v1/me/favorites/{track_id}`                               | Add a favorite track                                    |
|                  | `DELETE`   | `/api/v1/me/favorites/{track_id}`                               | Remove a favorite track                                 |
|                  | `GET/POST` | `/api/v1/me/playlists`                                          | List or create playlists                                |
|                  | `GET`      | `/api/v1/me/playlists/{id}`                                     | Fetch a playlist and its tracks                         |
|                  | `PUT`      | `/api/v1/me/playlists/{id}`                                     | Update a playlist                                       |
|                  | `DELETE`   | `/api/v1/me/playlists/{id}`                                     | Delete a playlist                                       |
| **Integrations** | `POST`     | `/api/v1/integrations/lastfm/login`                             | Connect Last.fm account (AES-256-GCM encrypted)         |
|                  | `GET`      | `/api/v1/integrations/lastfm/status`                            | Get Last.fm connection status                           |
|                  | `DELETE`   | `/api/v1/integrations/lastfm`                                   | Disconnect Last.fm account                              |
| **Docs**         | `GET`      | `/api/v1/docs`                                                  | Interactive OpenAPI 3.1 Scalar documentation            |
|                  | `GET`      | `/api/v1/docs.json`                                             | OpenAPI document in JSON format                         |
|                  | `GET`      | `/api/v1/docs.yaml`                                             | OpenAPI document in YAML format                         |
| **Other**        | `GET`      | `/open`                                                         | Open the client connection gateway                      |
|                  | `GET`      | `/api/v1/health`                                                | Health check                                            |

---

## Prerequisites

- [Rust](https://rustup.rs) (stable 1.85+ toolchain; Rust 2024 edition)
- [just](https://github.com/casey/just) (command task runner)
- [PostgreSQL](https://www.postgresql.org/) (with `pg_trgm` extension)
- **Telegram API Credentials**: `API_ID` & `API_HASH` from [my.telegram.org](https://my.telegram.org)
- **Primary Bot Token**: `BOT_TOKEN` from [@BotFather](https://t.me/BotFather)
- **Telegram Dump Channel**: Private channel accessible to the primary bot and any configured worker tokens

---

## Configuration (`.env`)

```env
# Required: Telegram API application ID.
API_ID=1234567
# Required: Telegram API application hash.
API_HASH=abcdef0123456789abcdef0123456789
# Required: Telegram bot token.
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ
# Required: Telegram administrator user ID.
ADMIN_ID=123456789
# Required: private Telegram dump channel ID.
DUMP_CHANNEL_ID=-1001234567890

# Required: PostgreSQL connection string.
DATABASE_URL=postgresql://user:password@localhost:5432/peerless

# Optional: comma-separated worker bot tokens; an empty/unset list falls back to the primary bot client.
STREAM_WORKER_BOT_TOKENS=bot_token_1,bot_token_2,bot_token_3
# Optional: overrides the stream server port saved in application settings.
# STREAM_SERVER_PORT=4444

# Optional: application log verbosity used to build its crate filters (default: info).
LOG_LEVEL=info
# Optional: tracing-subscriber filter directives; also used as the log-level fallback when LOG_LEVEL is unset.
# RUST_LOG=info

# Optional: used for signed stream tickets and encrypting Last.fm session keys; startup has a built-in default.
# APP_KEY=replace-with-a-private-secret
# Optional: encrypts stored worker sessions when APP_KEY is not set in the environment.
# SESSION_ENCRYPTION_KEY=replace-with-a-private-secret

# Optional: Apple wrapper endpoint; defaults to http://127.0.0.1:12340.
ALAC_WRAPPER_URL=http://127.0.0.1:12340
# Optional: API key for the Apple wrapper endpoint.
ALAC_WRAPPER_API_KEY=
# Optional: set to "endpoints" to use endpoint mode; otherwise native wrapper mode is used.
ALAC_WRAPPER_KIND=
# Optional: primary Apple mirror URL; set together with ALAC_API_KEY to override mirror selection.
ALAC_MIRROR_URL=
# Optional: API key paired with ALAC_MIRROR_URL for the primary Apple mirror override.
ALAC_API_KEY=
# Optional: Apple acquisition retry rounds (default: 3).
ALAC_STREAM_RETRIES=
# Optional: Apple acquisition retry base delay in milliseconds (default: 2000; capped at 30000).
ALAC_STREAM_RETRY_BASE_MS=
# Optional: maximum rip/upload retries (default: 3).
ALAC_MAX_RETRIES=
# Optional: rip/upload retry base delay in milliseconds (default: 2000).
ALAC_RETRY_BASE_MS=

# Optional: URL of the hosted Qobuz backend; setting it enables that backend.
QOBUZ_BACKEND_URL=
# Optional: authentication key for the hosted Qobuz backend.
QOBUZ_BACKEND_KEY=
# Optional: user authorization token for the native Qobuz adapter.
QOBUZ_USER_AUTH_TOKEN=
# Optional: application ID for the native Qobuz adapter.
QOBUZ_APP_ID=
# Optional: application secret used with the native Qobuz application ID.
QOBUZ_APP_SECRET=

# Optional: Last.fm API key; required with LASTFM_SHARED_SECRET to connect Last.fm accounts.
LASTFM_API_KEY=
# Optional: Last.fm shared secret; required with LASTFM_API_KEY to connect Last.fm accounts.
LASTFM_SHARED_SECRET=

# Optional: Spotify access token used by the Spotify lyrics provider.
SPOTIFY_ACCESS_TOKEN=
# Optional: Spotify client token used with SPOTIFY_ACCESS_TOKEN for lyrics lookups.
SPOTIFY_CLIENT_TOKEN=
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
docker build -t alac-bot:latest .
docker run -d --name peerless-server \
  --env-file .env \
  -p 4444:4444 \
  -v peerless-server-data:/app/bot-data \
  alac-bot:latest
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

## Client Token Onboarding

1. With the public streaming URL configured, an authorized user sends `/stream` to the bot in a private chat. The bot
   replies with a single-use code and an `/open?code=...` link.
2. The link opens the connection gateway, which packages the server URL and code into the Base64 client connection
   payload. To exchange the code directly, send:

   ```bash
   curl -X POST https://server.example/api/v1/auth/exchange \
     -H 'Content-Type: application/json' \
     -d '{"code":"ABC-XYZ","device_name":"Pixel 8 Pro","platform":"android"}'
   ```

   `device_name` and `platform` are optional. A successful response has this shape (the token fields contain the same
   issued token):

   ```json
   {
     "token_type": "Bearer",
     "token": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
     "access_token": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
     "refresh_token": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
     "expires_in": 259200,
     "expires_at": "2026-09-29T12:00:00Z",
     "expires_at_unix": 1790683200,
     "user": {
       "telegram_id": 123456789,
       "name": "Example User",
       "username": "example",
       "first_name": "Example",
       "last_name": "User"
     }
   }
   ```

3. Use the returned `access_token` as a Bearer token on subsequent authenticated API calls, for example
   `Authorization: Bearer <access_token>`.

---

## Testing & Quality Assurance

```bash
# Type check workspace
just check

# Run full test suite; just supplies postgres://admin:password@localhost:5432/alac_bot_test by default
just test

# Optional: override the test database URL
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/peerless_test just test

# Lint with clippy
just clippy

# Format code (nightly toolchain)
just fmt
```

---

## License

This project is licensed under the MIT License.
