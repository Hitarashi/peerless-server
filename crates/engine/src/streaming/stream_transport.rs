//! Audio stream endpoint validation and body handling.

use std::{fmt, sync::Arc, time::Duration};

use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use super::{
    http::{ByteStream, StreamHttp, StreamHttpError},
    source_id::SourceId,
};
use crate::{
    limits::{MAX_AUDIO_BYTES, MAX_ERROR_BODY_BYTES},
    orchestrator::types::RipActivity,
};

pub struct FetchEndpointOptions {
    pub stream_url: String,
    pub api_key: Option<String>,
    pub source: SourceId,
    pub signal: Option<CancellationToken>,
    pub timeout: Duration,
}

// Field names follow the approved design; `source` collides with thiserror's
// reserved error-source field, so Display is implemented by hand.
#[derive(Debug, Clone)]
pub enum StreamError {
    Message(String),
    /// A provider has confirmed that the requested stream cannot be
    /// acquired.  Unlike an ordinary technical failure, this must not consume
    /// retry budget.
    Permanent(String),
    /// The requested optional rendition is known not to exist. This is
    /// distinct from a transport or provider failure so callers can skip
    /// the rendition without consuming retry budget.
    Unavailable(String),
    Timeout {
        source: SourceId,
        secs: u64,
    },
    Authentication {
        source: SourceId,
    },
    SourceOffline {
        source: SourceId,
    },
    PlaylistParse {
        which: &'static str,
        detail: String,
    },
    License {
        detail: String,
    },
    Decrypt {
        detail: String,
    },
    IncompleteBody {
        source: SourceId,
        expected: u64,
        received: u64,
    },
    LimitExceeded {
        limit_mib: u64,
    },
    Cancelled,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) => formatter.write_str(message),
            Self::Permanent(message) => formatter.write_str(message),
            Self::Unavailable(reason) => formatter.write_str(reason),
            Self::Timeout { source, secs } => {
                write!(
                    formatter,
                    "stream handshake timed out after {secs}s on {source}"
                )
            }
            Self::Authentication { source } => {
                write!(formatter, "authentication failed on {source}")
            }
            Self::SourceOffline { source } => write!(formatter, "{source} is offline"),
            Self::PlaylistParse { which, detail } => {
                write!(formatter, "{which} playlist parse failed: {detail}")
            }
            Self::License { detail } => write!(formatter, "license error: {detail}"),
            Self::Decrypt { detail } => write!(formatter, "decrypt failed: {detail}"),
            Self::IncompleteBody {
                source,
                expected,
                received,
            } => write!(
                formatter,
                "incomplete audio body from {source}: expected {expected} bytes, received {received}"
            ),
            Self::LimitExceeded { limit_mib } => {
                write!(formatter, "audio stream exceeds the {limit_mib} MiB limit")
            }
            Self::Cancelled => formatter.write_str("Download was cancelled"),
        }
    }
}

impl std::error::Error for StreamError {}

impl StreamError {
    fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

pub struct AudioStreamSource {
    pub stream: ByteStream,
    pub source: SourceId,
    pub codec: String,
    pub bit_depth: u32,
    pub sample_rate: u32,
    /// Response `content-length`; `None` or `Some(0)` mean "unknown" for
    /// progress totals.
    pub content_length: Option<u64>,
}

impl fmt::Debug for AudioStreamSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AudioStreamSource")
            .field("source", &self.source)
            .field("codec", &self.codec)
            .field("bit_depth", &self.bit_depth)
            .field("sample_rate", &self.sample_rate)
            .field("content_length", &self.content_length)
            .finish_non_exhaustive()
    }
}

pub type ProgressCallback = Arc<dyn Fn(RipActivity) + Send + Sync>;

/// Transport over an injectable streaming adapter.
pub struct StreamTransport<H: StreamHttp> {
    http: H,
}

impl<H: StreamHttp> StreamTransport<H> {
    pub fn new(http: H) -> Self {
        Self { http }
    }

    pub fn http(&self) -> &H {
        &self.http
    }

    pub async fn fetch_endpoint(
        &self,
        options: FetchEndpointOptions,
    ) -> Result<AudioStreamSource, StreamError> {
        let FetchEndpointOptions {
            stream_url,
            api_key,
            source,
            signal,
            timeout,
        } = options;
        if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return Err(StreamError::Cancelled);
        }
        let response = self
            .http
            .fetch(&stream_url, api_key.as_deref(), timeout, signal.as_ref())
            .await
            .map_err(|error| self.map_http_error(error, timeout, &source))?;

        if !(200..300).contains(&response.status) {
            let text = if let Some(mut body) = response.body {
                collect_body(&mut body).await
            } else {
                String::new()
            };
            return Err(StreamError::message(format!(
                "HTTP {} on {source}: {}",
                response.status,
                text.chars().take(120).collect::<String>()
            )));
        }

        let body = response
            .body
            .ok_or_else(|| StreamError::message(format!("Empty body from {source}")))?;
        if response
            .content_length
            .is_some_and(|length| length > MAX_AUDIO_BYTES)
        {
            return Err(StreamError::LimitExceeded {
                limit_mib: MAX_AUDIO_BYTES / (1024 * 1024),
            });
        }
        // Malformed values parse as documented defaults instead of
        // failing, which only ever happens with misbehaving mirrors.
        let bit_depth = response
            .bit_depth
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or(24);
        let sample_rate = response
            .sample_rate
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or(96_000);
        Ok(AudioStreamSource {
            stream: body,
            source,
            codec: response.codec.unwrap_or_else(|| "alac".to_owned()),
            bit_depth,
            sample_rate,
            content_length: response.content_length,
        })
    }

    fn map_http_error(
        &self,
        error: StreamHttpError,
        timeout: Duration,
        source: &SourceId,
    ) -> StreamError {
        match error {
            StreamHttpError::Timeout { .. } => StreamError::Timeout {
                source: source.clone(),
                secs: u64::try_from(timeout.as_millis() / 1000).unwrap_or(u64::MAX),
            },
            StreamHttpError::Cancelled => StreamError::Cancelled,
            StreamHttpError::Network(message) => StreamError::message(message),
        }
    }
}

async fn collect_body(body: &mut ByteStream) -> String {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        if let Ok(chunk) = chunk {
            let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(bytes.len());
            bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            if bytes.len() >= MAX_ERROR_BODY_BYTES {
                break;
            }
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}
