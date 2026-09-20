//! HTTP seam used by the audio stream transport.
//!
//! Keeping reqwest behind these two traits makes all streaming behavior
//! testable without a network connection.

use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

/// Chrome user-agent used by the original implementation.
pub const CHROME_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/145.0.0.0";

/// An audio response whose body remains a live stream.
pub struct StreamHttpResponse {
    pub status: u16,
    pub codec: Option<String>,
    pub bit_depth: Option<String>,
    pub sample_rate: Option<String>,
    pub content_length: Option<u64>,
    pub body: Option<ByteStream>,
}

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, StreamBodyError>> + Send>>;

#[derive(Clone, Debug, thiserror::Error)]
pub enum StreamBodyError {
    #[error("{0}")]
    Network(String),
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum StreamHttpError {
    #[error("request timed out")]
    Timeout { elapsed_ms: u64 },
    #[error("request cancelled")]
    Cancelled,
    #[error("{0}")]
    Network(String),
}

/// Streaming GET for an audio endpoint. The timeout covers headers (the
/// handshake) only; it must not terminate a long-running audio body.
pub trait StreamHttp: Send + Sync {
    fn fetch(
        &self,
        url: &str,
        api_key: Option<&str>,
        handshake_timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> impl Future<Output = Result<StreamHttpResponse, StreamHttpError>> + Send;
}

/// Production reqwest adapter.
#[derive(Clone)]
pub struct ReqwestHttp {
    client: reqwest::Client,
}

impl Default for ReqwestHttp {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestHttp {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl StreamHttp for ReqwestHttp {
    async fn fetch(
        &self,
        url: &str,
        api_key: Option<&str>,
        handshake_timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> Result<StreamHttpResponse, StreamHttpError> {
        let mut request = self.client.get(url).header("User-Agent", CHROME_USER_AGENT);
        if let Some(api_key) = api_key {
            request = request.header("X-API-Key", api_key);
        }
        // Deliberately do not use reqwest's per-request timeout here: it also
        // applies while the body is being consumed.
        let started = Instant::now();
        let send_future = tokio::time::timeout(handshake_timeout, request.send());
        let response = if let Some(signal) = signal {
            if signal.is_cancelled() {
                return Err(StreamHttpError::Cancelled);
            }
            tokio::select! {
                _ = signal.cancelled() => return Err(StreamHttpError::Cancelled),
                result = send_future => result,
            }
        } else {
            send_future.await
        };
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                if error.is_timeout() {
                    return Err(StreamHttpError::Timeout {
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    });
                }
                return Err(StreamHttpError::Network(error.to_string()));
            }
            Err(_) => {
                return Err(StreamHttpError::Timeout {
                    elapsed_ms: started.elapsed().as_millis() as u64,
                });
            }
        };

        let headers = response.headers();
        let codec = headers
            .get("x-codec")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bit_depth = headers
            .get("x-bitdepth")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let sample_rate = headers
            .get("x-samplerate")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let status = response.status().as_u16();
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let body = Some(Box::pin(
            response
                .bytes_stream()
                .map(|result| result.map_err(|error| StreamBodyError::Network(error.to_string()))),
        ) as ByteStream);
        Ok(StreamHttpResponse {
            status,
            codec,
            bit_depth,
            sample_rate,
            content_length,
            body,
        })
    }
}
