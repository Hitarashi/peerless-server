use std::{
    future::Future,
    time::{Duration, Instant},
};

use tokio_util::sync::CancellationToken;

pub const CHROME_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/145.0.0.0";

#[derive(Clone, Debug, thiserror::Error)]
pub enum MirrorHttpError {
    #[error("request timed out")]
    Timeout { elapsed_ms: u64 },
    #[error("request cancelled")]
    Cancelled,
    #[error("HTTP {0}")]
    Status(u16),
    #[error("{0}")]
    Network(String),
}

pub trait MirrorHttp: Send + Sync {
    fn get(
        &self,
        url: &str,
        headers: &[(&str, String)],
        timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> impl Future<Output = Result<String, MirrorHttpError>> + Send;
}

#[derive(Clone)]
pub struct ReqwestMirrorHttp {
    client: reqwest::Client,
}

impl ReqwestMirrorHttp {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestMirrorHttp {
    fn default() -> Self {
        Self::new()
    }
}

impl MirrorHttp for ReqwestMirrorHttp {
    async fn get(
        &self,
        url: &str,
        headers: &[(&str, String)],
        timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> Result<String, MirrorHttpError> {
        let mut request = self.client.get(url).header("User-Agent", CHROME_USER_AGENT);
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let request = request.timeout(timeout);
        let started = Instant::now();
        let request_future = async {
            let response = request.send().await.map_err(|error| {
                if error.is_timeout() {
                    MirrorHttpError::Timeout {
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    }
                } else {
                    MirrorHttpError::Network(error.to_string())
                }
            })?;
            let status = response.status().as_u16();
            if !(200..300).contains(&status) {
                return Err(MirrorHttpError::Status(status));
            }
            response.text().await.map_err(|error| {
                if error.is_timeout() {
                    MirrorHttpError::Timeout {
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    }
                } else {
                    MirrorHttpError::Network(error.to_string())
                }
            })
        };

        if let Some(signal) = signal {
            if signal.is_cancelled() {
                return Err(MirrorHttpError::Cancelled);
            }
            tokio::select! {
                _ = signal.cancelled() => Err(MirrorHttpError::Cancelled),
                result = request_future => result,
            }
        } else {
            request_future.await
        }
    }
}
