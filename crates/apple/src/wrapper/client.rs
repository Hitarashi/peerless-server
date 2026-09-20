//! Client for wrapper-lite HTTP API (ports 12340).
//!
//! Exposes:
//! - `/status` -> check available storefronts
//! - `/m3u8?adamId={id}` -> master playlist URL
//! - `/key?adamId={id}&uri={uri}` -> FairPlay key templates for Temari

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use tracing::debug;

/// A typed wrapper outcome that can be propagated to an optional rendition.
///
/// The valid-master variant retains provider-playlist provenance; the
/// non-EC-3 variant is only produced after a wrapper candidate has resolved a
/// successful stream response that cannot satisfy an Atmos request. A missing
/// `/m3u8` is converted to a permanent primary failure by the wrapper engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapperUnavailableReason {
    NoAtmosVariantInValidMaster,
    /// The wrapper cannot resolve a master playlist for this track.
    M3u8NotFound,
    /// A wrapper endpoint resolved successfully, but only returned a stream
    /// that cannot satisfy an Atmos request.
    NonEc3StreamForAtmos,
}

impl std::fmt::Display for WrapperUnavailableReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAtmosVariantInValidMaster => {
                formatter.write_str("No Dolby Atmos stream variant found in master playlist")
            }
            Self::M3u8NotFound => formatter.write_str("Wrapper /m3u8 returned HTTP 404"),
            Self::NonEc3StreamForAtmos => {
                formatter.write_str("Wrapper returned a non-EC-3 stream for Atmos request")
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WrapperError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Wrapper API error (code {code}): {message}")]
    Api { code: i64, message: String },
    #[error("wrapper authentication rejected (HTTP {status})")]
    Auth { status: u16 },
    #[error("Temari template error: {0}")]
    Template(String),
    /// The requested rendition is not present in the provider playlist.
    /// This is distinct from a malformed playlist or transport failure so
    /// optional renditions can be skipped without retrying.
    #[error("{0}")]
    Unavailable(WrapperUnavailableReason),
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Deserialize)]
struct StatusData {
    #[serde(default)]
    regions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct M3u8Data {
    m3u8: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateData {
    #[serde(default)]
    pub ctx: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub rcx: String,
    #[serde(default)]
    pub rax: String,
    #[serde(default)]
    pub rdx: String,
    #[serde(default)]
    pub r9: String,
    #[serde(default)]
    pub rbp: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    code: i64,
    #[serde(default)]
    msg: String,
    data: Option<T>,
}

#[derive(Clone, Debug)]
pub struct WrapperLiteClient {
    base_url: String,
    client: reqwest::Client,
}

impl WrapperLiteClient {
    pub fn new(base_url: impl Into<String>, api_key: Option<&str>) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("AlacBot/1.0 (wrapper-lite client)"),
        );
        if let Some(key) = api_key
            && !key.trim().is_empty()
            && let Ok(val) = HeaderValue::from_str(key.trim())
        {
            headers.insert("X-API-Key", val);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let base = base_url.into().trim_end_matches('/').to_owned();
        Self {
            base_url: base,
            client,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Check wrapper status and return supported storefronts (e.g. `["in"]`).
    pub async fn check_status(&self) -> Result<Vec<String>, WrapperError> {
        let url = format!("{}/status", self.base_url);
        debug!(url = %url, "Checking wrapper-lite status");
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(auth_or_http("Wrapper /status", status));
        }
        let text = resp.text().await?;
        let env: ApiResponse<StatusData> = serde_json::from_str(&text)?;
        if env.code != 0 {
            return Err(api_or_auth(env.code, env.msg));
        }
        Ok(env.data.map(|d| d.regions).unwrap_or_default())
    }

    /// Fetch master HLS playlist URL for given Adam track ID.
    pub async fn fetch_m3u8_url(&self, adam_id: &str) -> Result<String, WrapperError> {
        let url = format!("{}/m3u8?adamId={}", self.base_url, adam_id);
        debug!(adam_id = %adam_id, url = %url, "Fetching m3u8 URL from wrapper");
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(WrapperError::Unavailable(
                    WrapperUnavailableReason::M3u8NotFound,
                ));
            }
            return Err(auth_or_http("Wrapper /m3u8", status));
        }
        let text = resp.text().await?;
        let env: ApiResponse<M3u8Data> = serde_json::from_str(&text)?;
        if env.code != 0 {
            if env.code == 404 {
                return Err(WrapperError::Unavailable(
                    WrapperUnavailableReason::M3u8NotFound,
                ));
            }
            return Err(api_or_auth(env.code, env.msg));
        }
        let m3u8 = env
            .data
            .and_then(|d| d.m3u8)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| WrapperError::Message("Wrapper returned empty m3u8 URL".into()))?;
        Ok(m3u8)
    }

    /// Fetch the web playback AAC playlist URL (stores with no lossless
    /// HLS still expose the lossy playlist here).
    pub async fn fetch_webplayback(&self, adam_id: &str) -> Result<String, WrapperError> {
        let url = format!("{}/webplayback?adamId={}", self.base_url, adam_id);
        debug!(adam_id = %adam_id, url = %url, "Fetching web playback URL from wrapper");
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(auth_or_http("Wrapper /webplayback", status));
        }
        let text = resp.text().await?;
        let env: ApiResponse<M3u8Data> = serde_json::from_str(&text)?;
        if env.code != 0 {
            return Err(api_or_auth(env.code, env.msg));
        }
        let m3u8 = env
            .data
            .and_then(|d| d.m3u8)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                WrapperError::Message("Wrapper returned empty web playback URL".into())
            })?;
        Ok(m3u8)
    }

    /// Relay a Widevine license challenge through wrapper-lite `/license`.
    /// `challenge` and the returned license are base64 strings; `uri` is
    /// the `"<prefix>,<pssh>"` pair wrapper-lite forwards to Apple.
    pub async fn fetch_license(
        &self,
        adam_id: &str,
        challenge_b64: &str,
        uri: &str,
    ) -> Result<String, WrapperError> {
        let url = format!("{}/license", self.base_url);
        debug!(adam_id = %adam_id, url = %url, "Posting license challenge to wrapper");
        #[derive(Serialize)]
        struct LicenseRequest<'a> {
            challenge: &'a str,
            uri: &'a str,
            #[serde(rename = "adamId")]
            adam_id: &'a str,
        }
        let body = serde_json::to_string(&LicenseRequest {
            challenge: challenge_b64,
            uri,
            adam_id,
        })?;
        let resp = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            let truncated = truncate_error_body(&text);
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(WrapperError::Auth {
                    status: status.as_u16(),
                });
            }
            return Err(WrapperError::Message(format!(
                "Wrapper /license HTTP {}: {}",
                status, truncated
            )));
        }
        let text = resp.text().await?;
        #[derive(Deserialize)]
        struct LicenseData {
            #[serde(default)]
            license: String,
        }
        let env: ApiResponse<LicenseData> = serde_json::from_str(&text)?;
        if env.code != 0 {
            return Err(api_or_auth(env.code, env.msg));
        }
        let license = env
            .data
            .map(|d| d.license)
            .filter(|l| !l.is_empty())
            .ok_or_else(|| WrapperError::Message("Wrapper returned empty license".into()))?;
        Ok(license)
    }

    /// Fetch FairPlay key template from wrapper `/key` endpoint and instantiate `temari::rounds::Template`.
    pub async fn fetch_template(
        &self,
        adam_id: &str,
        uri: &str,
    ) -> Result<temari::rounds::Template, WrapperError> {
        let url = format!(
            "{}/key?adamId={}&uri={}",
            self.base_url,
            adam_id,
            urlencoding(uri)
        );
        debug!(adam_id = %adam_id, uri = %uri, "Fetching key template from wrapper");

        let mut last_error = None;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(400)).await;
                debug!(adam_id = %adam_id, uri = %uri, attempt = attempt, "Retrying key template fetch after transient error");
            }
            let resp = match self.client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(WrapperError::Http(e));
                    continue;
                }
            };
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                last_error = Some(WrapperError::Message(format!(
                    "Wrapper /key HTTP {} for uri {}: {}",
                    status, uri, text
                )));
                if status.is_server_error() {
                    continue;
                } else {
                    break;
                }
            }
            let text = match resp.text().await {
                Ok(t) => t,
                Err(e) => {
                    last_error = Some(WrapperError::Http(e));
                    continue;
                }
            };
            let env: ApiResponse<TemplateData> = match serde_json::from_str(&text) {
                Ok(e) => e,
                Err(e) => {
                    last_error = Some(WrapperError::Json(e));
                    continue;
                }
            };
            if env.code != 0 {
                last_error = Some(WrapperError::Api {
                    code: env.code,
                    message: env.msg,
                });
                if env.code >= 500 {
                    continue;
                } else {
                    break;
                }
            }
            let data = env.data.ok_or_else(|| {
                WrapperError::Message("Wrapper returned empty key template data".into())
            })?;

            let json_body = serde_json::to_string(&data)?;
            let template = temari::template::template_from_json(&json_body)
                .map_err(|err| WrapperError::Template(err.to_string()))?;
            return Ok(template);
        }

        Err(last_error.unwrap_or_else(|| WrapperError::Message("Key template fetch failed".into())))
    }
}

fn urlencoding(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len() * 3);
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric()
            || byte == b'-'
            || byte == b'_'
            || byte == b'.'
            || byte == b'~'
        {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{:02X}", byte));
        }
    }
    encoded
}

/// Keep error bodies bounded: 120 chars mirrors what the mirror endpoint
/// reports upstream, plenty for a diagnostic message.
fn truncate_error_body(text: &str) -> String {
    let mut out: String = text.chars().take(120).collect();
    if text.chars().count() > 120 {
        out.push('…');
    }
    out
}

/// 401/403 are authentication failures, not transport noise.
fn auth_or_http(what: &str, status: reqwest::StatusCode) -> WrapperError {
    let code = status.as_u16();
    if code == 401 || code == 403 {
        return WrapperError::Auth { status: code };
    }
    WrapperError::Message(format!("{what} HTTP {status}"))
}

/// API body codes share HTTP's authentication semantics.
fn api_or_auth(code: i64, message: String) -> WrapperError {
    if code == 401 || code == 403 {
        return WrapperError::Auth {
            status: u16::try_from(code).unwrap_or_default(),
        };
    }
    WrapperError::Api { code, message }
}
