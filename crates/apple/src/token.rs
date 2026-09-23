//! Apple Music Developer Token scraper and AMP API transport.
//!
//! Provides cached, auto-refreshing Apple Music Developer Tokens by scraping
//! `https://music.apple.com/us/browse` index bundles. Shared across catalog and
//! playlist modules to eliminate redundant scraping.

use std::{
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::playlist::{
    PlaylistHttp, PlaylistHttpError, ReqwestPlaylistHttp, auth_headers, ua_header,
};

#[derive(Debug, Default)]
struct TokenCache {
    token: Option<String>,
    expires_at_ms: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Thread-safe manager for live Apple Music developer tokens.
#[derive(Debug)]
pub struct DeveloperTokenProvider<H: PlaylistHttp = ReqwestPlaylistHttp> {
    http: H,
    token_cache: Mutex<TokenCache>,
    token_refresh: tokio::sync::Mutex<()>,
}

impl<H: PlaylistHttp> DeveloperTokenProvider<H> {
    pub fn new(http: H) -> Self {
        Self {
            http,
            token_cache: Mutex::new(TokenCache::default()),
            token_refresh: tokio::sync::Mutex::new(()),
        }
    }

    /// Retrieves and caches the Apple Music Web Client developer token.
    ///
    /// Scraped tokens are cached for 24 hours. Callers that receive a 401 or 403
    /// should call [`invalidate_developer_token`] before retrying.
    pub async fn get_developer_token(&self) -> Result<String, String> {
        let _guard = self.token_refresh.lock().await;
        let now = now_ms();
        {
            let cache = self.token_cache.lock().expect("token cache poisoned");
            if let Some(token) = &cache.token
                && cache.expires_at_ms > now
            {
                return Ok(token.clone());
            }
        }

        match self.scrape_token().await {
            Ok(token) => {
                let mut cache = self.token_cache.lock().expect("token cache poisoned");
                cache.token = Some(token.clone());
                cache.expires_at_ms = now + 24 * 60 * 60 * 1000;
                tracing::debug!("Extracted live Apple Music developer token");
                Ok(token)
            }
            Err(err) => Err(err),
        }
    }

    /// Drops the cached token so the next call performs a fresh scrape.
    pub fn invalidate_developer_token(&self) {
        let mut cache = self.token_cache.lock().expect("token cache poisoned");
        cache.token = None;
        cache.expires_at_ms = 0;
    }

    /// Queries the Apple Music AMP API (`amp-api.music.apple.com`) with authentication,
    /// transparently retrying once on 401/403 with a freshly scraped token.
    pub async fn fetch_amp(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<String, PlaylistHttpError> {
        let mut token = self
            .get_developer_token()
            .await
            .map_err(PlaylistHttpError::Network)?;

        let body = match self.http.get(url, &auth_headers(&token), timeout).await {
            Ok(b) => b,
            Err(PlaylistHttpError::Status(401 | 403)) => {
                self.invalidate_developer_token();
                token = self
                    .get_developer_token()
                    .await
                    .map_err(PlaylistHttpError::Network)?;
                self.http.get(url, &auth_headers(&token), timeout).await?
            }
            Err(e) => return Err(e),
        };

        Ok(body)
    }

    async fn scrape_token(&self) -> Result<String, String> {
        let browse = self
            .http
            .get(
                "https://music.apple.com/us/browse",
                &[ua_header()],
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| e.to_string())?;

        let asset = find_asset_path(&browse).ok_or("no index asset in browse page")?;
        let js = self
            .http
            .get(
                &format!("https://music.apple.com{asset}"),
                &[ua_header()],
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| e.to_string())?;

        if let Some(var_name) = find_developer_token_var(&js)
            && let Some(value) = find_var_assignment(&js, &var_name)
        {
            return Ok(value);
        }

        if let Some(jwt) = find_direct_jwt(&js) {
            return Ok(jwt);
        }

        Err("no token found in asset".to_string())
    }
}

pub(crate) fn find_asset_path(html: &str) -> Option<String> {
    let pat = "/assets/index";
    let mut search_from = 0;
    while let Some(rel) = html[search_from..].find(pat) {
        let start = search_from + rel;
        let after = &html[start + pat.len()..];
        let mut chars = after.chars();
        if let Some(sep) = chars.next()
            && (sep == '~' || sep == '-' || sep == '.' || sep == '_')
        {
            let hash_len = chars
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .count();
            if hash_len > 0 {
                let end = start + pat.len() + 1 + hash_len;
                if html[end..].starts_with(".js") {
                    return Some(html[start..end + 3].to_string());
                }
            }
        }
        search_from = start + 1;
    }
    None
}

pub(crate) fn find_developer_token_var(js: &str) -> Option<String> {
    let pat = "developerToken:";
    let rel = js.find(pat)?;
    let after = &js[rel + pat.len()..];
    let name: String = after
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '$' || *c == '_')
        .collect();
    if name.is_empty() { None } else { Some(name) }
}

pub(crate) fn find_var_assignment(js: &str, var_name: &str) -> Option<String> {
    let mut search_from = 0;
    while let Some(rel) = js[search_from..].find(var_name) {
        let start = search_from + rel;
        let rest = &js[start + var_name.len()..];
        let ws_len = rest.chars().take_while(|c| c.is_whitespace()).count();
        let rest = &rest[ws_len..];
        if let Some(stripped) = rest.strip_prefix('=') {
            let ws_len = stripped.chars().take_while(|c| c.is_whitespace()).count();
            let rest = &stripped[ws_len..];
            if let Some(after_quote) = rest.strip_prefix('"')
                && let Some(end) = after_quote.find('"')
            {
                return Some(after_quote[..end].to_string());
            }
        }
        search_from = start + 1;
    }
    None
}

pub(crate) fn find_direct_jwt(js: &str) -> Option<String> {
    let start = js.find("eyJh")?;
    let rest = &js[start..];
    let mut end = 0;
    let mut dots = 0;
    for (i, ch) in rest.char_indices() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            end = i + ch.len_utf8();
        } else if ch == '.' && dots < 2 {
            dots += 1;
            end = i + 1;
        } else {
            break;
        }
    }
    if dots == 2 && end > 0 {
        Some(rest[..end].to_string())
    } else {
        None
    }
}
