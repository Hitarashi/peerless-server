use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::Deserialize;

use crate::{ServerState, error::ServerError};

static GATEWAY_TEMPLATE: &str = include_str!("gateway.html");

#[derive(Debug, Deserialize)]
pub struct OpenQuery {
    pub code: Option<String>,
}

pub fn render_gateway_page(data: &str) -> String {
    GATEWAY_TEMPLATE.replace("{data}", data)
}

pub async fn open_gateway(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(query): Query<OpenQuery>,
) -> Result<Response, ServerError> {
    let code = query
        .code
        .as_deref()
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .ok_or_else(|| ServerError::BadRequest("Missing 'code' query parameter".into()))?;

    let settings = state.settings_store.get_settings();
    let public_url = if let Some(ref u) = settings.stream_public_url {
        let trimmed = u.trim();
        if !trimmed.is_empty() {
            trimmed.to_string()
        } else {
            fallback_host_url(&headers, settings.stream_server_port)
        }
    } else {
        fallback_host_url(&headers, settings.stream_server_port)
    };

    let clean_url = public_url.trim_end_matches('/');

    let payload = serde_json::json!({
        "s": clean_url,
        "c": code,
    });
    let payload_bytes = serde_json::to_vec(&payload).map_err(|e| {
        ServerError::Internal(format!("Failed to serialize connection payload: {e}"))
    })?;
    let data = BASE64.encode(payload_bytes);

    let html = render_gateway_page(&data);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(html),
    )
        .into_response())
}

fn fallback_host_url(headers: &HeaderMap, port: u16) -> String {
    if let Some(host_hdr) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) {
        let scheme = if headers
            .get("x-forwarded-proto")
            .and_then(|h| h.to_str().ok())
            == Some("https")
        {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{host_hdr}")
    } else {
        format!("http://127.0.0.1:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_gateway_page_replaces_placeholder() {
        let sample_key = "eyJzIjoiaHR0cHM6Ly9zdHJlYW0uZXhhbXBsZS5jb20iLCJjIjoiMTIzNDU2In0=";
        let rendered = render_gateway_page(sample_key);

        assert!(!rendered.contains("{data}"));
        assert!(rendered.contains(sample_key));
        assert!(rendered.contains("peerless://auth?data="));
        assert!(rendered.contains("intent://auth?data="));
        assert!(rendered.contains("Open Peerless"));
        assert!(rendered.contains("Copy Connection Key"));
    }

    #[test]
    fn test_zero_external_dependencies() {
        assert!(!GATEWAY_TEMPLATE.contains("https://cdn."));
        assert!(!GATEWAY_TEMPLATE.contains("https://fonts.googleapis.com"));
        assert!(!GATEWAY_TEMPLATE.contains("<link rel=\"stylesheet\" href=\"http"));
        assert!(!GATEWAY_TEMPLATE.contains("<script src=\"http"));
    }

    #[test]
    fn test_liquid_glass_design_elements() {
        assert!(GATEWAY_TEMPLATE.contains("backdrop-filter: blur"));
        assert!(GATEWAY_TEMPLATE.contains("Instant Connection"));
        assert!(GATEWAY_TEMPLATE.contains("Peerless Lossless"));
        assert!(GATEWAY_TEMPLATE.contains("Install Peerless"));
    }
}
