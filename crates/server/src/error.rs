use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Not Found: {0}")]
    NotFound(String),
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Internal Server Error: {0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ServerError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", msg),
            ServerError::Forbidden(msg) => (StatusCode::FORBIDDEN, "FORBIDDEN", msg),
            ServerError::NotFound(msg) => (StatusCode::NOT_FOUND, "NOT_FOUND", msg),
            ServerError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "BAD_REQUEST", msg),
            ServerError::Internal(msg) => {
                tracing::error!(error = %msg, "Internal server error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_SERVER_ERROR",
                    "An internal server error occurred".to_string(),
                )
            }
        };

        (
            status,
            Json(ErrorBody {
                error: code,
                message,
            }),
        )
            .into_response()
    }
}

impl From<db::DbError> for ServerError {
    fn from(err: db::DbError) -> Self {
        match err {
            db::DbError::NotFound(msg) => ServerError::NotFound(msg),
            db::DbError::Unauthorized(msg) => ServerError::Unauthorized(msg),
            db::DbError::Validation(msg) => ServerError::BadRequest(msg),
            db::DbError::Database(e) => ServerError::Internal(e.to_string()),
            db::DbError::Pool(e) => ServerError::Internal(e),
            db::DbError::Migration(e) => ServerError::Internal(e),
            db::DbError::Row(e) => ServerError::Internal(e),
        }
    }
}

impl From<stream::StreamError> for ServerError {
    fn from(err: stream::StreamError) -> Self {
        match err {
            stream::StreamError::TrackNotFound(id) => {
                ServerError::NotFound(format!("Track {id} not found"))
            }
            stream::StreamError::NoMediaDocument(id) => {
                ServerError::NotFound(format!("No media document found for track {id}"))
            }
            stream::StreamError::InvalidRange(msg) => ServerError::BadRequest(msg),
            stream::StreamError::FileReferenceExpired => {
                ServerError::Internal("Telegram file reference expired".into())
            }
            stream::StreamError::FloodWait(secs) => {
                ServerError::Internal(format!("Telegram flood wait: {secs}s"))
            }
            stream::StreamError::AllWorkersUnavailable => {
                ServerError::Internal("All streaming workers are currently unavailable".into())
            }
            stream::StreamError::UnsupportedCdnRedirect => {
                ServerError::Internal("Unsupported CDN redirect".into())
            }
            stream::StreamError::Connect(e) => ServerError::Internal(e.to_string()),
            stream::StreamError::Telegram(e) => ServerError::Internal(e.to_string()),
            stream::StreamError::Db(e) => ServerError::from(e),
        }
    }
}

impl From<anyhow::Error> for ServerError {
    fn from(err: anyhow::Error) -> Self {
        ServerError::Internal(err.to_string())
    }
}
