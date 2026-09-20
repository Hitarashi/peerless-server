use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use sha2::{Digest, Sha256};

use crate::{
    DbError, DbPool,
    models::{NewTgWorkerSession, TgWorkerSession},
    schema::tg_worker_sessions,
};

/// High-leverage repository for persisting Telegram worker bot session strings.
#[derive(Clone)]
pub struct WorkerSessionStore {
    pool: DbPool,
    cipher: Option<Aes256Gcm>,
}

impl WorkerSessionStore {
    pub fn new(pool: DbPool, secret_key: Option<&str>) -> Self {
        let cipher = secret_key.and_then(|key_str| {
            let key = Sha256::digest(key_str.as_bytes());
            Aes256Gcm::new_from_slice(&key).ok()
        });
        Self { pool, cipher }
    }

    pub fn from_env(pool: DbPool) -> Self {
        let secret_key = std::env::var("APP_KEY")
            .or_else(|_| std::env::var("SESSION_ENCRYPTION_KEY"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self::new(pool, secret_key.as_deref())
    }

    /// Retrieve a saved session string by the bot token's SHA-256 hash.
    pub async fn get_session(&self, token_hash: &str) -> Result<Option<String>, DbError> {
        let mut conn = self.pool.connection().await?;
        let session = tg_worker_sessions::table
            .filter(tg_worker_sessions::bot_token_hash.eq(token_hash))
            .select(TgWorkerSession::as_select())
            .first(&mut *conn)
            .await
            .optional()?;

        let Some(s) = session else {
            return Ok(None);
        };

        let raw_data = s.session_data;
        if let Some(ref c) = self.cipher {
            if let Ok(decoded) = BASE64.decode(&raw_data)
                && decoded.len() >= 12
            {
                let (nonce_bytes, ciphertext) = decoded.split_at(12);
                if let Ok(nonce) = Nonce::try_from(nonce_bytes)
                    && let Ok(plaintext) = c.decrypt(&nonce, ciphertext)
                    && let Ok(session_str) = String::from_utf8(plaintext)
                {
                    return Ok(Some(session_str));
                }
            }
            tracing::warn!(token_hash, "Failed to decode or decrypt worker session");
            Ok(None)
        } else {
            Ok(Some(raw_data))
        }
    }

    /// Persist or update an authenticated session string for a worker bot token.
    pub async fn save_session(&self, token_hash: &str, session_data: &str) -> Result<(), DbError> {
        let mut conn = self.pool.connection().await?;
        let to_save = if let Some(ref c) = self.cipher {
            let nonce_bytes: [u8; 12] = rand::random();
            let nonce = Nonce::from(nonce_bytes);
            let ciphertext = c
                .encrypt(&nonce, session_data.as_bytes())
                .map_err(|e| DbError::Row(format!("encryption failed: {e}")))?;
            let mut combined = Vec::with_capacity(12 + ciphertext.len());
            combined.extend_from_slice(&nonce_bytes);
            combined.extend_from_slice(&ciphertext);
            BASE64.encode(&combined)
        } else {
            session_data.to_string()
        };

        let new_session = NewTgWorkerSession {
            bot_token_hash: token_hash,
            session_data: &to_save,
            updated_at: Utc::now(),
        };

        diesel::insert_into(tg_worker_sessions::table)
            .values(&new_session)
            .on_conflict(tg_worker_sessions::bot_token_hash)
            .do_update()
            .set(&new_session)
            .execute(&mut *conn)
            .await?;

        Ok(())
    }

    /// Remove a session record by token hash (e.g. on token revocation or invalid session).
    pub async fn delete_session(&self, token_hash: &str) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        let deleted = diesel::delete(
            tg_worker_sessions::table.filter(tg_worker_sessions::bot_token_hash.eq(token_hash)),
        )
        .execute(&mut *conn)
        .await?;

        Ok(deleted > 0)
    }
}
