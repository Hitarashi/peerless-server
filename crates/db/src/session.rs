//! Deep session management module.
//!
//! Encapsulates single-use OTP login code generation, 256-bit CSPRNG refresh token
//! creation, SHA-256 token hashing, sliding 3-day TTL renewal, and user status verification.

use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use rand::Rng;
use sha2::{Digest, Sha256};

use crate::{
    DbError, DbPool,
    models::{NewOneTimeAuthCode, NewUser, NewUserSession, OneTimeAuthCode, UserSession},
    schema::{one_time_auth_codes, user_sessions, users},
};

#[derive(Debug, Clone)]
pub struct SessionTokens {
    pub session_id: String,
    pub telegram_id: i64,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SessionIdentity {
    pub session_id: String,
    pub telegram_id: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ClientMetadata<'a> {
    pub device_name: Option<&'a str>,
    pub platform: Option<&'a str>,
}

#[derive(Clone)]
pub struct SessionManager {
    pool: DbPool,
    admin_id: i64,
}

/// Compute lowercase hex-encoded SHA-256 hash of a token after trimming leading and trailing whitespace.
pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.trim().as_bytes());
    let hash = hasher.finalize();
    let mut s = String::with_capacity(64);
    for byte in hash {
        use std::fmt::Write;
        let _ = write!(s, "{byte:02x}");
    }
    s
}

impl SessionManager {
    pub fn new(pool: DbPool, admin_id: i64) -> Self {
        Self { pool, admin_id }
    }

    pub fn hash_token(token: &str) -> String {
        hash_token(token)
    }

    /// Generates a single-use 6-character OTP (e.g. "ABC-XYZ") valid for 5 minutes.
    /// Fails if the user is not present in the authorized `users` table.
    pub async fn create_login_code(&self, telegram_id: i64) -> Result<String, DbError> {
        let mut conn = self.pool.connection().await?;

        if telegram_id == self.admin_id {
            diesel::insert_into(users::table)
                .values(NewUser {
                    telegram_id: self.admin_id,
                    name: Some("Admin"),
                })
                .on_conflict_do_nothing()
                .execute(&mut *conn)
                .await?;
        } else {
            let user_exists = users::table
                .filter(users::telegram_id.eq(telegram_id))
                .select(users::telegram_id)
                .first::<i64>(&mut *conn)
                .await
                .optional()?;

            if user_exists.is_none() {
                return Err(DbError::Unauthorized(format!(
                    "User {telegram_id} is not authorized"
                )));
            }
        }

        // Clean up expired codes or previous codes for this user
        diesel::delete(
            one_time_auth_codes::table.filter(
                one_time_auth_codes::telegram_id
                    .eq(telegram_id)
                    .or(one_time_auth_codes::expires_at.lt(Utc::now())),
            ),
        )
        .execute(&mut *conn)
        .await?;

        // 6 random characters from unambiguous alphabet (32 chars)
        let mut bytes = [0u8; 6];
        rand::rng().fill_bytes(&mut bytes);
        let alphabet = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        let code: String = bytes
            .iter()
            .map(|b| alphabet[(b % 32) as usize] as char)
            .collect();
        let formatted_code = format!("{}-{}", &code[..3], &code[3..]);

        let expires_at = Utc::now() + Duration::minutes(5);

        diesel::insert_into(one_time_auth_codes::table)
            .values(NewOneTimeAuthCode {
                code: &formatted_code,
                telegram_id,
                expires_at,
            })
            .execute(&mut *conn)
            .await?;

        Ok(formatted_code)
    }

    /// Exchanges a valid OTP for a session. Consumes the OTP, verifies user
    /// authorization, generates a 256-bit CSPRNG refresh token, stores its
    /// SHA-256 hash with a 3-day expiration, and returns the raw token.
    pub async fn exchange_code(
        &self,
        code: &str,
        metadata: ClientMetadata<'_>,
    ) -> Result<SessionTokens, DbError> {
        let mut conn = self.pool.connection().await?;
        let clean_code = code.trim().to_uppercase();

        let otp = one_time_auth_codes::table
            .filter(one_time_auth_codes::code.eq(&clean_code))
            .filter(one_time_auth_codes::expires_at.gt(Utc::now()))
            .first::<OneTimeAuthCode>(&mut *conn)
            .await
            .optional()?
            .ok_or_else(|| DbError::Validation("Invalid or expired login code".into()))?;

        // Assert user was not revoked after code was generated
        let user_authorized = if otp.telegram_id == self.admin_id {
            true
        } else {
            users::table
                .filter(users::telegram_id.eq(otp.telegram_id))
                .select(users::telegram_id)
                .first::<i64>(&mut *conn)
                .await
                .optional()?
                .is_some()
        };

        if !user_authorized {
            let _ = diesel::delete(
                one_time_auth_codes::table.filter(one_time_auth_codes::code.eq(&clean_code)),
            )
            .execute(&mut *conn)
            .await;
            return Err(DbError::Unauthorized(format!(
                "User {} is not authorized",
                otp.telegram_id
            )));
        }

        // Single-use: delete immediately
        diesel::delete(
            one_time_auth_codes::table.filter(one_time_auth_codes::code.eq(&clean_code)),
        )
        .execute(&mut *conn)
        .await?;

        // 32 bytes (256 bits) cryptographically secure random token
        let mut token_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut token_bytes);
        let raw_refresh_token: String = token_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let refresh_token_hash = Self::hash_token(&raw_refresh_token);

        let session_id = cuid2::create_id();
        let expires_at = Utc::now() + Duration::days(3);

        diesel::insert_into(user_sessions::table)
            .values(NewUserSession {
                id: &session_id,
                telegram_id: otp.telegram_id,
                refresh_token_hash: &refresh_token_hash,
                device_name: metadata.device_name,
                platform: metadata.platform,
                expires_at,
            })
            .execute(&mut *conn)
            .await?;

        Ok(SessionTokens {
            session_id,
            telegram_id: otp.telegram_id,
            refresh_token: raw_refresh_token,
            expires_at,
        })
    }

    /// Verifies the refresh token hash against `user_sessions`. Checks that the session
    /// has not expired or been revoked, and confirms the user still exists in `users`.
    /// Slides `expires_at` forward by 3 days and updates `last_active_at`.
    pub async fn verify_and_slide(
        &self,
        raw_refresh_token: &str,
    ) -> Result<SessionIdentity, DbError> {
        let mut conn = self.pool.connection().await?;
        let session = self
            .verify_session_record(raw_refresh_token, &mut conn, true)
            .await?;

        let new_expires_at = Utc::now() + Duration::days(3);
        diesel::update(user_sessions::table.filter(user_sessions::id.eq(&session.id)))
            .set((
                user_sessions::last_active_at.eq(Utc::now()),
                user_sessions::expires_at.eq(new_expires_at),
            ))
            .execute(&mut *conn)
            .await?;

        Ok(SessionIdentity {
            session_id: session.id,
            telegram_id: session.telegram_id,
            expires_at: new_expires_at,
        })
    }

    /// Verifies the refresh token hash, expiry, revocation status, and current user
    /// authorization without changing the session's expiry or activity timestamp.
    pub async fn verify_session(
        &self,
        raw_refresh_token: &str,
    ) -> Result<SessionIdentity, DbError> {
        let mut conn = self.pool.connection().await?;
        let session = self
            .verify_session_record(raw_refresh_token, &mut conn, false)
            .await?;

        Ok(SessionIdentity {
            session_id: session.id,
            telegram_id: session.telegram_id,
            expires_at: session.expires_at,
        })
    }

    async fn verify_session_record(
        &self,
        raw_refresh_token: &str,
        conn: &mut AsyncPgConnection,
        revoke_unauthorized_user: bool,
    ) -> Result<UserSession, DbError> {
        let hash = Self::hash_token(raw_refresh_token.trim());

        let session = user_sessions::table
            .filter(user_sessions::refresh_token_hash.eq(&hash))
            .filter(user_sessions::revoked.eq(false))
            .first::<UserSession>(&mut *conn)
            .await
            .optional()?
            .ok_or_else(|| DbError::Unauthorized("Invalid or revoked session".into()))?;

        if session.expires_at <= Utc::now() {
            return Err(DbError::Unauthorized("Session has expired".into()));
        }

        // Check if user was revoked by admin in the bot
        if session.telegram_id != self.admin_id {
            let user_exists = users::table
                .filter(users::telegram_id.eq(session.telegram_id))
                .select(users::telegram_id)
                .first::<i64>(&mut *conn)
                .await
                .optional()?;

            if user_exists.is_none() {
                if revoke_unauthorized_user {
                    diesel::update(user_sessions::table.filter(user_sessions::id.eq(&session.id)))
                        .set(user_sessions::revoked.eq(true))
                        .execute(&mut *conn)
                        .await?;
                }
                return Err(DbError::Unauthorized(format!(
                    "User {} access has been revoked",
                    session.telegram_id
                )));
            }
        }

        Ok(session)
    }

    /// Marks a session as revoked.
    pub async fn revoke(&self, raw_refresh_token: &str) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        let hash = Self::hash_token(raw_refresh_token.trim());

        let affected = diesel::update(
            user_sessions::table.filter(user_sessions::refresh_token_hash.eq(&hash)),
        )
        .set(user_sessions::revoked.eq(true))
        .execute(&mut *conn)
        .await?;

        Ok(affected > 0)
    }

    /// Revokes all active sessions for a user (e.g. upon user revocation).
    pub async fn revoke_all_for_user(&self, telegram_id: i64) -> Result<usize, DbError> {
        let mut conn = self.pool.connection().await?;
        let affected =
            diesel::update(user_sessions::table.filter(user_sessions::telegram_id.eq(telegram_id)))
                .set(user_sessions::revoked.eq(true))
                .execute(&mut *conn)
                .await?;

        Ok(affected)
    }

    /// Lists active (unexpired, unrevoked) sessions for a user.
    pub async fn list_active_sessions(
        &self,
        telegram_id: i64,
    ) -> Result<Vec<UserSession>, DbError> {
        let mut conn = self.pool.connection().await?;
        let rows = user_sessions::table
            .filter(user_sessions::telegram_id.eq(telegram_id))
            .filter(user_sessions::revoked.eq(false))
            .filter(user_sessions::expires_at.gt(Utc::now()))
            .order(user_sessions::last_active_at.desc())
            .load::<UserSession>(&mut *conn)
            .await?;

        Ok(rows)
    }

    pub async fn purge_expired_sessions(&self) -> Result<usize, DbError> {
        let mut conn = self.pool.connection().await?;
        let affected = diesel::delete(
            user_sessions::table.filter(
                user_sessions::expires_at
                    .lt(Utc::now())
                    .or(user_sessions::revoked.eq(true)),
            ),
        )
        .execute(&mut *conn)
        .await?;
        Ok(affected)
    }

    pub async fn purge_expired_codes(&self) -> Result<usize, DbError> {
        let mut conn = self.pool.connection().await?;
        let affected = diesel::delete(
            one_time_auth_codes::table.filter(one_time_auth_codes::expires_at.lt(Utc::now())),
        )
        .execute(&mut *conn)
        .await?;
        Ok(affected)
    }

    pub async fn get_user_name(&self, telegram_id: i64) -> Result<Option<String>, DbError> {
        let mut conn = self.pool.connection().await?;
        let name = users::table
            .filter(users::telegram_id.eq(telegram_id))
            .select(users::name)
            .first::<Option<String>>(&mut *conn)
            .await
            .optional()?
            .flatten();
        Ok(name)
    }

    /// Update an existing user's display name only when it has changed.
    /// This deliberately never inserts a row, so syncing profile metadata
    /// cannot grant authorization to a user that was removed.
    pub async fn update_user_name_if_changed(
        &self,
        telegram_id: i64,
        name: &str,
    ) -> Result<bool, DbError> {
        let name = name.trim();
        if name.is_empty() {
            return Ok(false);
        }

        let mut conn = self.pool.connection().await?;
        let current_name = users::table
            .filter(users::telegram_id.eq(telegram_id))
            .select(users::name)
            .first::<Option<String>>(&mut *conn)
            .await
            .optional()?
            .flatten();

        if current_name.as_deref() == Some(name) {
            return Ok(false);
        }

        let updated = diesel::update(users::table.filter(users::telegram_id.eq(telegram_id)))
            .set(users::name.eq(name))
            .execute(&mut *conn)
            .await?;
        Ok(updated > 0)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;

    async fn create_test_session() -> Option<(SessionManager, SessionTokens)> {
        let pool = match crate::connect_test_isolated().await {
            Ok(pool) => pool,
            Err(_) => {
                eprintln!("Skipping session verification test: TEST_DATABASE_URL not set");
                return None;
            }
        };
        crate::migrate(&pool).await.expect("database migrations");

        let admin_id = 999_000_101;
        let session_manager = SessionManager::new(pool, admin_id);
        let code = session_manager
            .create_login_code(admin_id)
            .await
            .expect("create admin login code");
        let tokens = session_manager
            .exchange_code(&code, ClientMetadata::default())
            .await
            .expect("exchange admin login code");

        Some((session_manager, tokens))
    }

    async fn stored_expiry(session_manager: &SessionManager, session_id: &str) -> DateTime<Utc> {
        let mut conn = session_manager
            .pool
            .connection()
            .await
            .expect("database connection");
        user_sessions::table
            .filter(user_sessions::id.eq(session_id))
            .select(user_sessions::expires_at)
            .first(&mut *conn)
            .await
            .expect("stored session expiry")
    }

    #[tokio::test]
    async fn verify_session_succeeds_without_extending_expiry() {
        let Some((session_manager, tokens)) = create_test_session().await else {
            return;
        };
        let stored_expiry_before = stored_expiry(&session_manager, &tokens.session_id).await;

        let identity = session_manager
            .verify_session(&tokens.refresh_token)
            .await
            .expect("verify valid session");

        let stored_expiry_after = stored_expiry(&session_manager, &tokens.session_id).await;
        assert_eq!(identity.session_id, tokens.session_id);
        assert_eq!(identity.telegram_id, tokens.telegram_id);
        assert_eq!(identity.expires_at, stored_expiry_before);
        assert_eq!(stored_expiry_after, stored_expiry_before);
    }

    #[tokio::test]
    async fn verify_session_rejects_expired_session() {
        let Some((session_manager, tokens)) = create_test_session().await else {
            return;
        };
        let mut conn = session_manager
            .pool
            .connection()
            .await
            .expect("database connection");
        diesel::update(user_sessions::table.filter(user_sessions::id.eq(&tokens.session_id)))
            .set(user_sessions::expires_at.eq(Utc::now() - Duration::seconds(1)))
            .execute(&mut *conn)
            .await
            .expect("expire session");
        drop(conn);

        let result = session_manager.verify_session(&tokens.refresh_token).await;
        assert!(result.is_err(), "expired session must be rejected");
    }

    #[tokio::test]
    async fn verify_session_rejects_revoked_session() {
        let Some((session_manager, tokens)) = create_test_session().await else {
            return;
        };
        assert!(
            session_manager
                .revoke(&tokens.refresh_token)
                .await
                .expect("revoke session")
        );

        let result = session_manager.verify_session(&tokens.refresh_token).await;
        assert!(result.is_err(), "revoked session must be rejected");
    }
}
