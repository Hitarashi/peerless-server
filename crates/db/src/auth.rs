use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use crate::{DbError, DbPool, User, models::NewUser, schema::users};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthedPeer {
    pub telegram_id: i64,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Authorization service backed by the users table.
#[derive(Clone)]
pub struct Auth {
    pool: DbPool,
    admin_id: i64,
}

impl Auth {
    pub fn new(pool: DbPool, admin_id: i64) -> Self {
        Self { pool, admin_id }
    }

    pub fn is_admin(&self, user_id: i64) -> bool {
        user_id == self.admin_id
    }

    pub async fn is_authorized(&self, user_id: i64, chat_id: Option<i64>) -> Result<bool, DbError> {
        if self.is_admin(user_id) {
            return Ok(true);
        }
        let ids = chat_id
            .filter(|id| *id != user_id)
            .map_or_else(|| vec![user_id], |chat_id| vec![user_id, chat_id]);
        let mut connection = self.pool.connection().await?;
        let found = users::table
            .filter(users::telegram_id.eq_any(ids))
            .select(users::telegram_id)
            .first::<i64>(&mut *connection)
            .await
            .optional()?;
        Ok(found.is_some())
    }

    pub async fn authorize(&self, telegram_id: i64, name: Option<&str>) -> Result<bool, DbError> {
        let mut connection = self.pool.connection().await?;
        let existing = users::table
            .filter(users::telegram_id.eq(telegram_id))
            .select(users::telegram_id)
            .first::<i64>(&mut *connection)
            .await
            .optional()?;
        diesel::insert_into(users::table)
            .values(NewUser { telegram_id, name })
            .on_conflict(users::telegram_id)
            .do_update()
            .set(users::name.eq(name))
            .execute(&mut *connection)
            .await?;
        Ok(existing.is_none())
    }

    pub async fn revoke(&self, telegram_id: i64) -> Result<bool, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(
            diesel::delete(users::table.filter(users::telegram_id.eq(telegram_id)))
                .execute(&mut *connection)
                .await?
                > 0,
        )
    }

    pub async fn list_authorized(&self) -> Result<Vec<AuthedPeer>, DbError> {
        let mut connection = self.pool.connection().await?;
        let rows = users::table
            .select(User::as_select())
            .order(users::created_at.asc())
            .load::<User>(&mut *connection)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| AuthedPeer {
                telegram_id: row.telegram_id,
                name: row.name,
                created_at: row.created_at,
            })
            .collect())
    }
}
