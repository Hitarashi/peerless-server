//! Live check that `db::establish` can actually reach a TLS-requiring server.
//!
//! Establishing the connection *is* the thing under test: the failure this
//! guards against is a pool that times out during connect, not a bad query.
//!
//! Skipped unless `DATABASE_URL` is set, so it never runs in CI by accident:
//!
//! ```sh
//! DATABASE_URL='postgres://user:pw@host/db?sslmode=require' \
//!   cargo test -p db --test tls_connect -- --nocapture
//! ```
#[tokio::test]
async fn establishes_a_connection_when_sslmode_requires_tls() {
    let Some(url) = std::env::var("DATABASE_URL").ok().filter(|v| !v.is_empty()) else {
        eprintln!("skipping: DATABASE_URL is not set");
        return;
    };

    db::establish(&url)
        .await
        .unwrap_or_else(|error| panic!("establish() failed: {error}"));

    eprintln!("established a connection to {url}");
}
