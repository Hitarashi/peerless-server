//! Live checks for the pooled TLS path.
//!
//! Skipped unless `DATABASE_URL` is set:
//!
//! ```sh
//! DATABASE_URL='postgres://user:pw@host/db?sslmode=require' \
//!   cargo test -p db --test pool_tls -- --nocapture
//! ```

#[tokio::test]
async fn pool_gets_a_tls_connection() {
    let Some(url) = std::env::var("DATABASE_URL").ok().filter(|v| !v.is_empty()) else {
        eprintln!("skipping: DATABASE_URL is not set");
        return;
    };

    let pool = db::connect(&url)
        .await
        .unwrap_or_else(|error| panic!("connect() failed: {error}"));

    // A second get proves the manager can build more than one connection.
    let _first = pool
        .connection()
        .await
        .expect("pool should hand out a TLS connection");
    let _second = pool
        .connection()
        .await
        .expect("pool should build additional TLS connections");

    eprintln!("pool established TLS connections to {url}");
}
