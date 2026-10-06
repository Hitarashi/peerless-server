#[tokio::test]
async fn pool_gets_a_tls_connection() {
    let Some(url) = std::env::var("DATABASE_URL").ok().filter(|v| !v.is_empty()) else {
        eprintln!("skipping: DATABASE_URL is not set");
        return;
    };

    let pool = db::connect(&url)
        .await
        .unwrap_or_else(|error| panic!("connect() failed: {error}"));

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
