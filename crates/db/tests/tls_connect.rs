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
