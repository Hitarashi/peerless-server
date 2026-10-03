//! One-off live smoke check for the production transport + catalog mapping
//! against a configured catalog API. NOT part of the test suite (network).
//!
//! Run: `CATALOG_API_URL=https://host/api/v1 cargo run -p apple --example live_check`

use apple::catalog::{Catalog, ReqwestTransport};
use engine::settings::LyricspornApiEndpoint;

#[tokio::main]
async fn main() {
    let api_url =
        std::env::var("CATALOG_API_URL").expect("set CATALOG_API_URL to the catalog API base URL");
    let endpoint = LyricspornApiEndpoint::new(Some(&api_url));
    let catalog = Catalog::with_endpoint(ReqwestTransport::new(), endpoint);

    let meta = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("live track lookup");
    println!(
        "track: {} — {} [{}] {}s",
        meta.artist, meta.title, meta.album, meta.duration_secs
    );
    println!("  artwork: {}", meta.artwork_url);
    println!("  release: {}", meta.release_date);

    let search = catalog
        .search_catalog("the weeknd blinded lights", 3, "us")
        .await
        .expect("live search");
    println!("search: {} results", search.len());
    for t in search.iter().take(3) {
        println!("  {} — {} (id {})", t.artist, t.title, t.id);
    }
}
