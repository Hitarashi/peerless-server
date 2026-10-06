use apple::wrapper::{CodecPreference, WrapperEngine};
use engine::streaming::{ProgressCallback, StreamError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let track_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1561413895".to_owned());
    let out_path = std::env::args().nth(2).unwrap_or_else(|| {
        std::path::Path::new("/tmp/opencode/cenc_probe.m4a")
            .to_string_lossy()
            .to_string()
    });

    let preference = match std::env::args().nth(3).as_deref() {
        Some("atmos") => CodecPreference::Atmos,
        _ => CodecPreference::HighestQuality,
    };

    let wrapper_url =
        std::env::var("ALAC_WRAPPER_URL").unwrap_or_else(|_| "http://localhost:12340".into());
    let engine = WrapperEngine::new(&wrapper_url, None);

    println!("ripping {track_id} via {wrapper_url} (preference: {preference:?})");
    let progress: ProgressCallback = std::sync::Arc::new(|activity| {
        println!("  progress: {activity:?}");
    });
    let source = engine
        .rip_track(&track_id, None, Some(progress), preference)
        .await
        .map_err(|e: StreamError| e)?;

    let mut collected = Vec::new();
    use futures_util::StreamExt;
    let mut stream = source.stream;
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk?);
    }

    println!(
        "codec={} rate={} depth={} bytes={} source={}",
        source.codec,
        source.sample_rate,
        source.bit_depth,
        collected.len(),
        source.source
    );
    if collected.len() < 8 {
        eprintln!("ERROR: empty stream");
        std::process::exit(1);
    }
    println!("head: {:?}", &collected[..16.min(collected.len())]);
    let has_ftyp = collected.starts_with(&[0, 0, 0, b'f']) || {
        let fourcc: Vec<u8> = collected[4..8].to_vec();
        fourcc == b"ftyp"
    };
    println!("ftyp box: {has_ftyp}");

    std::fs::create_dir_all(std::path::Path::new(&out_path).parent().unwrap())?;
    std::fs::write(&out_path, &collected)?;
    println!("wrote {out_path}");
    Ok(())
}
