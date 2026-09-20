use std::time::Duration;

use bytes::Bytes;
use stream::{ByteRange, CHUNK_SIZE, ChunkCache, CircuitBreaker, hash_bot_token};

#[test]
fn test_hash_bot_token_deterministic() {
    let token = "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ";
    let hash1 = hash_bot_token(token);
    let hash2 = hash_bot_token(token);
    assert_eq!(hash1, hash2);
    assert_eq!(hash1.len(), 64);

    // Whitespace trimming
    let hash_with_spaces = hash_bot_token(&format!("  {token} \n"));
    assert_eq!(hash1, hash_with_spaces);
}

#[test]
fn test_uniform_chunk_size() {
    assert_eq!(CHUNK_SIZE, 512 * 1024);
}

#[tokio::test]
async fn test_chunk_cache_concurrency_and_lru() {
    let cache = ChunkCache::new(2 * 1024 * 1024); // 2 MB capacity

    // Insert two 512KB chunks
    let chunk1 = Bytes::from(vec![1u8; 512 * 1024]);
    let chunk2 = Bytes::from(vec![2u8; 512 * 1024]);
    cache.insert(100, 0, chunk1.clone()).await;
    cache.insert(100, 1, chunk2.clone()).await;

    assert_eq!(cache.get(100, 0).await, Some(chunk1));
    assert_eq!(cache.get(100, 1).await, Some(chunk2));
    assert_eq!(cache.get(100, 2).await, None);
}

#[test]
fn test_circuit_breaker_multi_worker_quarantine() {
    let cb = CircuitBreaker::new(4);
    for i in 0..4 {
        assert!(cb.is_available(i));
    }

    // Quarantine worker 1 for FloodWait
    cb.quarantine(1, Duration::from_millis(50), "FloodWait(1)");
    assert!(cb.is_available(0));
    assert!(!cb.is_available(1));
    assert!(cb.is_available(2));
    assert!(cb.is_available(3));

    // Worker 1 expires after sleep
    std::thread::sleep(Duration::from_millis(60));
    assert!(cb.is_available(1));
}

#[test]
fn test_byte_range_edge_cases() {
    let total = 50_000_000;

    // Full range
    let r = ByteRange::parse("bytes=0-49999999", total).unwrap();
    assert_eq!(r.start, 0);
    assert_eq!(r.end, 49_999_999);
    assert_eq!(r.length(), 50_000_000);

    // Initial prefix
    let r = ByteRange::parse("bytes=0-100", total).unwrap();
    assert_eq!(r.start, 0);
    assert_eq!(r.end, 100);
    assert_eq!(r.length(), 101);

    // Seek open range
    let r = ByteRange::parse("bytes=25000000-", total).unwrap();
    assert_eq!(r.start, 25_000_000);
    assert_eq!(r.end, 49_999_999);
    assert_eq!(r.length(), 25_000_000);

    // Tail suffix range
    let r = ByteRange::parse("bytes=-1000", total).unwrap();
    assert_eq!(r.start, 49_999_000);
    assert_eq!(r.end, 49_999_999);
    assert_eq!(r.length(), 1000);

    // Over-boundary clamping
    let r = ByteRange::parse("bytes=40000000-60000000", total).unwrap();
    assert_eq!(r.start, 40_000_000);
    assert_eq!(r.end, 49_999_999);
}
