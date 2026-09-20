use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use ferogram::PeerRef;
use server::{
    ServerState,
    streaming::{StreamTicket, create_stream_ticket, verify_stream_ticket},
};
use tower::ServiceExt;

#[tokio::test]
async fn test_playback_ticket_cryptography() {
    let key = "super_secret_master_key_for_testing";
    let track_id = 42;
    let user_id = 123456789;
    let ttl = 7200;

    let ticket = create_stream_ticket(key, track_id, user_id, ttl);
    assert!(!ticket.is_empty());

    let (verified_track, verified_user) =
        verify_stream_ticket(key, &ticket).expect("valid ticket must verify");
    assert_eq!(verified_track, track_id);
    assert_eq!(verified_user, user_id);

    // Direct StreamTicket methods
    let pt = StreamTicket::new(track_id, user_id, ttl);
    let encoded = pt.encode(key);
    let decoded = StreamTicket::decode(key, &encoded).expect("valid ticket must decode");
    assert_eq!(decoded.track_id, track_id);
    assert_eq!(decoded.user_id, user_id);

    // Tampered key fails
    assert!(verify_stream_ticket("wrong_key", &ticket).is_err());
    assert!(StreamTicket::decode("wrong_key", &encoded).is_err());

    // Expired ticket fails
    let expired_ticket = create_stream_ticket(key, track_id, user_id, -10);
    assert!(verify_stream_ticket(key, &expired_ticket).is_err());
    let expired_pt = StreamTicket::new(track_id, user_id, -10);
    assert!(StreamTicket::decode(key, &expired_pt.encode(key)).is_err());
}

#[tokio::test]
async fn test_docs_and_unauthorized_endpoints() {
    let _ = dotenvy::from_filename(".env");
    let Ok(db_url) = std::env::var("DATABASE_URL").or_else(|_| std::env::var("TEST_DATABASE_URL"))
    else {
        eprintln!("Skipping HTTP router integration test: DATABASE_URL not set");
        return;
    };

    let pool = match db::connect(&db_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping HTTP router test: cannot connect to {db_url}: {e}");
            return;
        }
    };
    db::migrate(&pool).await.expect("database migrations");

    let worker_pool = stream::StreamWorkerPool::empty();
    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(pool.clone()),
        None,
        PeerRef::from(0),
    ));

    let session_mgr = Arc::new(db::SessionManager::new(pool.clone(), 12345));
    let library_mgr = Arc::new(db::LibraryManager::new(pool.clone()));
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_testing_routes_32_bytes";
    let state = Arc::new(ServerState::new(
        stream_engine,
        session_mgr,
        library_mgr,
        tracks_repo,
        settings_store,
        orchestrator,
        app_key.to_string(),
    ));

    let app = server::create_router(state);

    // 1. Test Scalar UI
    let req = Request::builder()
        .uri("/api/v1/docs")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("@scalar/api-reference"));
    assert!(html.contains("/api/v1/docs.json"));

    // 2. Test OpenAPI JSON
    let req = Request::builder()
        .uri("/api/v1/docs.json")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = String::from_utf8(body.to_vec()).unwrap();
    assert!(json.contains("\"openapi\":\"3.1"));

    // 3. Test OpenAPI YAML
    let req = Request::builder()
        .uri("/api/v1/docs.yaml")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. Test Unauthorized access to /api/v1/auth/me
    let req = Request::builder()
        .uri("/api/v1/auth/me")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 5. Test Unauthorized access to /api/v1/tracks/1/playback (GET and POST)
    let req = Request::builder()
        .uri("/api/v1/tracks/1/playback")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tracks/1/playback")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 6. Test Bad Request to /api/v1/stream (missing ticket and track_id)
    let req = Request::builder()
        .uri("/api/v1/stream")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // 7. Test /api/v1/stream with invalid ticket
    let req = Request::builder()
        .uri("/api/v1/stream?ticket=bogus_ticket_signature")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 8. Test /open endpoint
    let req = Request::builder()
        .uri("/open?code=test_code_123")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("peerless://auth?data="));
    assert!(html.contains("Open Peerless"));
    assert!(html.contains("Copy Connection Key"));

    // 9. Test /open without code (400 Bad Request)
    let req = Request::builder().uri("/open").body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // 10. Test /api/v1/health endpoint
    let req = Request::builder()
        .uri("/api/v1/health")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let health_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(health_res["status"], "healthy");
    assert!(health_res["workers_total"].as_u64().is_some());
    assert!(health_res["workers_available"].as_u64().is_some());
    assert!(health_res["cache_entries"].as_u64().is_some());
    assert!(health_res["cache_bytes"].as_u64().is_some());
    assert!(health_res["uptime_seconds"].as_u64().is_some());
}

#[tokio::test]
async fn test_auth_and_library_lifecycle() {
    let _ = dotenvy::from_filename(".env");
    let Ok(db_url) = std::env::var("DATABASE_URL").or_else(|_| std::env::var("TEST_DATABASE_URL"))
    else {
        eprintln!("Skipping database lifecycle test: DATABASE_URL not set");
        return;
    };

    let pool = match db::connect(&db_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to {db_url}: {e}");
            return;
        }
    };
    db::migrate(&pool).await.expect("database migrations");

    let admin_id = 888_000_123;
    let _ = db::integrations::delete_integration(&pool, admin_id, "lastfm").await;
    let worker_pool = stream::StreamWorkerPool::empty();
    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(pool.clone()),
        None,
        PeerRef::from(0),
    ));

    let session_mgr = Arc::new(db::SessionManager::new(pool.clone(), admin_id));
    let library_mgr = Arc::new(db::LibraryManager::new(pool.clone()));
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_testing_lifecycle_32";
    let state = Arc::new(ServerState::new(
        stream_engine,
        session_mgr.clone(),
        library_mgr,
        tracks_repo,
        settings_store,
        orchestrator,
        app_key.to_string(),
    ));

    let app = server::create_router(state);

    // 1. Generate OTP login code for admin
    let otp_code = session_mgr.create_login_code(admin_id).await.unwrap();

    // 2. Exchange OTP for AdonisJS-style opaque token via HTTP POST /api/v1/auth/exchange
    let exchange_payload = serde_json::json!({
        "code": otp_code,
        "device_name": "Test Runner",
        "platform": "linux"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/auth/exchange")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&exchange_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let exchange_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(exchange_res["token_type"], "Bearer");
    assert_eq!(exchange_res["expires_in"], 259200);
    assert!(exchange_res["expires_at_unix"].as_i64().is_some());
    let token = exchange_res["token"].as_str().unwrap().to_string();
    assert!(!token.is_empty());
    assert_eq!(exchange_res["access_token"], token);
    assert_eq!(exchange_res["refresh_token"], token);
    assert_eq!(exchange_res["user"]["telegram_id"], admin_id);

    // 2b. Refresh token via /api/v1/auth/refresh
    let refresh_payload = serde_json::json!({
        "refresh_token": token
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/auth/refresh")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&refresh_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let refresh_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(refresh_res["token_type"], "Bearer");
    assert_eq!(refresh_res["access_token"], token);
    assert_eq!(refresh_res["refresh_token"], token);
    assert_eq!(refresh_res["expires_in"], 259200);
    assert!(refresh_res["expires_at_unix"].as_i64().is_some());

    // 2c. Test authorized POST /api/v1/tracks/1/playback - forbidden without Last.fm connection
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tracks/1/playback")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Save Last.fm integration for admin
    let cipher = db::crypto::CryptoCipher::new(app_key).unwrap();
    let enc_key = cipher.encrypt("lastfm_test_session_key").unwrap();
    db::integrations::save_integration(&pool, admin_id, "lastfm", "testuser", &enc_key)
        .await
        .unwrap();

    // With Last.fm connected, playback succeeds
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tracks/1/playback")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let pb_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        pb_res["stream_url"]
            .as_str()
            .unwrap()
            .contains("/api/v1/stream?ticket=")
    );
    assert_eq!(pb_res["expires_in"], 7200);
    assert!(pb_res["file_size"].as_i64().is_some());

    // Test GET /api/v1/integrations/lastfm/status
    let req = Request::builder()
        .uri("/api/v1/integrations/lastfm/status")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let status_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status_res["connected"], true);
    assert_eq!(status_res["username"], "testuser");
    assert_eq!(status_res["session_key"], "lastfm_test_session_key");

    // Test DELETE /api/v1/integrations/lastfm
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/integrations/lastfm")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // After disconnect, status is not connected
    let req = Request::builder()
        .uri("/api/v1/integrations/lastfm/status")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let status_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status_res["connected"], false);
    assert!(status_res["session_key"].is_null());

    // Playback is forbidden again after disconnect
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tracks/1/playback")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Restore integration for any downstream test steps
    db::integrations::save_integration(&pool, admin_id, "lastfm", "testuser", &enc_key)
        .await
        .unwrap();

    // Nonexistent track returns 404
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tracks/99999999/playback")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 2d. Test authorized GET /api/v1/albums/test_album
    let req = Request::builder()
        .uri("/api/v1/albums/test_album")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 3. Query /api/v1/auth/me with Bearer token
    let req = Request::builder()
        .uri("/api/v1/auth/me")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let me_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(me_res["user"]["telegram_id"], admin_id);
    assert!(!me_res["sessions"].as_array().unwrap().is_empty());

    // 4. Create a playlist via /api/v1/me/playlists
    let playlist_payload = serde_json::json!({ "name": "Phase 4 Lossless Hits" });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/me/playlists")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&playlist_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let playlist_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let playlist_id = playlist_res["id"].as_i64().unwrap();
    assert_eq!(playlist_res["name"], "Phase 4 Lossless Hits");

    // 5. List playlists
    let req = Request::builder()
        .uri("/api/v1/me/playlists")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 5b. Test hydrated favorites endpoint /api/v1/me/favorites
    let req = Request::builder()
        .uri("/api/v1/me/favorites")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let favs_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(favs_res.is_array());

    // 6. Delete playlist
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/v1/me/playlists/{playlist_id}"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 7. Logout via /api/v1/auth/logout (unauthenticated with refresh_token)
    let logout_payload = serde_json::json!({ "refresh_token": token });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/auth/logout")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&logout_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 8. Subsequent requests with revoked token must fail with 401 UNAUTHORIZED
    let req = Request::builder()
        .uri("/api/v1/auth/me")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let _ = db::integrations::delete_integration(&pool, admin_id, "lastfm").await;
}
