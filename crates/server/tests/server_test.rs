use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use ferogram::PeerRef;
use server::{
    ServerState,
    rip_task_rpc::{
        AuthenticatedIdentity, RipTaskRpcErrorCode, RipTaskRpcRequest, RipTaskRpcStatus,
        RipTaskRpcSuccess,
    },
    streaming::{StreamTicket, create_stream_ticket, verify_stream_ticket},
};
use tower::ServiceExt;

async fn handle_rpc_as(
    session_mgr: &db::SessionManager,
    state: Arc<ServerState>,
    token: &str,
    expected_user_id: i64,
    request: RipTaskRpcRequest,
) -> Result<RipTaskRpcSuccess, server::rip_task_rpc::RipTaskRpcError> {
    // Match the WebSocket dispatcher: verify the live session immediately before each RPC.
    // verify_session is deliberately non-sliding and does not extend activity or expiry.
    let session = session_mgr
        .verify_session(token)
        .await
        .expect("RPC session must remain valid");
    assert_eq!(session.telegram_id, expected_user_id);

    server::rip_task_rpc::handle_request(
        AuthenticatedIdentity {
            telegram_id: session.telegram_id,
        },
        request,
        state,
    )
    .await
}

fn create_rip_rpc_request(
    request_id: &str,
    track_id: &str,
    codec: Option<&str>,
) -> RipTaskRpcRequest {
    RipTaskRpcRequest::Create {
        request_id: request_id.to_owned(),
        request: server::tasks::RipTaskRequest {
            provider: "apple".to_owned(),
            track_id: track_id.to_owned(),
            codec: codec.map(str::to_owned),
            title: None,
            artist: None,
            album: None,
            duration: None,
        },
    }
}

fn cancel_rip_rpc_request(request_id: &str, task_id: &str) -> RipTaskRpcRequest {
    RipTaskRpcRequest::Cancel {
        request_id: request_id.to_owned(),
        task_id: task_id.to_owned(),
    }
}

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

    let req = Request::builder()
        .uri("/api/v1/auth/me/avatar")
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

#[tokio::test]
async fn test_tasks_rip_create_and_cancel_lifecycle() {
    let _ = dotenvy::from_filename(".env");
    let Ok(database_url) =
        std::env::var("DATABASE_URL").or_else(|_| std::env::var("TEST_DATABASE_URL"))
    else {
        eprintln!(
            "Skipping database lifecycle test: neither DATABASE_URL nor TEST_DATABASE_URL is set"
        );
        return;
    };
    let pool = match db::connect(&database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to {database_url}: {e}");
            return;
        }
    };
    let worker_pool = stream::StreamWorkerPool::empty();
    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(pool.clone()),
        None,
        PeerRef::from(0),
    ));
    let admin_id = 77777;
    let owner_id = 12345;
    let other_id = 99999;

    let session_mgr = Arc::new(db::SessionManager::new(pool.clone(), admin_id));
    let library_mgr = Arc::new(db::LibraryManager::new(pool.clone()));
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_tasks_lifecycle_32";
    let state = Arc::new(
        ServerState::new(
            stream_engine,
            session_mgr.clone(),
            library_mgr,
            tracks_repo.clone(),
            settings_store,
            orchestrator,
            app_key.to_string(),
        )
        .with_admin_id(admin_id),
    );

    // Keep the existing database-backed authentication fixture. The RPC service is called
    // directly below, but every operation first verifies its live session as the WebSocket
    // dispatcher does.
    let app = server::create_router(state.clone());
    let auth = db::Auth::new(pool.clone(), admin_id);
    auth.authorize(owner_id, Some("Owner")).await.unwrap();
    auth.authorize(other_id, Some("Other")).await.unwrap();

    let get_token_for = |user_id: i64| {
        let session_mgr = session_mgr.clone();
        let app = app.clone();
        async move {
            let code = session_mgr.create_login_code(user_id).await.unwrap();
            let payload = serde_json::json!({ "code": code });
            let req = Request::builder()
                .method("POST")
                .uri("/api/v1/auth/exchange")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();
            let res = app.oneshot(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let body = axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap();
            let val: serde_json::Value = serde_json::from_slice(&body).unwrap();
            val["token"].as_str().unwrap().to_string()
        }
    };

    let owner_token = get_token_for(owner_id).await;
    let other_token = get_token_for(other_id).await;
    let admin_token = get_token_for(admin_id).await;
    let mut task_sync_events = state.task_sync_tx.subscribe();
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let owner_track_id = format!("lifecycle_owner_{unique_suffix}");

    // 1. Owner creates a new queued task through the typed RPC service.
    let created = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request("create-owner", &owner_track_id, Some("alac")),
    )
    .await
    .expect("owner create succeeds");
    let task_id = match created {
        RipTaskRpcSuccess::Created {
            request_id,
            task_id,
            status,
            result_track_id,
        } => {
            assert_eq!(request_id, "create-owner");
            assert_eq!(status, RipTaskRpcStatus::Queued);
            assert_eq!(result_track_id, None);
            assert!(!task_id.is_empty());
            task_id
        }
        other => panic!("expected created response, got {other:?}"),
    };

    let controller = {
        let tasks = state.active_tasks.read();
        let meta = tasks.get(&task_id).expect("task must be in active_tasks");
        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.owner_id, owner_id);
        assert_eq!(meta.track_id, owner_track_id);
        assert_eq!(meta.codec.as_deref(), Some("alac"));
        assert!(!meta.controller.is_cancelled());
        meta.controller.clone()
    };
    match task_sync_events.recv().await.unwrap() {
        server::tasks::TaskSyncEvent::Updated {
            task_id: updated_id,
        } => {
            assert_eq!(updated_id, task_id);
        }
        other => panic!("expected task update, got {other:?}"),
    }

    // 2. A non-owner/non-admin cannot cancel the task; it remains live and emits no feed event.
    let error = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &other_token,
        other_id,
        cancel_rip_rpc_request("cancel-forbidden", &task_id),
    )
    .await
    .expect_err("non-owner cancel must be rejected");
    assert_eq!(error.request_id.as_deref(), Some("cancel-forbidden"));
    assert_eq!(error.code, RipTaskRpcErrorCode::NotAuthorized);
    assert!(state.active_tasks.read().contains_key(&task_id));
    assert!(!controller.is_cancelled());
    assert!(matches!(
        task_sync_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    // 3. Cancelling a task id that does not exist returns NotFound.
    let error = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        cancel_rip_rpc_request("cancel-missing", "task_nonexistent_12345"),
    )
    .await
    .expect_err("missing task cancel must fail");
    assert_eq!(error.request_id.as_deref(), Some("cancel-missing"));
    assert_eq!(error.code, RipTaskRpcErrorCode::NotFound);
    assert!(matches!(
        task_sync_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    // 4. Owner cancels their task and the task feed reports its dismissal.
    assert_eq!(
        handle_rpc_as(
            &session_mgr,
            state.clone(),
            &owner_token,
            owner_id,
            cancel_rip_rpc_request("cancel-owner", &task_id),
        )
        .await
        .expect("owner cancel succeeds"),
        RipTaskRpcSuccess::Cancelled {
            request_id: "cancel-owner".to_owned(),
            task_id: task_id.clone(),
        }
    );
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == task_id
    ));
    assert!(controller.is_cancelled());
    assert!(!state.active_tasks.read().contains_key(&task_id));

    // 5. Admins can cancel a task owned by another user.
    let admin_task = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request(
            "create-admin-cancel",
            &format!("lifecycle_admin_{unique_suffix}"),
            Some("alac"),
        ),
    )
    .await
    .expect("admin-cancellable task create succeeds");
    let admin_task_id = match admin_task {
        RipTaskRpcSuccess::Created {
            task_id,
            status: RipTaskRpcStatus::Queued,
            result_track_id: None,
            ..
        } => task_id,
        other => panic!("expected queued task, got {other:?}"),
    };
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Updated { task_id: updated_id }
            if updated_id == admin_task_id
    ));
    assert_eq!(
        handle_rpc_as(
            &session_mgr,
            state.clone(),
            &admin_token,
            admin_id,
            cancel_rip_rpc_request("cancel-as-admin", &admin_task_id),
        )
        .await
        .expect("admin cancel succeeds"),
        RipTaskRpcSuccess::Cancelled {
            request_id: "cancel-as-admin".to_owned(),
            task_id: admin_task_id.clone(),
        }
    );
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == admin_task_id
    ));
    assert!(!state.active_tasks.read().contains_key(&admin_task_id));

    // 6. Identical in-flight requests coalesce after provider/track/codec normalization.
    let dedup_track_id = format!("lifecycle_dedup_{unique_suffix}");
    let dedup_first = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request("dedup-first", &dedup_track_id, Some("ALAC")),
    )
    .await
    .expect("first dedup create succeeds");
    let dedup_task_id = match dedup_first {
        RipTaskRpcSuccess::Created {
            request_id,
            task_id,
            status,
            result_track_id,
        } => {
            assert_eq!(request_id, "dedup-first");
            assert_eq!(status, RipTaskRpcStatus::Queued);
            assert_eq!(result_track_id, None);
            task_id
        }
        other => panic!("expected created response, got {other:?}"),
    };
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Updated { task_id: updated_id }
            if updated_id == dedup_task_id
    ));

    let dedup_again = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request("dedup-again", &dedup_track_id, Some("alac")),
    )
    .await
    .expect("duplicate in-flight create succeeds");
    assert_eq!(
        dedup_again,
        RipTaskRpcSuccess::Created {
            request_id: "dedup-again".to_owned(),
            task_id: dedup_task_id.clone(),
            status: RipTaskRpcStatus::Queued,
            result_track_id: None,
        }
    );
    assert!(matches!(
        task_sync_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    // A different normalized codec is a distinct dedup key and gets its own task.
    let different_codec = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request("dedup-flac", &dedup_track_id, Some("flac")),
    )
    .await
    .expect("different codec create succeeds");
    let different_codec_task_id = match different_codec {
        RipTaskRpcSuccess::Created {
            task_id,
            status: RipTaskRpcStatus::Queued,
            result_track_id: None,
            ..
        } => task_id,
        other => panic!("expected distinct queued task, got {other:?}"),
    };
    assert_ne!(different_codec_task_id, dedup_task_id);
    assert_eq!(
        state.active_tasks.read()[&different_codec_task_id]
            .codec
            .as_deref(),
        Some("flac")
    );
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Updated { task_id: updated_id }
            if updated_id == different_codec_task_id
    ));

    // Completed tasks disappear from the active task feed.
    state.complete_task(&dedup_task_id);
    assert!(!state.active_tasks.read().contains_key(&dedup_task_id));
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == dedup_task_id
    ));
    state.complete_task(&different_codec_task_id);
    assert!(
        !state
            .active_tasks
            .read()
            .contains_key(&different_codec_task_id)
    );
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == different_codec_task_id
    ));

    // 7. Fast-path DB cache hit returns the result id and an empty, non-cancellable task id.
    let save_input = engine::orchestrator::deps::SaveTrackInput {
        track_key: music::TrackKey::new(
            music::Provider::Apple,
            "cached_track_fastpath".to_string(),
        ),
        codec: music::Codec::Alac,
        message_id: 202,
        file_id: "tg_file_fastpath".to_string(),
        file_unique_id: "unique_fastpath".to_string(),
        title: "Fastpath Song".to_string(),
        artist: "Fastpath Artist".to_string(),
        album: "Fastpath Album".to_string(),
        duration: 210,
        bit_depth: 24,
        sample_rate: 96000,
        genre: "Pop".to_string(),
        release_date: "2024".to_string(),
        track_number: 1,
        track_count: 1,
        isrc: None,
    };
    tracks_repo.save_track(&save_input).await.unwrap();
    let cached_track = tracks_repo
        .find_all_by_provider_track_id(music::Provider::Apple, "cached_track_fastpath")
        .await
        .unwrap()
        .into_iter()
        .find(|track| track.codec == music::Codec::Alac)
        .expect("saved fast-path track must be queryable");

    let cached = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request("cache-hit", "cached_track_fastpath", Some("alac")),
    )
    .await
    .expect("cached create succeeds");
    assert_eq!(
        cached,
        RipTaskRpcSuccess::Created {
            request_id: "cache-hit".to_owned(),
            task_id: String::new(),
            status: RipTaskRpcStatus::Completed,
            result_track_id: Some(cached_track.id),
        }
    );
    assert!(state.active_tasks.read().is_empty());
    assert!(matches!(
        task_sync_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
