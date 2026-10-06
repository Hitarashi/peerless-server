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
    _codec: Option<&str>,
) -> RipTaskRpcRequest {
    RipTaskRpcRequest::Create {
        request_id: request_id.to_owned(),
        request: server::rip_tasks::RipTaskRequest {
            track_id: Some(track_id.to_owned()),
            album_id: None,
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

    let pt = StreamTicket::new(track_id, user_id, ttl);
    let encoded = pt.encode(key);
    let decoded = StreamTicket::decode(key, &encoded).expect("valid ticket must decode");
    assert_eq!(decoded.db_track_id, track_id);
    assert_eq!(decoded.user_id, user_id);

    assert!(verify_stream_ticket("wrong_key", &ticket).is_err());
    assert!(StreamTicket::decode("wrong_key", &encoded).is_err());

    let expired_ticket = create_stream_ticket(key, track_id, user_id, -10);
    assert!(verify_stream_ticket(key, &expired_ticket).is_err());
    let expired_pt = StreamTicket::new(track_id, user_id, -10);
    assert!(StreamTicket::decode(key, &expired_pt.encode(key)).is_err());
}

async fn test_pool() -> Option<db::DbPool> {
    let _ = dotenvy::from_filename(".env");
    let pool = if let Ok(pool) = db::connect_test_isolated().await {
        pool
    } else if let Ok(db_url) =
        std::env::var("DATABASE_URL").or_else(|_| std::env::var("TEST_DATABASE_URL"))
    {
        match db::connect(&db_url).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Skipping test: cannot connect to {db_url}: {e}");
                return None;
            }
        }
    } else {
        eprintln!("Skipping test: TEST_DATABASE_URL/DATABASE_URL not set");
        return None;
    };
    db::migrate(&pool).await.expect("database migrations");
    Some(pool)
}

#[tokio::test]
async fn test_docs_and_unauthorized_endpoints() {
    let Some(pool) = test_pool().await else {
        return;
    };

    let worker_pool = stream::StreamWorkerPool::empty();
    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(pool.clone()),
        None,
        PeerRef::from(0),
    ));

    let session_mgr = Arc::new(db::SessionManager::new(pool.clone(), 12345));
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_testing_routes_32_bytes";
    let state = Arc::new(ServerState::new(
        stream_engine,
        session_mgr,
        tracks_repo,
        settings_store,
        orchestrator,
        app_key.to_string(),
    ));

    let app = server::create_router(state);

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

    let req = Request::builder()
        .uri("/api/v1/docs.yaml")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

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

    let req = Request::builder()
        .uri("/api/v1/tracks/1/playback")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let req = Request::builder()
        .uri("/api/v1/tracks/1/stream")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let req = Request::builder()
        .uri("/api/v1/tracks/1/stream?ticket=bogus_ticket_signature")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let req = Request::builder()
        .uri("/api/v1/stream")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

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

    let req = Request::builder().uri("/open").body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

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

    let lookup_payload = serde_json::json!({
        "track_ids": ["2147483000"],
        "album_ids": ["test_album"]
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/lookup")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&lookup_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_lookup_requires_authenticated_session() {
    let _ = dotenvy::from_filename(".env");
    let Ok(db_url) = std::env::var("DATABASE_URL").or_else(|_| std::env::var("TEST_DATABASE_URL"))
    else {
        eprintln!("Skipping lookup authentication test: DATABASE_URL not set");
        return;
    };

    let pool = match db::connect(&db_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping lookup authentication test: cannot connect to {db_url}: {e}");
            return;
        }
    };
    db::migrate(&pool).await.expect("database migrations");

    let user_id = 666_000_321;
    let worker_pool = stream::StreamWorkerPool::empty();
    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(pool.clone()),
        None,
        PeerRef::from(0),
    ));

    let session_mgr = Arc::new(db::SessionManager::new(pool.clone(), user_id));
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_lookup_auth_32_bytes";
    let state = Arc::new(ServerState::new(
        stream_engine,
        session_mgr.clone(),
        tracks_repo,
        settings_store,
        orchestrator,
        app_key.to_string(),
    ));

    let app = server::create_router(state);

    let code = session_mgr.create_login_code(user_id).await.unwrap();
    let exchange_payload = serde_json::json!({
        "code": code,
        "device_name": "Lookup Auth Test",
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
    let token = exchange_res["token"].as_str().unwrap().to_string();
    assert!(!token.is_empty());

    let lookup_payload = serde_json::json!({
        "track_ids": ["2147483000"],
        "album_ids": ["test_album"]
    });
    let body = Body::from(serde_json::to_vec(&lookup_payload).unwrap());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/lookup")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let body = Body::from(serde_json::to_vec(&lookup_payload).unwrap());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/lookup")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(body)
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_auth_lifecycle() {
    let Some(pool) = test_pool().await else {
        return;
    };

    let admin_id = 888_000_123;
    let _ = db::integrations::delete_integration(&pool, admin_id, "lastfm").await;
    let _ = db::integrations::delete_integration(&pool, admin_id, "listenbrainz").await;
    let worker_pool = stream::StreamWorkerPool::empty();
    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(pool.clone()),
        None,
        PeerRef::from(0),
    ));

    let session_mgr = Arc::new(db::SessionManager::new(pool.clone(), admin_id));
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let playback_track_key = format!("auth_lifecycle_{admin_id}");
    tracks_repo
        .save_track(&engine::orchestrator::deps::SaveTrackInput {
            track_id: playback_track_key.clone(),
            codec: music::Codec::Alac,
            message_id: 987_654_321,
            file_id: "auth_lifecycle_file".to_owned(),
            file_unique_id: "auth_lifecycle_unique".to_owned(),
        })
        .await
        .unwrap();
    let playback_track_id = tracks_repo
        .find_all_by_track_id(&playback_track_key)
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("auth lifecycle track is stored")
        .id;
    let playback_uri = format!("/api/v1/tracks/{playback_track_id}/playback");
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_testing_lifecycle_32";
    let state = Arc::new(ServerState::new(
        stream_engine,
        session_mgr.clone(),
        tracks_repo,
        settings_store,
        orchestrator,
        app_key.to_string(),
    ));

    let app = server::create_router(state);

    let otp_code = session_mgr.create_login_code(admin_id).await.unwrap();

    let exchange_payload = serde_json::json!({
        "code": otp_code,
        "client_name": "Laboon",
        "client_version": "0.0.3",
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

    let req = Request::builder()
        .uri(&playback_uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let forbidden_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let forbidden_msg = forbidden_res["message"].as_str().unwrap();
    assert!(
        forbidden_msg.contains("Last.fm") && forbidden_msg.contains("ListenBrainz"),
        "with neither account connected the message must name both providers, got {forbidden_msg:?}"
    );

    let cipher = db::crypto::CryptoCipher::new(app_key).unwrap();
    let enc_key = cipher.encrypt("lastfm_test_session_key").unwrap();
    db::integrations::save_integration(&pool, admin_id, "lastfm", "testuser", &enc_key)
        .await
        .unwrap();

    let req = Request::builder()
        .uri(&playback_uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let forbidden_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let forbidden_msg = forbidden_res["message"].as_str().unwrap();
    assert!(
        forbidden_msg.contains("ListenBrainz"),
        "with only Last.fm connected the message must name ListenBrainz, got {forbidden_msg:?}"
    );
    assert!(
        !forbidden_msg.contains("Last.fm"),
        "with only Last.fm connected the message must not name the connected provider, got {forbidden_msg:?}"
    );

    let lb_enc_key = cipher.encrypt("test_lb_token_abc_123").unwrap();
    db::integrations::save_integration(
        &pool,
        admin_id,
        "listenbrainz",
        "test_lb_user",
        &lb_enc_key,
    )
    .await
    .unwrap();
    db::integrations::delete_integration(&pool, admin_id, "lastfm")
        .await
        .unwrap();

    let req = Request::builder()
        .uri(&playback_uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let forbidden_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let forbidden_msg = forbidden_res["message"].as_str().unwrap();
    assert!(
        forbidden_msg.contains("Last.fm"),
        "with only ListenBrainz connected the message must name Last.fm, got {forbidden_msg:?}"
    );
    assert!(
        !forbidden_msg.contains("ListenBrainz"),
        "with only ListenBrainz connected the message must not name the connected provider, got {forbidden_msg:?}"
    );

    db::integrations::save_integration(&pool, admin_id, "lastfm", "testuser", &enc_key)
        .await
        .unwrap();

    let req = Request::builder()
        .uri(&playback_uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let pb_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        pb_res["stream_url"]
            .as_str()
            .unwrap()
            .contains("/api/v1/tracks/1/stream?ticket=")
    );
    assert_eq!(pb_res["expires_in"], 7200);
    assert!(pb_res["file_size"].as_i64().is_some());

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

    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/integrations/lastfm")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

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

    let req = Request::builder()
        .uri(&playback_uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let forbidden_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let forbidden_msg = forbidden_res["message"].as_str().unwrap();
    assert!(
        forbidden_msg.contains("Last.fm") && !forbidden_msg.contains("ListenBrainz"),
        "after disconnecting Last.fm only ListenBrainz remains, so only Last.fm should be named, got {forbidden_msg:?}"
    );

    db::integrations::save_integration(&pool, admin_id, "lastfm", "testuser", &enc_key)
        .await
        .unwrap();

    let req = Request::builder()
        .uri("/api/v1/tracks/99999999/playback")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let req = Request::builder()
        .uri("/api/v1/albums/test_album")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

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
    let sessions = me_res["sessions"].as_array().unwrap();
    assert!(!sessions.is_empty());
    assert_eq!(sessions[0]["client_name"], "Laboon");
    assert_eq!(sessions[0]["client_version"], "0.0.3");

    db::integrations::delete_integration(&pool, admin_id, "listenbrainz")
        .await
        .unwrap();

    let req = Request::builder()
        .uri("/api/v1/integrations/listenbrainz/status")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let lb_status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(lb_status["connected"], false);

    let cipher = db::crypto::CryptoCipher::new(app_key).unwrap();
    let encrypted = cipher.encrypt("test_lb_token_abc_123").unwrap();
    db::integrations::save_integration(&pool, admin_id, "listenbrainz", "test_lb_user", &encrypted)
        .await
        .unwrap();

    let req = Request::builder()
        .uri("/api/v1/integrations/listenbrainz/status")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let lb_status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(lb_status["connected"], true);
    assert_eq!(lb_status["username"], "test_lb_user");
    assert_eq!(lb_status["token"], "test_lb_token_abc_123");

    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/integrations/listenbrainz")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let req = Request::builder()
        .uri("/api/v1/integrations/listenbrainz/status")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let lb_status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(lb_status["connected"], false);

    let logout_payload = serde_json::json!({ "refresh_token": token });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/auth/logout")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&logout_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .uri("/api/v1/auth/me")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let _ = db::integrations::delete_integration(&pool, admin_id, "lastfm").await;
    let _ = db::integrations::delete_integration(&pool, admin_id, "listenbrainz").await;
}

#[tokio::test]
async fn test_tasks_rip_create_and_cancel_lifecycle() {
    let Some(pool) = test_pool().await else {
        return;
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
    let tracks_repo = Arc::new(db::TracksRepository::new(pool.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(pool.clone()));
    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::default());

    let app_key = "test_app_key_for_tasks_lifecycle_32";
    let state = Arc::new(
        ServerState::new(
            stream_engine,
            session_mgr.clone(),
            tracks_repo.clone(),
            settings_store,
            orchestrator,
            app_key.to_string(),
        )
        .with_admin_id(admin_id),
    );

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
    let mut task_sync_events = state.subscribe_task_sync();
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let owner_track_id = format!("lifecycle_owner_{unique_suffix}");

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
        let meta = state
            .tasks()
            .get(&task_id)
            .expect("task must be in the active-task registry");
        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.owner_id, owner_id);
        assert_eq!(meta.track_id, owner_track_id);
        assert_eq!(meta.codec.as_deref(), None);
        assert!(!meta.controller.is_cancelled());
        meta.controller.clone()
    };
    match task_sync_events.recv().await.unwrap() {
        server::rip_tasks::TaskSyncEvent::Updated {
            task_id: updated_id,
        } => {
            assert_eq!(updated_id, task_id);
        }
        other => panic!("expected task update, got {other:?}"),
    }

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
    assert!(state.tasks().contains_key(&task_id));
    assert!(!controller.is_cancelled());
    assert!(matches!(
        task_sync_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

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
        server::rip_tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == task_id
    ));
    assert!(controller.is_cancelled());
    assert!(!state.tasks().contains_key(&task_id));

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
        server::rip_tasks::TaskSyncEvent::Updated { task_id: updated_id }
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
        server::rip_tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == admin_task_id
    ));
    assert!(!state.tasks().contains_key(&admin_task_id));

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
        server::rip_tasks::TaskSyncEvent::Updated { task_id: updated_id }
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

    let distinct_track_id = format!("lifecycle_distinct_{unique_suffix}");
    let distinct_task = handle_rpc_as(
        &session_mgr,
        state.clone(),
        &owner_token,
        owner_id,
        create_rip_rpc_request("distinct-task", &distinct_track_id, None),
    )
    .await
    .expect("distinct track create succeeds");
    let distinct_task_id = match distinct_task {
        RipTaskRpcSuccess::Created {
            task_id,
            status: RipTaskRpcStatus::Queued,
            result_track_id: None,
            ..
        } => task_id,
        other => panic!("expected distinct queued task, got {other:?}"),
    };
    assert_ne!(distinct_task_id, dedup_task_id);
    assert_eq!(
        state
            .tasks()
            .get(&distinct_task_id)
            .expect("distinct task must be registered")
            .codec
            .as_deref(),
        None
    );
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::rip_tasks::TaskSyncEvent::Updated { task_id: updated_id }
            if updated_id == distinct_task_id
    ));

    state.complete_task(&dedup_task_id);
    assert!(!state.tasks().contains_key(&dedup_task_id));
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::rip_tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == dedup_task_id
    ));
    state.complete_task(&distinct_task_id);
    assert!(!state.tasks().contains_key(&distinct_task_id));
    assert!(matches!(
        task_sync_events.recv().await.unwrap(),
        server::rip_tasks::TaskSyncEvent::Dismissed { task_id: dismissed_id }
            if dismissed_id == distinct_task_id
    ));

    let save_input = engine::orchestrator::deps::SaveTrackInput {
        track_id: "cached_track_fastpath".to_string(),
        codec: music::Codec::Alac,
        message_id: 202,
        file_id: "tg_file_fastpath".to_string(),
        file_unique_id: "unique_fastpath".to_string(),
    };
    tracks_repo.save_track(&save_input).await.unwrap();
    let cached_track = tracks_repo
        .find_all_by_track_id("cached_track_fastpath")
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
    assert!(state.tasks().is_empty());
    assert!(matches!(
        task_sync_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
