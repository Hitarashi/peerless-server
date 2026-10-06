use db::{Auth, ClientMetadata, SessionManager, connect_test_isolated, migrate};

#[tokio::test]
async fn session_lifecycle_and_sliding_auth() {
    let client = match connect_test_isolated().await {
        Ok(c) => c,
        Err(_) => {
            eprintln!("Skipping session test: TEST_DATABASE_URL not set");
            return;
        }
    };
    migrate(&client).await.expect("database migrations");

    let admin_id = 999_000_001;
    let auth = Auth::new(client.clone(), admin_id);
    let session_mgr = SessionManager::new(client.clone(), admin_id);
    let test_user = 999_000_002;

    let admin_code = session_mgr
        .create_login_code(admin_id)
        .await
        .expect("create admin login code directly");
    assert_eq!(admin_code.len(), 7);
    assert_eq!(
        session_mgr
            .get_user_name(admin_id)
            .await
            .unwrap()
            .as_deref(),
        Some("Admin")
    );
    assert!(
        !session_mgr
            .update_user_name_if_changed(admin_id, "Admin")
            .await
            .unwrap(),
        "an unchanged Telegram name must not trigger a database update"
    );
    assert!(
        session_mgr
            .update_user_name_if_changed(admin_id, "Sayeed Hitarashi")
            .await
            .unwrap(),
        "a changed Telegram name must update the existing user row"
    );
    assert_eq!(
        session_mgr
            .get_user_name(admin_id)
            .await
            .unwrap()
            .as_deref(),
        Some("Sayeed Hitarashi")
    );
    assert!(
        !session_mgr
            .update_user_name_if_changed(admin_id, "Sayeed Hitarashi")
            .await
            .unwrap(),
        "rechecking the same name must not trigger another update"
    );
    let unknown_user = 999_000_099;
    assert!(
        !session_mgr
            .update_user_name_if_changed(unknown_user, "Unknown")
            .await
            .unwrap(),
        "profile sync must not create an authorized user row"
    );
    assert!(!auth.is_authorized(unknown_user, None).await.unwrap());

    let admin_tokens = session_mgr
        .exchange_code(
            &admin_code,
            ClientMetadata {
                client_name: Some("peerless-cli"),
                client_version: Some("1.0.0"),
                device_name: Some("Admin Terminal"),
                platform: Some("linux"),
            },
        )
        .await
        .expect("admin exchange code");
    assert_eq!(admin_tokens.telegram_id, admin_id);

    let admin_identity = session_mgr
        .verify_and_slide(&admin_tokens.refresh_token)
        .await
        .expect("admin verify and slide");
    assert_eq!(admin_identity.telegram_id, admin_id);

    let unauth_res = session_mgr.create_login_code(test_user).await;
    assert!(
        unauth_res.is_err(),
        "Unauthorized user must not be able to generate login code"
    );

    auth.authorize(test_user, Some("Session Test User"))
        .await
        .expect("authorize user");

    let code = session_mgr
        .create_login_code(test_user)
        .await
        .expect("create login code");
    assert_eq!(code.len(), 7, "Code format should be XXX-XXX (7 chars)");

    let meta = ClientMetadata {
        client_name: Some("Laboon"),
        client_version: Some("1.2.3"),
        device_name: Some("Pixel 8"),
        platform: Some("android"),
    };

    let bad_exchange = session_mgr
        .exchange_code("INVALID-CODE", meta.clone())
        .await;
    assert!(bad_exchange.is_err());

    let tokens = session_mgr
        .exchange_code(&code, meta.clone())
        .await
        .expect("exchange code");
    assert_eq!(tokens.telegram_id, test_user);
    assert_eq!(
        tokens.refresh_token.len(),
        64,
        "Refresh token must be 64-char hex string (32 bytes)"
    );
    assert!(tokens.expires_at > chrono::Utc::now());

    let replay_res = session_mgr.exchange_code(&code, meta.clone()).await;
    assert!(replay_res.is_err(), "Single-use code cannot be reused");

    let identity = session_mgr
        .verify_and_slide(&tokens.refresh_token)
        .await
        .expect("verify and slide");
    assert_eq!(identity.telegram_id, test_user);
    assert_eq!(identity.session_id, tokens.session_id);
    assert!(identity.expires_at >= tokens.expires_at);

    let active_sessions = session_mgr
        .list_active_sessions(test_user)
        .await
        .expect("list sessions");
    assert_eq!(active_sessions.len(), 1);
    assert_eq!(active_sessions[0].id, tokens.session_id);
    assert_eq!(active_sessions[0].client_name.as_deref(), Some("Laboon"));
    assert_eq!(active_sessions[0].client_version.as_deref(), Some("1.2.3"));

    let revoked = session_mgr
        .revoke(&tokens.refresh_token)
        .await
        .expect("revoke session");
    assert!(revoked);
    let post_revoke = session_mgr.verify_and_slide(&tokens.refresh_token).await;
    assert!(post_revoke.is_err(), "Revoked token must be rejected");

    let sessions_after_single_revoke = session_mgr
        .list_active_sessions(test_user)
        .await
        .expect("list sessions");
    assert!(
        sessions_after_single_revoke.is_empty(),
        "No active sessions remain after single revoke"
    );

    let code2 = session_mgr
        .create_login_code(test_user)
        .await
        .expect("create login code 2");
    let _tokens2 = session_mgr
        .exchange_code(&code2, meta.clone())
        .await
        .expect("exchange code 2");
    let active_before_all = session_mgr
        .list_active_sessions(test_user)
        .await
        .expect("list sessions");
    assert_eq!(active_before_all.len(), 1);

    let revoked_count = session_mgr
        .revoke_all_for_user(test_user)
        .await
        .expect("revoke all for user");
    assert!(revoked_count >= 1);
    let active_after_all = session_mgr
        .list_active_sessions(test_user)
        .await
        .expect("list sessions");
    assert!(active_after_all.is_empty(), "All sessions revoked");

    let code3 = session_mgr
        .create_login_code(test_user)
        .await
        .expect("create login code 3");
    let tokens3 = session_mgr
        .exchange_code(&code3, meta)
        .await
        .expect("exchange code 3");

    auth.revoke(test_user).await.expect("revoke user");

    let post_user_revoke = session_mgr.verify_and_slide(&tokens3.refresh_token).await;
    assert!(
        post_user_revoke.is_err(),
        "Revoked user must be rejected on token verification"
    );

    let sessions_after_user_revoke = session_mgr
        .list_active_sessions(test_user)
        .await
        .expect("list sessions");
    assert!(
        sessions_after_user_revoke.is_empty(),
        "No active sessions remain after user revocation"
    );

    session_mgr
        .revoke(&admin_tokens.refresh_token)
        .await
        .expect("revoke admin session");
    let purged_sessions = session_mgr
        .purge_expired_sessions()
        .await
        .expect("purge expired sessions");
    assert!(purged_sessions >= 1, "Purged revoked/expired sessions");

    let _purged_codes = session_mgr
        .purge_expired_codes()
        .await
        .expect("purge expired codes");

    let worker_store = db::WorkerSessionStore::new(client.clone(), None);
    let token_hash = "abc123def4567890123456789012345678901234567890123456789012345678";

    assert_eq!(
        worker_store
            .get_session(token_hash)
            .await
            .expect("get initial"),
        None
    );

    worker_store
        .save_session(token_hash, "session_data_v1")
        .await
        .expect("save session v1");
    assert_eq!(
        worker_store
            .get_session(token_hash)
            .await
            .expect("get saved"),
        Some("session_data_v1".to_string())
    );

    worker_store
        .save_session(token_hash, "session_data_v2")
        .await
        .expect("save session v2");
    assert_eq!(
        worker_store
            .get_session(token_hash)
            .await
            .expect("get updated"),
        Some("session_data_v2".to_string())
    );

    assert!(
        worker_store
            .delete_session(token_hash)
            .await
            .expect("delete session")
    );
    assert_eq!(
        worker_store
            .get_session(token_hash)
            .await
            .expect("get after delete"),
        None
    );

    let secret = "test-secret-key-32-bytes-long-abc";
    let enc_store = db::WorkerSessionStore::new(client.clone(), Some(secret));
    let enc_token_hash = "111222333444555666777888999000aaabbbcccdddeeefff1112223334445556";
    let plain_session = "1?dc=2&auth_key=deadbeefcafe1234567890";

    enc_store
        .save_session(enc_token_hash, plain_session)
        .await
        .expect("save encrypted session");

    let loaded = enc_store
        .get_session(enc_token_hash)
        .await
        .expect("get encrypted session");
    assert_eq!(loaded, Some(plain_session.to_string()));

    let raw_store = db::WorkerSessionStore::new(client.clone(), None);
    let raw_db_data = raw_store
        .get_session(enc_token_hash)
        .await
        .expect("get raw DB session")
        .expect("raw DB row exists");
    assert_ne!(raw_db_data, plain_session);

    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&raw_db_data)
        .expect("ciphertext is valid base64");
    assert!(decoded.len() >= 12, "ciphertext has at least 12-byte nonce");

    let wrong_key_store = db::WorkerSessionStore::new(client.clone(), Some("wrong-secret-key"));
    let decrypt_failed = wrong_key_store
        .get_session(enc_token_hash)
        .await
        .expect("wrong key returns Ok(None)");
    assert_eq!(decrypt_failed, None);

    assert!(
        enc_store
            .delete_session(enc_token_hash)
            .await
            .expect("delete encrypted session")
    );
    assert_eq!(
        enc_store
            .get_session(enc_token_hash)
            .await
            .expect("get after delete"),
        None
    );
}
