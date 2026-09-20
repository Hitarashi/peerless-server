use db::{Auth, connect_test_isolated, migrate};

#[tokio::test]
async fn authorization_and_migrations_round_trip() {
    let client = connect_test_isolated()
        .await
        .expect("TEST_DATABASE_URL and PostgreSQL are required for db tests");
    migrate(&client).await.expect("database migrations");
    migrate(&client).await.expect("migration is idempotent");

    let auth = Auth::new(client.clone(), 900_000_001);
    let user = 900_000_002;
    let chat = -900_000_003;
    let _ = auth.revoke(user).await;
    let _ = auth.revoke(chat).await;

    assert!(
        auth.authorize(user, Some("first"))
            .await
            .expect("authorize")
    );
    assert!(
        !auth
            .authorize(user, Some("updated"))
            .await
            .expect("re-authorize")
    );
    assert!(auth.is_authorized(user, None).await.expect("user auth"));
    assert!(
        auth.is_authorized(123, Some(user))
            .await
            .expect("chat auth")
    );
    assert!(!auth.is_authorized(123, Some(456)).await.expect("auth miss"));
    assert!(auth.is_admin(900_000_001));
    assert!(
        auth.is_authorized(900_000_001, None)
            .await
            .expect("admin auth")
    );

    assert!(
        auth.authorize(chat, Some("group"))
            .await
            .expect("group authorize")
    );
    let list = auth.list_authorized().await.expect("list authorized");
    let listed = list
        .iter()
        .find(|item| item.telegram_id == user)
        .expect("user listed");
    assert_eq!(listed.name.as_deref(), Some("updated"));
    assert!(listed.created_at.timestamp() > 0);

    assert!(auth.revoke(user).await.expect("revoke hit"));
    assert!(!auth.revoke(user).await.expect("revoke miss"));
    assert!(!auth.is_authorized(user, None).await.expect("revoked user"));
    assert!(auth.revoke(chat).await.expect("cleanup group"));
}
