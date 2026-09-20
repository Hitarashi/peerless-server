use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use apple::{MANIFEST_URL, MirrorError, MirrorHttp, MirrorHttpError, MirrorPolicyManager};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
enum Reply {
    Body(String),
    Error(MirrorHttpError),
}

#[derive(Clone)]
struct FakeHttp {
    routes: HashMap<String, Reply>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeHttp {
    fn new() -> Self {
        Self {
            routes: HashMap::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn route(&mut self, needle: &str, reply: Reply) {
        self.routes.insert(needle.to_owned(), reply);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl MirrorHttp for FakeHttp {
    async fn get(
        &self,
        url: &str,
        headers: &[(&str, String)],
        timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> Result<String, MirrorHttpError> {
        let _ = (headers, timeout, signal);
        self.calls.lock().unwrap().push(url.to_owned());
        self.routes
            .iter()
            .find(|(needle, _)| url.contains(needle.as_str()))
            .map_or_else(
                || Err(MirrorHttpError::Status(404)),
                |(_, reply)| match reply {
                    Reply::Body(body) => Ok(body.clone()),
                    Reply::Error(error) => Err(error.clone()),
                },
            )
    }
}

fn manifest(mirror: &str, key: &str) -> String {
    format!(r#"{{"source":{{"apple":"{mirror}"}},"key":"{key}"}}"#)
}

fn ready_http() -> FakeHttp {
    let mut http = FakeHttp::new();
    http.route(
        MANIFEST_URL,
        Reply::Body(manifest("https://mirror/", " k ")),
    );
    http.route(
        "https://mirror/status",
        Reply::Body(r#"{"wrapper_instances":[1]}"#.into()),
    );
    http
}

#[tokio::test]
async fn env_override_returns_without_http_and_strips_slashes() {
    let http = FakeHttp::new();
    let manager = MirrorPolicyManager::new(
        http,
        Some(("https://configured///".into(), "secret".into())),
    );
    let endpoint = manager.get_endpoint(false, None).await.unwrap();
    assert_eq!(endpoint.mirror_url, "https://configured");
    assert_eq!(manager.http().calls().len(), 0);
}

#[tokio::test]
async fn empty_env_override_falls_through_to_manifest_discovery() {
    // Empty URL/key means "not configured" — discovery runs.
    let manager = MirrorPolicyManager::new(ready_http(), Some(("".into(), "".into())));
    let endpoint = manager.get_endpoint(false, None).await.unwrap();
    assert_eq!(endpoint.mirror_url, "https://mirror");
    assert_eq!(manager.http().calls().len(), 2, "manifest + status fetched");
}

#[tokio::test]
async fn manifest_network_failure_opens_circuit_and_reuses_stored_error() {
    let mut http = FakeHttp::new();
    http.route(
        MANIFEST_URL,
        Reply::Error(MirrorHttpError::Network("offline".into())),
    );
    let manager = MirrorPolicyManager::new(http, None);
    let first = manager.get_endpoint(false, None).await.unwrap_err();
    assert_eq!(
        first.to_string(),
        "Mirror manifest lookup timed out after 0ms: offline"
    );
    let second = manager.get_endpoint(false, None).await.unwrap_err();
    assert_eq!(second.to_string(), first.to_string());
    assert_eq!(manager.http().calls().len(), 1);
}

#[tokio::test]
async fn force_refresh_bypasses_circuit() {
    let mut http = FakeHttp::new();
    http.route(
        MANIFEST_URL,
        Reply::Error(MirrorHttpError::Network("offline".into())),
    );
    let manager = MirrorPolicyManager::new(http, None);
    manager.get_endpoint(false, None).await.unwrap_err();
    manager.get_endpoint(true, None).await.unwrap_err();
    assert_eq!(manager.http().calls().len(), 2);
}

#[tokio::test]
async fn cache_ttl_controls_refetch() {
    let manager = MirrorPolicyManager::with_config(
        ready_http(),
        None,
        Duration::from_secs(30),
        Duration::from_millis(1),
        Duration::from_secs(8),
    );
    manager.get_endpoint(false, None).await.unwrap();
    manager.get_endpoint(false, None).await.unwrap();
    assert_eq!(manager.http().calls().len(), 2);
    tokio::time::sleep(Duration::from_millis(3)).await;
    manager.get_endpoint(false, None).await.unwrap();
    assert_eq!(manager.http().calls().len(), 4);
}

#[tokio::test]
async fn manifest_http_and_missing_fields_have_exact_messages() {
    let mut http = FakeHttp::new();
    http.route(MANIFEST_URL, Reply::Error(MirrorHttpError::Status(503)));
    let manager = MirrorPolicyManager::new(http, None);
    assert_eq!(
        manager
            .get_endpoint(false, None)
            .await
            .unwrap_err()
            .to_string(),
        "Failed to fetch mirror manifest (HTTP 503)"
    );

    let mut http = FakeHttp::new();
    http.route(
        MANIFEST_URL,
        Reply::Body(r#"{"source":{"apple":"https://m"}}"#.into()),
    );
    let manager = MirrorPolicyManager::new(http, None);
    assert_eq!(
        manager
            .get_endpoint(false, None)
            .await
            .unwrap_err()
            .to_string(),
        "Mirror manifest returned empty apple endpoint or api key"
    );
}

#[tokio::test]
async fn status_failures_have_exact_messages() {
    let mut http = FakeHttp::new();
    http.route(MANIFEST_URL, Reply::Body(manifest("https://mirror", "key")));
    http.route(
        "https://mirror/status",
        Reply::Error(MirrorHttpError::Network("down".into())),
    );
    let manager = MirrorPolicyManager::new(http, None);
    assert_eq!(
        manager
            .get_endpoint(false, None)
            .await
            .unwrap_err()
            .to_string(),
        "Mirror /status check timed out after 0ms: down"
    );

    let mut http = FakeHttp::new();
    http.route(MANIFEST_URL, Reply::Body(manifest("https://mirror", "key")));
    http.route(
        "https://mirror/status",
        Reply::Error(MirrorHttpError::Status(502)),
    );
    let manager = MirrorPolicyManager::new(http, None);
    assert_eq!(
        manager
            .get_endpoint(false, None)
            .await
            .unwrap_err()
            .to_string(),
        "Mirror /status check failed (HTTP 502)"
    );
}

#[tokio::test]
async fn status_offline_rules_and_bad_json_match_ts() {
    for body in [
        r#"{"wrapper_lossless_available":false}"#,
        r#"{"wrapper_instances":[]}"#,
    ] {
        let mut http = FakeHttp::new();
        http.route(MANIFEST_URL, Reply::Body(manifest("https://mirror", "key")));
        http.route("https://mirror/status", Reply::Body(body.into()));
        let manager = MirrorPolicyManager::new(http, None);
        assert_eq!(
            manager
                .get_endpoint(false, None)
                .await
                .unwrap_err()
                .to_string(),
            "Lossless wrapper is currently offline on mirror"
        );
    }
    for body in [r#"{"wrapper_instances":"notarray"}"#, "not json"] {
        let mut http = FakeHttp::new();
        http.route(MANIFEST_URL, Reply::Body(manifest("https://mirror", "key")));
        http.route("https://mirror/status", Reply::Body(body.into()));
        let manager = MirrorPolicyManager::new(http, None);
        assert!(manager.get_endpoint(false, None).await.is_ok());
    }
}

#[tokio::test]
async fn success_caches_and_record_success_clears_circuit() {
    let manager = MirrorPolicyManager::new(ready_http(), None);
    manager.get_endpoint(false, None).await.unwrap();
    manager.get_endpoint(false, None).await.unwrap();
    assert_eq!(manager.http().calls().len(), 2);
    manager.record_failure("bad");
    assert!(manager.is_circuit_open());
    manager.record_success();
    assert!(!manager.is_circuit_open());
    manager.clear_cache();
    manager.get_endpoint(false, None).await.unwrap();
    assert_eq!(manager.http().calls().len(), 4);
}

#[tokio::test]
async fn malformed_manifest_json_propagates_without_opening_circuit() {
    let mut http = FakeHttp::new();
    http.route(MANIFEST_URL, Reply::Body("not json".into()));
    let manager = MirrorPolicyManager::new(http, None);
    assert!(matches!(
        manager.get_endpoint(false, None).await,
        Err(MirrorError::Json(_))
    ));
    assert!(!manager.is_circuit_open());
    assert_eq!(manager.http().calls().len(), 1);
}

#[tokio::test]
async fn shared_clone_observes_and_updates_the_same_policy_state() {
    let manager = MirrorPolicyManager::new(ready_http(), None);
    let shared = manager.shared();

    manager.record_failure("shared failure");
    assert!(shared.is_circuit_open());
    assert_eq!(
        shared
            .get_endpoint(false, None)
            .await
            .unwrap_err()
            .to_string(),
        "shared failure"
    );

    shared.clear_cache();
    assert!(!manager.is_circuit_open());
    manager.get_endpoint(false, None).await.unwrap();
    shared.get_endpoint(false, None).await.unwrap();
    assert_eq!(manager.http().calls().len(), 2, "cache is shared by clones");
}
