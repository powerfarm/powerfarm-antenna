mod common;

use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

fn signed_headers(secret: &str, delivery: &str, event: &str, body: &[u8], timestamp: i64) -> Vec<(&'static str, String)> {
    let digest = antenna::observation::sha256_hex(body);
    let canonical = format!("{timestamp}\n{delivery}\n{event}\n{digest}\n");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(canonical.as_bytes());
    vec![
        ("x-antenna-timestamp", timestamp.to_string()),
        ("x-antenna-delivery", delivery.to_string()),
        ("x-antenna-github-event", event.to_string()),
        ("x-antenna-body-sha256", digest),
        ("x-antenna-signature", format!("v1={}", hex::encode(mac.finalize().into_bytes()))),
    ]
}

async fn harness(tag: &str) -> common::Harness {
    let dir = common::temp_dir(tag);
    let mut cfg = common::config_for(&dir, &[]);
    cfg.github.handoff_secret = Some("integration-handoff-secret".into());
    common::start_with_config(dir, cfg).await
}

async fn post_signed(h: &common::Harness, delivery: &str, event: &str, body: &[u8]) -> reqwest::Response {
    let now = chrono::Utc::now().timestamp();
    let mut request = common::client().post(format!("{}/internal/github", h.base)).body(body.to_vec());
    for (name, value) in signed_headers("integration-handoff-secret", delivery, event, body, now) {
        request = request.header(name, value);
    }
    request.send().await.unwrap()
}

#[tokio::test]
async fn valid_handoff_is_durable_before_acknowledgement() {
    let h = harness("github-valid").await;
    let body = br#"{"action":"created","repository":{"id":42,"full_name":"powerfarm/new","owner":{"login":"powerfarm"},"visibility":"private","archived":false,"default_branch":"main"},"installation":{"id":9}}"#;
    let response = post_signed(&h, "delivery-valid", "repository", body).await;
    assert_eq!(response.status(), 202);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["status"], "durably_accepted");
    let delivery_count: i64 = h.app.db.with(|c| Ok(c.query_row(
        "SELECT COUNT(*) FROM github_deliveries WHERE delivery_id='delivery-valid'", [], |r| r.get(0))?)).unwrap();
    let receipt_count: i64 = h.app.db.with(|c| Ok(c.query_row(
        "SELECT COUNT(*) FROM receipts WHERE id=?1", [value["receipt_id"].as_str().unwrap()], |r| r.get(0))?)).unwrap();
    assert_eq!((delivery_count, receipt_count), (1, 1));
}

#[tokio::test]
async fn invalid_tampered_and_missing_identity_are_rejected() {
    let h = harness("github-reject").await;
    let body = b"{}";
    let response = common::client().post(format!("{}/internal/github", h.base)).body(body.to_vec()).send().await.unwrap();
    assert_eq!(response.status(), 401);

    let now = chrono::Utc::now().timestamp();
    let mut request = common::client().post(format!("{}/internal/github", h.base)).body(b"tampered".to_vec());
    for (name, value) in signed_headers("integration-handoff-secret", "delivery-tampered", "push", body, now) {
        request = request.header(name, value);
    }
    assert_eq!(request.send().await.unwrap().status(), 401);

    let mut request = common::client().post(format!("{}/internal/github", h.base)).body(body.to_vec());
    for (name, value) in signed_headers("wrong-secret", "delivery-invalid", "push", body, now) {
        request = request.header(name, value);
    }
    assert_eq!(request.send().await.unwrap().status(), 401);
}

#[tokio::test]
async fn duplicate_delivery_has_two_receipts_but_one_observation() {
    let h = harness("github-dedupe").await;
    let body = serde_json::to_vec(&json!({
        "repository": {"id": 43, "full_name": "powerfarm/repo", "owner": {"login":"powerfarm"}},
        "ref": "refs/heads/main", "before": "a", "after": "b"
    })).unwrap();
    let first: Value = post_signed(&h, "same-delivery", "push", &body).await.json().await.unwrap();
    let second: Value = post_signed(&h, "same-delivery", "push", &body).await.json().await.unwrap();
    assert_eq!(first["duplicate"], false);
    assert_eq!(second["duplicate"], true);
    let counts: (i64, i64, i64) = h.app.db.with(|c| Ok((
        c.query_row("SELECT COUNT(*) FROM github_delivery_receipts WHERE delivery_id='same-delivery'", [], |r| r.get(0))?,
        c.query_row("SELECT COUNT(*) FROM observations WHERE github_delivery_id='same-delivery'", [], |r| r.get(0))?,
        c.query_row("SELECT duplicate_count FROM github_deliveries WHERE delivery_id='same-delivery'", [], |r| r.get(0))?,
    ))).unwrap();
    assert_eq!(counts, (2, 1, 1));
}

#[tokio::test]
async fn unknown_event_and_action_are_retained_as_unknown() {
    let h = harness("github-unknown").await;
    let body = br#"{"action":"future_action","repository":{"id":44,"full_name":"powerfarm/future"}}"#;
    let value: Value = post_signed(&h, "delivery-unknown", "future_event", body).await.json().await.unwrap();
    assert_eq!(value["classification"], "UNKNOWN");
    let stored: (String, String, String) = h.app.db.with(|c| Ok(c.query_row(
        "SELECT classification,event_name,action FROM observations WHERE github_delivery_id='delivery-unknown'",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?)).unwrap();
    assert_eq!(stored, ("UNKNOWN".into(), "future_event".into(), "future_action".into()));
}

#[tokio::test]
async fn oversized_internal_request_is_rejected_without_receipt() {
    let dir = common::temp_dir("github-oversize");
    let mut cfg = common::config_for(&dir, &[]);
    cfg.github.handoff_secret = Some("integration-handoff-secret".into());
    cfg.server.max_event_bytes = 128;
    let h = common::start_with_config(dir, cfg).await;
    let body = vec![b'x'; 129];
    let response = post_signed(&h, "delivery-large", "push", &body).await;
    assert_eq!(response.status(), 413);
    let count: i64 = h.app.db.with(|c| Ok(c.query_row("SELECT COUNT(*) FROM receipts", [], |r| r.get(0))?)).unwrap();
    assert_eq!(count, 0);
}
