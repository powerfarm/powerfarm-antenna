mod common;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

async fn configured(tag: &str) -> common::Harness {
    let dir = common::temp_dir(tag);
    let mut cfg = common::config_for(&dir, &[]);
    cfg.services.enabled = true;
    cfg.services.python = Some(
        std::env::var("ANTENNA_TEST_PYTHON")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("runtime/.venv/bin/python")
            }),
    );
    cfg.services.worker_path =
        Some(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("runtime/run_graph.py"));
    let h = common::start_with_config(dir, cfg).await;
    install(&h, false).await;
    h
}
async fn install(h: &common::Harness, revoked: bool) {
    install_transport(h, revoked, "http").await;
}
async fn install_transport(h: &common::Harness, revoked: bool, transport: &str) {
    let now = chrono::Utc::now();
    let b = json!({"contract_id":"client-1","name":"document-client","terms_sha256":"terms-1","service_id":"service-1","service_sha256":"service-terms-1",
        "client_id":"app-1","terms":{"max_bytes":4096,"destinations":[]},"definition_sha256":"graph-1","valid_until":now+chrono::Duration::hours(1),
        "credentials":[{"sha256":hex::encode(Sha256::digest(b"client-token")),"valid_until":null}],
        "definition":{"schema":"antenna.service.v1","title":"Document service","transport":transport,"capabilities":["object.store","document.inspect"],
            "graph":{"start":"store","nodes":{"store":"object.store","inspect":"document.inspect"},"edges":[["store","inspect"],["inspect","END"]],
            "parameters":{"inspect":{"digest":{"from":"result.digest"}}}}}});
    let value = json!({"schema":"antenna.snapshot.v1","audience":"antenna","issued_at":now,"expires_at":now+chrono::Duration::seconds(60),"bindings":if revoked {vec![]}else{vec![b]}});
    let payload = value.to_string();
    let mut mac = Hmac::<Sha256>::new_from_slice(b"daemon-token").unwrap();
    mac.update(payload.as_bytes());
    h.app
        .services
        .install(
            &payload,
            &hex::encode(mac.finalize().into_bytes()),
            "daemon-token",
            "antenna",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn mcp_invokes_and_inspects_a_contract_with_the_client_credential() {
    let h = configured("contract-mcp").await;
    let call = |args: Value| {
        common::client().post(&h.base).bearer_auth("client-token")
        .header("Mcp-Protocol-Version","2026-07-28").header("Mcp-Method","tools/call").header("Mcp-Name","invoke_service")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"invoke_service","arguments":args,
            "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}))
    };
    let value: Value = call(json!({"contract":"document-client","input":{"content":"MCP work"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(value["result"]["isError"], false, "{value}");
    let rid = value["result"]["_meta"]["antenna/receipt_id"]
        .as_str()
        .unwrap();
    let inspected: Value = call(json!({"contract":"document-client","receipt_id":rid}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        inspected["result"]["structuredContent"]["result"]["receipt_id"], rid,
        "{inspected}"
    );
    assert_eq!(
        inspected["result"]["structuredContent"]["result"]["status"], "routed",
        "{inspected}"
    );
}

#[tokio::test]
async fn streaming_service_delivers_the_graph_result_as_sse() {
    let h = configured("contract-sse").await;
    install_transport(&h, false, "sse").await;
    let response = common::client()
        .post(&h.base)
        .bearer_auth("client-token")
        .header("Antenna-Contract", "document-client")
        .header("Accept", "text/event-stream")
        .json(&json!({"content":"streamed work"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let body = response.text().await.unwrap();
    assert!(body.contains("event: result"), "{body}");
    assert!(body.contains("service.invoke"), "{body}");
}

#[tokio::test]
async fn websocket_contract_executes_and_revocation_closes_the_next_frame() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let h = configured("contract-ws").await;
    install_transport(&h, false, "websocket").await;
    let mut req = format!("ws://{}/", h.addr).into_client_request().unwrap();
    req.headers_mut()
        .insert("Antenna-Contract", "document-client".parse().unwrap());
    req.headers_mut()
        .insert("Authorization", "Bearer client-token".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(Message::Text(
        json!({"id":"ws-client","content":"websocket work"})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let message = tokio::time::timeout(std::time::Duration::from_secs(20), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let reply: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(reply["result"]["capability"], "service.invoke", "{reply}");
    install_transport(&h, true, "websocket").await;
    ws.send(Message::Text(
        json!({"content":"revoked"}).to_string().into(),
    ))
    .await
    .unwrap();
    let reply = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
        .await
        .unwrap();
    assert!(
        !matches!(reply, Some(Ok(Message::Text(_)))),
        "revoked frame must not execute"
    );
}

#[tokio::test]
async fn accepted_contract_executes_the_supplied_continuity_graph() {
    let h = configured("contract-graph").await;
    let response = common::client()
        .post(&h.base)
        .bearer_auth("client-token")
        .header("Antenna-Contract", "document-client")
        .json(&json!({"content":"the service did actual work"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["result"]["capability"], "service.invoke", "{body}");
    assert_eq!(
        body["result"]["outputs"]["store"]["size_bytes"], 27,
        "{body}"
    );
    assert!(
        body["result"]["outputs"]["inspect"]["observation"].is_object(),
        "{body}"
    );
    assert!(h.dir.join("continuity.db").exists());
    let rid = body["receipt_id"].as_str().unwrap().to_string();
    let r = h
        .app
        .db
        .call(move |c| antenna::receipt::load(c, &rid))
        .await
        .unwrap()
        .unwrap();
    assert!(antenna::services::context(&r).unwrap().is_some());
    install(&h, true).await;
    let denied = common::client()
        .post(&h.base)
        .bearer_auth("client-token")
        .header("Antenna-Contract", "document-client")
        .json(&json!({"content":"revoked"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
}

#[tokio::test]
async fn forged_proof_and_wrong_credentials_cannot_invoke_contracts() {
    let h = configured("contract-auth").await;
    let bad = common::client()
        .post(&h.base)
        .bearer_auth("wrong")
        .header("Antenna-Contract", "document-client")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 403);
    let response: Value = common::client()
        .post(&h.base)
        .header(
            "Antenna-Verified-Service",
            json!({"contract_id":"client-1"}).to_string(),
        )
        .json(&json!({"content":"ordinary ingress"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["result"]["capability"], "echo");
    let bad = common::client()
        .post(&h.base)
        .bearer_auth("client-token")
        .header("Antenna-Contract", "document-client")
        .body(vec![0u8; 5000])
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 413);
}
