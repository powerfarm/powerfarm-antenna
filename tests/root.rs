mod common;
use serde_json::{json, Value};
use futures_util::{SinkExt, StreamExt};

#[tokio::test]
async fn july_mcp_discovery_and_execution_use_root() {
    let h = common::start("root-mcp", &[]).await;
    for (id, method, name) in [(1,"server/discover", None), (2,"tools/call", Some("list_capabilities"))] {
        let mut params = json!({"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}});
        let mut call = common::client().post(&h.base)
            .header("Mcp-Protocol-Version", "2026-07-28").header("Mcp-Method", method);
        if let Some(name) = name {
            call = call.header("Mcp-Name", name);
            params["name"] = json!(name); params["arguments"] = json!({});
        }
        let response = call.json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["result"]["resultType"], "complete", "{body}");
        if name.is_some() { assert!(body["result"]["_meta"]["antenna/receipt_id"].is_string(), "{body}"); }
    }
}

#[tokio::test]
async fn mismatched_headers_and_notifications_never_execute_tools() {
    let h = common::start("mcp-conflict", &[]).await;
    let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"store_object","arguments":{"text":"must not store"}}});
    let r = common::client().post(&h.base).header("Mcp-Name", "query_receipts").json(&body).send().await.unwrap();
    assert_eq!(r.status(), 400);
    let mut notification = body.clone(); notification.as_object_mut().unwrap().remove("id");
    let r = common::client().post(&h.base).json(&notification).send().await.unwrap();
    assert_eq!(r.status(), 202);
    let count: i64 = h.app.db.call(|c| Ok(c.query_row("SELECT count(*) FROM receipts", [], |r| r.get(0))?)).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn protected_tools_cannot_bypass_inspection_auth() {
    let dir = common::temp_dir("mcp-auth");
    let mut cfg = common::config_for(&dir, &[]); cfg.server.inspect_token = Some("operator-test-token".into());
    let h = common::start_with_config(dir, cfg).await;
    let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query_receipts","arguments":{}}});
    for path in ["", "/mcp"] {
        let r = common::client().post(format!("{}{path}", h.base)).json(&body).send().await.unwrap();
        assert_eq!(r.status(), 401);
        let r = common::client().post(format!("{}{path}", h.base)).bearer_auth("operator-test-token").json(&body).send().await.unwrap();
        assert_eq!(r.status(), 200);
    }
}

#[tokio::test]
async fn root_upload_uses_blob_limit_and_async_has_no_orphan_delivery() {
    let dir = common::temp_dir("root-blob");
    let mut cfg = common::config_for(&dir, &[]);
    cfg.server.max_event_bytes = 128; cfg.server.max_blob_bytes = 4096;
    let h = common::start_with_config(dir, cfg).await;
    let r = common::client().post(format!("{}/?wait=0",h.base))
        .header("Content-Type","application/octet-stream").body(vec![42u8;2048]).send().await.unwrap();
    assert_eq!(r.status(),202);
    let body: Value = r.json().await.unwrap(); let rid = body["receipt_id"].as_str().unwrap().to_string();
    for _ in 0..100 {
        let rid = rid.clone();
        let done: bool = h.app.db.call(move |c| Ok(c.query_row("SELECT status='routed' FROM receipts WHERE id=?1",[rid],|r|r.get(0))?)).await.unwrap();
        if done { break; }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let (status, path, deliveries): (String, Option<String>,i64) = h.app.db.call(move |c| Ok(c.query_row(
        "SELECT status,return_path,(SELECT count(*) FROM deliveries WHERE receipt_id=?1) FROM receipts WHERE id=?1",[rid],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?)).await.unwrap();
    assert_eq!(status,"routed"); assert!(path.is_none()); assert_eq!(deliveries,0);
    let r = common::client().post(&h.base).header("Content-Type","application/octet-stream").body(vec![0u8;5000]).send().await.unwrap();
    assert_eq!(r.status(),413);
}

#[tokio::test]
async fn root_websocket_executes_through_same_corridor() {
    let h = common::start("root-ws", &[]).await;
    let (mut ws,_) = tokio_tungstenite::connect_async(format!("ws://{}/",h.addr)).await.unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(json!({"id":"root-client","work":"root-ws"}).to_string().into())).await.unwrap();
    let reply = tokio::time::timeout(std::time::Duration::from_secs(5),ws.next()).await.unwrap().unwrap().unwrap();
    let body: Value = serde_json::from_str(reply.to_text().unwrap()).unwrap();
    assert_eq!(body["id"],"root-client"); assert_eq!(body["status"],"completed");
}

#[tokio::test]
async fn failed_completion_rolls_back_the_entire_outbox() {
    let dir = common::temp_dir("atomic-completion");
    let app = antenna::build(common::config_for(&dir,&[]), &common::routes_path()).unwrap();
    app.db.call(|c| {
        c.execute_batch("CREATE TRIGGER fail_completion BEFORE UPDATE OF status ON runs WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'injected completion failure'); END;")?;
        Ok(())
    }).await.unwrap();
    let accepted = antenna::journey::accept(&app, antenna::receipt::NewReceipt {
        transport:"http".into(), interaction:"call".into(), raw_storage:"sqlite".into(),
        raw_body:Some(b"atomic".to_vec()), return_path:Some("active-response:atomic".into()),
        ..Default::default()
    },antenna::ids::new_trace_id()).await.unwrap();
    let outcome = antenna::journey::process(&app,&accepted.receipt_id,Default::default()).await;
    assert!(matches!(outcome,antenna::journey::Outcome::Failed{..}));
    let counts: (i64,i64) = app.db.call(|c| Ok((
        c.query_row("SELECT count(*) FROM deliveries",[],|r|r.get(0))?,
        c.query_row("SELECT count(*) FROM runs WHERE status='completed'",[],|r|r.get(0))?
    ))).await.unwrap();
    assert_eq!(counts,(0,0),"no delivery survives a failed completion transaction");
}
