//! Journey D: modern stateless MCP over Streamable HTTP, through the same core.

mod common;
use serde_json::json;

async fn rpc(base: &str, body: serde_json::Value) -> serde_json::Value {
    common::client().post(format!("{base}/mcp")).json(&body)
        .send().await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn initialize_is_the_legacy_handshake() {
    let h = common::start("mcp-init", &[]).await;
    let r = rpc(&h.base, json!({
        "jsonrpc":"2.0","id":1,"method":"initialize",
        "params":{"protocolVersion":"2026-07-28"}
    })).await;
    assert_eq!(r["result"]["protocolVersion"], antenna::ingress::mcp::LEGACY_VERSION);
    assert_eq!(r["result"]["serverInfo"]["name"], "antenna");
}

#[tokio::test]
async fn there_is_no_server_initiated_stream() {
    let h = common::start("mcp-get", &[]).await;
    let res = common::client().get(format!("{}/mcp", h.base)).send().await.unwrap();
    // Stateless means stateless: no session to resume, nothing to attach to.
    assert_eq!(res.status(), 405);
}

#[tokio::test]
async fn tools_are_bounded_actions() {
    let h = common::start("mcp-tools", &[]).await;
    let r = rpc(&h.base, json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).await;
    let names: Vec<String> = r["result"]["tools"].as_array().unwrap().iter()
        .map(|t| t["name"].as_str().unwrap().to_string()).collect();

    for expected in ["inspect_document","store_object","create_delivery",
                     "list_capabilities","query_receipts","request_analysis"] {
        assert!(names.contains(&expected.to_string()), "missing tool {expected}");
    }
    // Nothing that executes arbitrary code is exposed as a semantic tool.
    for forbidden in ["shell","exec","run_command","eval","bash"] {
        assert!(!names.iter().any(|n| n.contains(forbidden)),
            "unbounded action {forbidden:?} must never be an MCP tool");
    }
}

#[tokio::test]
async fn tools_call_creates_a_receipt_and_runs_through_the_shared_core() {
    let h = common::start("mcp-call", &[]).await;
    let r = rpc(&h.base, json!({
        "jsonrpc":"2.0","id":3,"method":"tools/call",
        "params":{"name":"list_capabilities","arguments":{}}
    })).await;

    assert_eq!(r["result"]["isError"], false);
    let receipt_id = r["result"]["_meta"]["antenna/receipt_id"].as_str().unwrap();

    let one: serde_json::Value = common::client()
        .get(format!("{}/receipts/{receipt_id}", h.base))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(one["receipt"]["transport"], "mcp");
    assert_eq!(one["receipt"]["interaction"], "call");
    // The same router, the same run machinery as HTTP and WebSocket.
    assert_eq!(one["runs"][0]["capability"], "mcp.dispatch");
    assert_eq!(one["runs"][0]["status"], "completed");
}

#[tokio::test]
async fn routing_metadata_alone_is_enough() {
    // The metadata must agree with the persisted request on recovery.
    let h = common::start("mcp-headers", &[]).await;
    let r: serde_json::Value = common::client()
        .post(format!("{}/mcp", h.base))
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "query_receipts")
        .header("MCP-Protocol-Version", "2026-07-28")
        .json(&json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"query_receipts","arguments":{"limit":5},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}))
        .send().await.unwrap().json().await.unwrap();

    assert_eq!(r["result"]["isError"], false);
    assert!(r["result"]["structuredContent"]["result"]["count"].is_number());
}

#[tokio::test]
async fn unknown_tools_are_refused_without_committing_work() {
    let h = common::start("mcp-unknown", &[]).await;
    let before: serde_json::Value = common::client()
        .get(format!("{}/receipts?limit=100", h.base)).send().await.unwrap().json().await.unwrap();

    let r = rpc(&h.base, json!({
        "jsonrpc":"2.0","id":5,"method":"tools/call",
        "params":{"name":"definitely_not_a_tool","arguments":{}}
    })).await;
    assert_eq!(r["error"]["code"], -32602);

    let after: serde_json::Value = common::client()
        .get(format!("{}/receipts?limit=100", h.base)).send().await.unwrap().json().await.unwrap();
    assert_eq!(before["count"], after["count"], "an unroutable call is not accepted work");
}

#[tokio::test]
async fn batching_is_rejected() {
    let h = common::start("mcp-batch", &[]).await;
    let res = common::client().post(format!("{}/mcp", h.base))
        .json(&json!([{"jsonrpc":"2.0","id":1,"method":"ping"}]))
        .send().await.unwrap();
    assert_eq!(res.status(), 400);
}
