//! MCP ingress: journey D. Modern, stateless Streamable HTTP (spec §16).
//!
//! Stateless by construction: there is no session map, no hidden server state,
//! no resumable stream. Everything that must survive is a Receipt, a Run or a
//! Delivery in SQLite — never a session object in RAM.
//!
//! Routing metadata is exploited before the body is parsed: `Mcp-Method` and
//! `Mcp-Name` headers reach the router as-is, so an MCP tool call is routed
//! deterministically without a model and without reading the payload.

use crate::app::App;
use crate::ingress::{header, principal_of, source_of, trace_id_from};
use crate::journey;
use crate::receipt::NewReceipt;
use crate::router::RouteMeta;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::net::SocketAddr;

pub const PROTOCOL_VERSION: &str = "2026-07-28";

fn err(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn ok(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// GET /mcp — stateless: there is no server-initiated stream to attach to.
pub async fn get_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
            "error": "this MCP endpoint is stateless; no server-initiated SSE stream is offered",
            "protocolVersion": PROTOCOL_VERSION
        })),
    )
}

#[tracing::instrument(skip_all, fields(transport = "mcp"))]
pub async fn post(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let req: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST,
                Json(err(Value::Null, -32700, &format!("parse error: {e}")))).into_response()
        }
    };

    // Batches were removed from MCP; one JSON-RPC message per POST.
    if req.is_array() {
        return (StatusCode::BAD_REQUEST,
            Json(err(Value::Null, -32600, "JSON-RPC batching is not supported"))).into_response();
    }

    let id = req.get("id").cloned().unwrap_or(Value::Null);
    // Header first, body second — transport metadata is cheaper and is
    // exactly what the router wants.
    let method = header(&headers, "mcp-method")
        .or_else(|| req.get("method").and_then(|m| m.as_str()).map(String::from))
        .unwrap_or_default();

    match method.as_str() {
        "initialize" => (StatusCode::OK, Json(ok(id, json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": {
                "name": "antenna",
                "title": "Antenna",
                "version": env!("CARGO_PKG_VERSION")
            },
            "instructions": "Antenna is an ingress/egress membrane. Tools are bounded actions; \
none of them execute arbitrary code. Payloads you pass in are recorded as received, \
not asserted as true."
        })))).into_response(),

        // Notifications carry no id and get no body.
        m if m.starts_with("notifications/") => StatusCode::ACCEPTED.into_response(),

        "ping" => (StatusCode::OK, Json(ok(id, json!({})))).into_response(),

        "tools/list" => {
            let tools: Vec<Value> = crate::capability::builtins::MCP_TOOLS
                .iter()
                .filter_map(|(tool, cap_name)| {
                    let cap = app.caps.get(cap_name)?;
                    Some(json!({
                        "name": tool,
                        "description": cap.description(),
                        "inputSchema": cap.input_schema()
                            .unwrap_or_else(|| json!({ "type": "object", "properties": {} }))
                    }))
                })
                .collect();
            (StatusCode::OK, Json(ok(id, json!({ "tools": tools })))).into_response()
        }

        "tools/call" => tools_call(app, peer, headers, body, req, id).await,

        other => (StatusCode::OK,
            Json(err(id, -32601, &format!("method not found: {other}")))).into_response(),
    }
}

async fn tools_call(
    app: App,
    peer: SocketAddr,
    headers: HeaderMap,
    body: Bytes,
    req: Value,
    id: Value,
) -> axum::response::Response {
    let tool = header(&headers, "mcp-name")
        .or_else(|| req.pointer("/params/name").and_then(|v| v.as_str()).map(String::from));

    let tool_name = match &tool {
        Some(t) => t.clone(),
        None => {
            return (StatusCode::OK,
                Json(err(id, -32602, "tools/call requires params.name"))).into_response()
        }
    };

    // Refuse unknown tools before committing anything: an unroutable call is
    // not work Antenna accepted.
    if crate::capability::builtins::mcp_tool_map(&tool_name).is_none() {
        return (StatusCode::OK,
            Json(err(id, -32602, &format!("unknown tool: {tool_name}")))).into_response();
    }

    let correlation_id = crate::ids::new_id("cor");
    let trace_id = trace_id_from(&headers);

    let n = NewReceipt {
        transport: "mcp".into(),
        source: Some(source_of(&headers, Some(peer))),
        principal: principal_of(&headers),
        provenance: header(&headers, "user-agent"),
        content_type: Some("application/json".into()),
        content_length: Some(body.len() as i64),
        raw_storage: "sqlite".into(),
        raw_digest: Some(blake3::hash(&body).to_hex().to_string()),
        raw_body: Some(body.to_vec()),
        interaction: "call".into(),
        correlation_id: Some(correlation_id.clone()),
        return_path: Some(format!("mcp:{correlation_id}")),
        route_hint: Some(tool_name.clone()),
        relationships: Some(json!({
            "jsonrpc_id": id,
            "mcp_protocol_version": header(&headers, "mcp-protocol-version")
        }).to_string()),
        ..Default::default()
    };

    let accepted = match journey::accept(&app, n, trace_id).await {
        Ok(a) => a,
        Err(e) => {
            return (StatusCode::OK,
                Json(err(id, -32603, &format!("could not commit receipt: {e}")))).into_response()
        }
    };

    // Same corridor as every other ingress: register the return path, let the
    // router decide, let the outbox correlate the answer back.
    let key = format!("mcp:{}", accepted.correlation_id);
    let rx = app.returns.register_once(&key);

    let meta = RouteMeta { method: Some("tools/call".into()), tool: Some(tool_name.clone()) };
    let outcome = journey::process(&app, &accepted.receipt_id, meta).await;

    let timeout = std::time::Duration::from_millis(app.cfg.server.call_timeout_ms);
    let answer = tokio::time::timeout(timeout, rx).await;
    app.returns.forget(&key);

    let structured = match answer {
        Ok(Ok(envelope)) => envelope.get("result").cloned().unwrap_or(envelope),
        _ => outcome.to_json(&accepted.receipt_id),
    };

    let is_error = matches!(
        outcome,
        journey::Outcome::Failed { .. } | journey::Outcome::Quarantined { .. }
    );

    let text = serde_json::to_string_pretty(&structured)
        .unwrap_or_else(|_| "{}".to_string());

    (StatusCode::OK, Json(ok(id, json!({
        "content": [ { "type": "text", "text": text } ],
        "structuredContent": structured,
        "isError": is_error,
        "_meta": {
            "antenna/receipt_id": accepted.receipt_id,
            "antenna/correlation_id": accepted.correlation_id
        }
    })))).into_response()
}
