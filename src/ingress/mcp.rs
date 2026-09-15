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
pub const LEGACY_VERSION: &str = "2025-11-25";
const VERSION_META: &str = "io.modelcontextprotocol/protocolVersion";

fn err(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn ok(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn complete(id: Value, mut result: Value, modern: bool) -> Value {
    if modern {
        result["resultType"] = json!("complete");
        result["_meta"]["io.modelcontextprotocol/serverInfo"] = json!({
            "name": "antenna", "version": env!("CARGO_PKG_VERSION")
        });
    }
    ok(id, result)
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
    mut headers: HeaderMap,
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
    let Some(method) = req.get("method").and_then(Value::as_str) else {
        return (StatusCode::BAD_REQUEST, Json(err(id, -32600, "method is required"))).into_response();
    };
    if req.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || req.get("params").is_some_and(|v| !v.is_object())
        || req.get("id").is_some_and(|v| !(v.is_string() || v.is_i64() || v.is_u64()))
    {
        return (StatusCode::BAD_REQUEST, Json(err(Value::Null, -32600, "invalid request"))).into_response();
    }
    // A notification cannot invoke a tool or create accepted work.
    if req.get("id").is_none() {
        return StatusCode::ACCEPTED.into_response();
    }
    if req.pointer("/params/_meta").is_some_and(|v| !v.is_object()) {
        return (StatusCode::BAD_REQUEST, Json(err(id, -32602, "invalid metadata"))).into_response();
    }
    let meta_version = req.pointer("/params/_meta").and_then(|v| v.get(VERSION_META));
    let transport_version = header(&headers, "mcp-protocol-version");
    let version = meta_version.and_then(Value::as_str).or(transport_version.as_deref());
    let modern = version == Some(PROTOCOL_VERSION);
    if meta_version.is_some_and(|v| !v.is_string())
        || version.is_some_and(|v| v != PROTOCOL_VERSION && v != LEGACY_VERSION)
    {
        return (StatusCode::BAD_REQUEST, Json(err(id, -32022, "unsupported protocol version"))).into_response();
    }
    // The July protocol rules are shared with the supplied Labor transport:
    // per-request metadata and matching method/name headers, no initialize.
    if modern && meta_version.and_then(Value::as_str) != Some(PROTOCOL_VERSION) {
        return (StatusCode::BAD_REQUEST, Json(err(id, -32602, "per-request protocol metadata required"))).into_response();
    }
    let mut expected = vec![("mcp-method", Some(method))];
    if modern { expected.push(("mcp-protocol-version", Some(PROTOCOL_VERSION))); }
    if method == "tools/call" {
        expected.push(("mcp-name", req.pointer("/params/name").and_then(Value::as_str)));
    }
    for (name, expected) in expected {
        let actual = header(&headers, name);
        if (modern || actual.is_some()) && (actual.is_none() || actual.as_deref() != expected) {
            return (StatusCode::BAD_REQUEST, Json(err(id, -32020, &format!("missing or mismatched header: {name}")))).into_response();
        }
    }
    let method = method.to_string();

    match method.as_str() {
        "initialize" if !modern => (StatusCode::OK, Json(ok(id, json!({
            "protocolVersion": LEGACY_VERSION,
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

        "server/discover" if modern => (StatusCode::OK, Json(complete(id, json!({
            "supportedVersions": [PROTOCOL_VERSION, LEGACY_VERSION],
            "capabilities": { "tools": {} },
            "ttlMs": 300000, "cacheScope": "public",
            "instructions": "Tools invoke bounded Antenna operations; calls require the operator credential when configured."
        }), true))).into_response(),

        // Notifications carry no id and get no body.
        m if m.starts_with("notifications/") => StatusCode::ACCEPTED.into_response(),

        "ping" => (StatusCode::OK, Json(complete(id, json!({}), modern))).into_response(),

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
            (StatusCode::OK, Json(complete(id, json!({ "tools": tools }), modern))).into_response()
        }

        "tools/call" => {
            if req.pointer("/params/name").and_then(Value::as_str)==Some("invoke_service") {
                let Some(contract)=req.pointer("/params/arguments/contract").and_then(Value::as_str) else {
                    return (StatusCode::BAD_REQUEST,Json(err(id,-32602,"invoke_service requires arguments.contract"))).into_response();
                };
                if let Err(error)=crate::services::admit_mcp(&app,&mut headers,contract).await {
                    return (StatusCode::FORBIDDEN,Json(err(id,-32001,&error.to_string()))).into_response();
                }
            }
            let contracted=crate::services::is_service(&headers) && req.pointer("/params/name").and_then(Value::as_str)==Some("invoke_service");
            if !contracted && !crate::ingress::inspection_allowed(&app, &headers) {
                return (StatusCode::UNAUTHORIZED, Json(err(id, -32001, "operator authentication required"))).into_response();
            }
            if req.pointer("/params/arguments").is_some_and(|v| !v.is_object()) {
                return (StatusCode::BAD_REQUEST, Json(err(id, -32602, "arguments must be an object"))).into_response();
            }
            tools_call(app, peer, headers, body, req, id, modern).await
        },

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
    modern: bool,
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
        relationships: crate::services::relationships(&headers,json!({
            "jsonrpc_id": id,
            "mcp_protocol_version": header(&headers, "mcp-protocol-version")
        })),
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
    let worker_app = app.clone();
    let rid = accepted.receipt_id.clone();
    let task = tokio::spawn(async move { journey::process(&worker_app, &rid, meta).await });
    let timeout = std::time::Duration::from_millis(app.cfg.server.call_timeout_ms);
    let answer = tokio::time::timeout(timeout, async {
        let outcome = task.await?;
        let envelope = if matches!(outcome, journey::Outcome::Completed { .. }) {
            rx.await.ok()
        } else { None };
        Ok::<_, tokio::task::JoinError>((outcome, envelope))
    }).await;
    app.returns.forget(&key);

    let (structured, is_error) = match answer {
        Ok(Ok((outcome, envelope))) => (
            envelope.map(|v| v.get("result").cloned().unwrap_or(v))
                .unwrap_or_else(|| outcome.to_json(&accepted.receipt_id)),
            matches!(outcome, journey::Outcome::Failed { .. } | journey::Outcome::Quarantined { .. }),
        ),
        _ => (json!({"status":"accepted", "receipt_id": accepted.receipt_id}), false),
    };

    let text = serde_json::to_string_pretty(&structured)
        .unwrap_or_else(|_| "{}".to_string());

    (StatusCode::OK, Json(complete(id, json!({
        "content": [ { "type": "text", "text": text } ],
        "structuredContent": structured,
        "isError": is_error,
        "_meta": {
            "antenna/receipt_id": accepted.receipt_id,
            "antenna/correlation_id": accepted.correlation_id
        }
    }), modern))).into_response()
}
