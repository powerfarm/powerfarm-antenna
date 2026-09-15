//! WebSocket ingress: journey C.
//!
//! The connection is EXECUTION state — ephemeral, not durable. What is durable
//! is the receipt for each meaningful message, and the delivery that answers
//! it. Kill the process and the socket dies; the receipts do not.

use crate::app::App;
use crate::ingress::{principal_of, source_of, trace_id_from};
use crate::journey;
use crate::receipt::NewReceipt;
use crate::router::RouteMeta;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub async fn upgrade(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| serve(app, socket, peer, headers))
}

async fn serve(app: App, socket: WebSocket, peer: SocketAddr, headers: HeaderMap) {
    let conn_id = crate::ids::new_id("ws");
    let return_path = format!("websocket:{conn_id}");
    let source = source_of(&headers, Some(peer));
    let principal = principal_of(&headers);

    tracing::info!(connection = %conn_id, source = %source, "websocket open");

    // The registry entry IS the return path. When this connection dies the
    // entry goes, and any delivery aimed at it becomes honestly orphaned
    // rather than silently retried into nothing.
    let mut outbound = app.returns.register_many(&return_path);
    let (mut sink, mut stream) = socket.split();

    // Correlation ids are Antenna's. Clients may also carry their own `id`,
    // and they expect to see it back — so we remember the mapping for the
    // life of the connection only.
    let client_ids: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));

    let ids_for_writer = client_ids.clone();
    let writer = tokio::spawn(async move {
        while let Some(mut envelope) = outbound.recv().await {
            if let Some(obj) = envelope.as_object_mut() {
                let cor = obj.get("correlation_id").and_then(|v| v.as_str()).map(String::from);
                if let Some(cor) = cor {
                    if let Some(cid) = ids_for_writer.lock().unwrap().remove(&cor) {
                        obj.insert("id".into(), cid);
                    }
                }
            }
            let text = serde_json::to_string(&envelope).unwrap_or_else(|_| "{}".into());
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(connection = %conn_id, error = %e, "websocket read error");
                break;
            }
        };

        let (body, content_type) = match msg {
            Message::Text(t) => (t.as_bytes().to_vec(), "application/json"),
            Message::Binary(b) => (b.to_vec(), "application/octet-stream"),
            // Ping/pong and close are transport mechanics, not meaningful
            // events. They get no receipt: Antenna records signals, not noise.
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => break,
        };

        let correlation_id = crate::ids::new_id("cor");
        if let Err(error)=crate::services::check_frame(&app,&headers,body.len()).await {
            tracing::warn!(error=%error,"websocket client contract no longer permits input");
            break;
        }
        if let Ok(v) = serde_json::from_slice::<Value>(&body) {
            if let Some(cid) = v.get("id") {
                client_ids.lock().unwrap().insert(correlation_id.clone(), cid.clone());
            }
        }

        let n = NewReceipt {
            transport: "websocket".into(),
            source: Some(source.clone()),
            principal: principal.clone(),
            content_type: Some(content_type.into()),
            content_length: Some(body.len() as i64),
            raw_storage: "sqlite".into(),
            raw_digest: Some(blake3::hash(&body).to_hex().to_string()),
            raw_body: Some(body),
            interaction: "stream".into(),
            correlation_id: Some(correlation_id.clone()),
            return_path: Some(return_path.clone()),
            relationships: crate::services::relationships(&headers,json!({ "connection": conn_id })),
            ..Default::default()
        };

        let trace_id = trace_id_from(&headers);
        match journey::accept(&app, n, trace_id).await {
            Err(e) => tracing::error!(error = %e, "could not commit websocket receipt"),
            Ok(accepted) => {
                // Do not block the read loop: one slow capability must not
                // stall the connection's other messages.
                let app2 = app.clone();
                tokio::spawn(async move {
                    let outcome =
                        journey::process(&app2, &accepted.receipt_id, RouteMeta::default()).await;
                    tracing::debug!(
                        receipt = %accepted.receipt_id, outcome = outcome.status(),
                        "websocket message settled"
                    );
                });
            }
        }
    }

    app.returns.forget(&return_path);
    writer.abort();
    tracing::info!(connection = %conn_id, "websocket closed");
}
