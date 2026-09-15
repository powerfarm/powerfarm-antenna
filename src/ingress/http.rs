//! HTTP ingress: journeys A (event/call) and B (blob).

use crate::app::App;
use crate::ingress::{header, inspection_allowed, principal_of, source_of, trace_id_from};
use crate::journey;
use crate::receipt::NewReceipt;
use crate::router::RouteMeta;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::Json;
use futures_util::StreamExt;
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Deserialize)]
pub struct IngressQuery {
    /// `wait=0` turns a CALL into a fire-and-forget EVENT.
    #[serde(default)]
    pub wait: Option<u8>,
}

/// POST /ingress — small signals. Body is held in memory, so it is capped.
#[tracing::instrument(skip_all, fields(transport = "http", interaction = "call"))]
pub async fn ingress(
    State(app): State<App>,
    Query(q): Query<IngressQuery>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let wants_answer = q.wait != Some(0);
    let trace_id = trace_id_from(&headers);
    let correlation_id = header(&headers, "antenna-correlation-id")
        .unwrap_or_else(|| crate::ids::new_id("cor"));

    let digest = blake3::hash(&body).to_hex().to_string();

    let n = NewReceipt {
        transport: "http".into(),
        source: Some(source_of(&headers, Some(peer))),
        principal: principal_of(&headers),
        provenance: header(&headers, "user-agent"),
        content_type: header(&headers, "content-type"),
        content_length: Some(body.len() as i64),
        raw_storage: "sqlite".into(),
        raw_body: Some(body.to_vec()),
        raw_digest: Some(digest.clone()),
        interaction: if wants_answer { "call".into() } else { "event".into() },
        correlation_id: Some(correlation_id.clone()),
        return_path: wants_answer.then(|| format!("active-response:{correlation_id}")),
        route_hint: header(&headers, "antenna-route-hint"),
        ..Default::default()
    };

    // Commit before acknowledging. Everything after this point may crash
    // without losing the fact that we received these bytes.
    let accepted = match journey::accept(&app, n, trace_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "could not commit receipt");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not commit receipt", "detail": e.to_string() })),
            )
                .into_response();
        }
    };

    if !wants_answer {
        let app2 = app.clone();
        let rid = accepted.receipt_id.clone();
        tokio::spawn(async move { journey::process(&app2, &rid, RouteMeta::default()).await; });
        return (
            StatusCode::ACCEPTED,
            Json(json!({
                "receipt_id": accepted.receipt_id,
                "correlation_id": accepted.correlation_id,
                "digest": digest,
                "status": "accepted"
            })),
        )
            .into_response();
    }

    // CALL: hold the response open and let the outbox correlate the answer
    // back to it, exactly as it would for any other return path.
    let key = format!("active-response:{}", accepted.correlation_id);
    let rx = app.returns.register_once(&key);

    let outcome = journey::process(&app, &accepted.receipt_id, RouteMeta::default()).await;

    let timeout = std::time::Duration::from_millis(app.cfg.server.call_timeout_ms);
    let answer = tokio::time::timeout(timeout, rx).await;
    app.returns.forget(&key);

    match answer {
        // The envelope already carries receipt_id, correlation_id and status;
        // we only add what the transport itself knows.
        Ok(Ok(mut envelope)) => {
            if let Some(o) = envelope.as_object_mut() {
                o.insert("digest".into(), json!(digest));
            }
            (StatusCode::OK, Json(envelope)).into_response()
        }
        // No answer came back through the outbox. Report the journey's own
        // verdict rather than pretending, and keep the receipt id so the
        // caller can go and look.
        _ => {
            let code = match outcome {
                journey::Outcome::Failed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                journey::Outcome::Quarantined { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                _ => StatusCode::ACCEPTED,
            };
            (code, Json(outcome.to_json(&accepted.receipt_id))).into_response()
        }
    }
}

/// POST /blob — files. Streamed to the bucket; never held whole in RAM.
#[tracing::instrument(skip_all, fields(transport = "http", interaction = "blob"))]
pub async fn blob(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: Request,
) -> impl IntoResponse {
    let trace_id = trace_id_from(&headers);
    let correlation_id = header(&headers, "antenna-correlation-id")
        .unwrap_or_else(|| crate::ids::new_id("cor"));

    let staging_id = crate::ids::new_id("stg");
    let staged = app.bucket.staging_path(&staging_id);

    let mut file = match tokio::fs::File::create(&staged).await {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not open staging file", "detail": e.to_string() })))
                .into_response()
        }
    };

    let mut hasher = blake3::Hasher::new();
    let mut written: u64 = 0;
    let max = app.cfg.server.max_blob_bytes;
    let mut stream = req.into_body().into_data_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                let _ = tokio::fs::remove_file(&staged).await;
                return (StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "upload interrupted", "detail": e.to_string() })))
                    .into_response();
            }
        };
        written += chunk.len() as u64;
        if written > max {
            // Bound before preserve. We refuse rather than fill the disk.
            let _ = tokio::fs::remove_file(&staged).await;
            return (StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({ "error": "blob exceeds max_blob_bytes", "max_blob_bytes": max })))
                .into_response();
        }
        hasher.update(&chunk);
        if let Err(e) = file.write_all(&chunk).await {
            let _ = tokio::fs::remove_file(&staged).await;
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not write staged blob", "detail": e.to_string() })))
                .into_response();
        }
    }

    if let Err(e) = file.flush().await.and(file.sync_all().await) {
        let _ = tokio::fs::remove_file(&staged).await;
        return (StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "could not flush staged blob", "detail": e.to_string() })))
            .into_response();
    }
    drop(file);

    let digest = hasher.finalize().to_hex().to_string();
    if let Err(e) = app.bucket.commit_staged(&staged, &digest).await {
        return (StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "could not commit object", "detail": e.to_string() })))
            .into_response();
    }

    let content_type = header(&headers, "content-type");
    {
        let (d, ct, sz) = (digest.clone(), content_type.clone(), written as i64);
        if let Err(e) = app.db.call(move |c| {
            c.execute(
                "INSERT OR IGNORE INTO objects (digest, path, size, content_type, created_at)
                 VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![
                    d, crate::storage::objects::object_path(&d).to_string(),
                    sz, ct, crate::ids::now_rfc3339()
                ],
            )?;
            Ok(())
        }).await {
            tracing::error!(error = %e, "object written but not indexed");
        }
    }

    // SQLite stores the reference and digest; the bytes stay in the bucket.
    let n = NewReceipt {
        transport: "http".into(),
        source: Some(source_of(&headers, Some(peer))),
        principal: principal_of(&headers),
        provenance: header(&headers, "user-agent"),
        content_type,
        content_length: Some(written as i64),
        raw_storage: "object".into(),
        raw_ref: Some(digest.clone()),
        raw_digest: Some(digest.clone()),
        interaction: "blob".into(),
        correlation_id: Some(correlation_id.clone()),
        return_path: Some(format!("active-response:{correlation_id}")),
        route_hint: header(&headers, "antenna-route-hint"),
        ..Default::default()
    };

    let accepted = match journey::accept(&app, n, trace_id).await {
        Ok(a) => a,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not commit receipt", "detail": e.to_string() })))
                .into_response()
        }
    };

    let key = format!("active-response:{}", accepted.correlation_id);
    let rx = app.returns.register_once(&key);
    let outcome = journey::process(&app, &accepted.receipt_id, RouteMeta::default()).await;
    let timeout = std::time::Duration::from_millis(app.cfg.server.call_timeout_ms);
    let answer = tokio::time::timeout(timeout, rx).await;
    app.returns.forget(&key);

    match answer {
        Ok(Ok(mut envelope)) => {
            if let Some(o) = envelope.as_object_mut() {
                o.insert("digest".into(), json!(digest));
                o.insert("size_bytes".into(), json!(written));
                o.insert("object_ref".into(),
                    json!(crate::storage::objects::object_path(&digest).to_string()));
            }
            (StatusCode::CREATED, Json(envelope)).into_response()
        }
        _ => (StatusCode::ACCEPTED, Json(outcome.to_json(&accepted.receipt_id))).into_response(),
    }
}

// --------------------------------------------------------- universal submit ----
/// POST / — one URL for everything. Antenna sorts it out.
///   - application/json, text/*  → ingress (receipt with raw_body)
///   - everything else           → blob   (streamed to bucket)
#[tracing::instrument(skip_all, fields(transport = "http"))]
pub async fn submit(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Query(q): Query<IngressQuery>,
    headers: HeaderMap,
    req: Request,
) -> impl IntoResponse {
    let content_type = header(&headers, "content-type").unwrap_or_default().to_lowercase();
    // Explicit MCP transport metadata takes precedence over payload media type.
    if headers.contains_key("mcp-method") || headers.contains_key("mcp-protocol-version") {
        let body = match axum::body::to_bytes(req.into_body(), app.cfg.server.max_event_bytes).await {
            Ok(body) => body,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
        return super::mcp::post(State(app), ConnectInfo(peer), headers, body).await.into_response();
    }
    let wants_answer = q.wait != Some(0);
    let trace_id = trace_id_from(&headers);
    let correlation_id = header(&headers, "antenna-correlation-id")
        .unwrap_or_else(|| crate::ids::new_id("cor"));

    // Is this a file? Anything that is not JSON or text streams to the bucket.
    let is_file = !content_type.starts_with("application/json")
        && !content_type.starts_with("text/");

    if is_file {
        // ---- blob path (streamed, capped at max_blob_bytes) ----
        let staging_id = crate::ids::new_id("stg");
        let staged = app.bucket.staging_path(&staging_id);

        let mut file = match tokio::fs::File::create(&staged).await {
            Ok(f) => f,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "could not open staging file", "detail": e.to_string() })))
                    .into_response()
            }
        };

        let mut hasher = blake3::Hasher::new();
        let mut written: u64 = 0;
        let max = app.cfg.server.max_blob_bytes;
        let mut stream = req.into_body().into_data_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    let _ = tokio::fs::remove_file(&staged).await;
                    return (StatusCode::BAD_REQUEST,
                        Json(json!({ "error": "upload interrupted", "detail": e.to_string() })))
                        .into_response();
                }
            };
            written += chunk.len() as u64;
            if written > max {
                let _ = tokio::fs::remove_file(&staged).await;
                return (StatusCode::PAYLOAD_TOO_LARGE,
                    Json(json!({ "error": "blob exceeds max_blob_bytes", "max_blob_bytes": max })))
                    .into_response();
            }
            hasher.update(&chunk);
            if let Err(e) = file.write_all(&chunk).await {
                let _ = tokio::fs::remove_file(&staged).await;
                return (StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "could not write staged blob", "detail": e.to_string() })))
                    .into_response();
            }
        }

        if let Err(e) = file.flush().await.and(file.sync_all().await) {
            let _ = tokio::fs::remove_file(&staged).await;
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not flush staged blob", "detail": e.to_string() })))
                .into_response();
        }
        drop(file);

        let digest = hasher.finalize().to_hex().to_string();
        if let Err(e) = app.bucket.commit_staged(&staged, &digest).await {
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not commit object", "detail": e.to_string() })))
                .into_response();
        }

        let ct = header(&headers, "content-type");
        {
            let (d, ct, sz) = (digest.clone(), ct.clone(), written as i64);
            if let Err(e) = app.db.call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO objects (digest, path, size, content_type, created_at)
                     VALUES (?1,?2,?3,?4,?5)",
                    rusqlite::params![
                        d, crate::storage::objects::object_path(&d).to_string(),
                        sz, ct, crate::ids::now_rfc3339()
                    ],
                )?;
                Ok(())
            }).await {
                tracing::error!(error = %e, "object written but not indexed");
            }
        }

        let n = NewReceipt {
            transport: "http".into(),
            source: Some(source_of(&headers, Some(peer))),
            principal: principal_of(&headers),
            provenance: header(&headers, "user-agent"),
            content_type: ct,
            content_length: Some(written as i64),
            raw_storage: "object".into(),
            raw_ref: Some(digest.clone()),
            raw_digest: Some(digest.clone()),
            interaction: "blob".into(),
            correlation_id: Some(correlation_id.clone()),
            return_path: wants_answer.then(|| format!("active-response:{correlation_id}")),
            route_hint: header(&headers, "antenna-route-hint"),
            ..Default::default()
        };

        let accepted = match journey::accept(&app, n, trace_id).await {
            Ok(a) => a,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "could not commit receipt", "detail": e.to_string() })))
                    .into_response()
            }
        };

        if !wants_answer {
            let app2 = app.clone();
            let rid = accepted.receipt_id.clone();
            tokio::spawn(async move { journey::process(&app2, &rid, RouteMeta::default()).await; });
            return (StatusCode::ACCEPTED, Json(json!({
                "receipt_id": accepted.receipt_id,
                "correlation_id": accepted.correlation_id,
                "digest": digest,
                "size_bytes": written,
                "object_ref": crate::storage::objects::object_path(&digest).to_string(),
                "status": "accepted"
            }))).into_response();
        }

        let key = format!("active-response:{}", accepted.correlation_id);
        let rx = app.returns.register_once(&key);
        let outcome = journey::process(&app, &accepted.receipt_id, RouteMeta::default()).await;
        let timeout = std::time::Duration::from_millis(app.cfg.server.call_timeout_ms);
        let answer = tokio::time::timeout(timeout, rx).await;
        app.returns.forget(&key);

        match answer {
            Ok(Ok(mut envelope)) => {
                if let Some(o) = envelope.as_object_mut() {
                    o.insert("digest".into(), json!(digest));
                    o.insert("size_bytes".into(), json!(written));
                    o.insert("object_ref".into(),
                        json!(crate::storage::objects::object_path(&digest).to_string()));
                }
                (StatusCode::CREATED, Json(envelope)).into_response()
            }
            _ => (StatusCode::ACCEPTED, Json(outcome.to_json(&accepted.receipt_id))).into_response(),
        }
    } else {
        // ---- ingress path (JSON / text, buffered, capped at max_event_bytes) ----
        let body = match axum::body::to_bytes(req.into_body(), app.cfg.server.max_event_bytes as usize).await {
            Ok(b) => b,
            Err(e) => {
                return (StatusCode::PAYLOAD_TOO_LARGE,
                    Json(json!({ "error": "body too large for ingress", "detail": e.to_string() })))
                    .into_response();
            }
        };

        // Preserve compatibility with clients that send a JSON-RPC envelope
        // without MCP routing headers. Ordinary JSON remains ordinary ingress.
        if serde_json::from_slice::<serde_json::Value>(&body).ok()
            .is_some_and(|v| v.get("jsonrpc").is_some())
        {
            return super::mcp::post(State(app), ConnectInfo(peer), headers, body).await.into_response();
        }
        let digest = blake3::hash(&body).to_hex().to_string();

        let n = NewReceipt {
            transport: "http".into(),
            source: Some(source_of(&headers, Some(peer))),
            principal: principal_of(&headers),
            provenance: header(&headers, "user-agent"),
            content_type: header(&headers, "content-type"),
            content_length: Some(body.len() as i64),
            raw_storage: "sqlite".into(),
            raw_body: Some(body.to_vec()),
            raw_digest: Some(digest.clone()),
            interaction: if wants_answer { "call".into() } else { "event".into() },
            correlation_id: Some(correlation_id.clone()),
            return_path: wants_answer.then(|| format!("active-response:{correlation_id}")),
            route_hint: header(&headers, "antenna-route-hint"),
            ..Default::default()
        };

        let accepted = match journey::accept(&app, n, trace_id).await {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(error = %e, "could not commit receipt");
                return (StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "could not commit receipt", "detail": e.to_string() })))
                    .into_response();
            }
        };

        if !wants_answer {
            let app2 = app.clone();
            let rid = accepted.receipt_id.clone();
            tokio::spawn(async move { journey::process(&app2, &rid, RouteMeta::default()).await; });
            return (StatusCode::ACCEPTED, Json(json!({
                "receipt_id": accepted.receipt_id,
                "correlation_id": accepted.correlation_id,
                "digest": digest,
                "status": "accepted"
            }))).into_response();
        }

        let key = format!("active-response:{}", accepted.correlation_id);
        let rx = app.returns.register_once(&key);
        let outcome = journey::process(&app, &accepted.receipt_id, RouteMeta::default()).await;
        let timeout = std::time::Duration::from_millis(app.cfg.server.call_timeout_ms);
        let answer = tokio::time::timeout(timeout, rx).await;
        app.returns.forget(&key);

        match answer {
            Ok(Ok(mut envelope)) => {
                if let Some(o) = envelope.as_object_mut() {
                    o.insert("digest".into(), json!(digest));
                }
                (StatusCode::OK, Json(envelope)).into_response()
            }
            _ => {
                let code = match outcome {
                    journey::Outcome::Failed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                    journey::Outcome::Quarantined { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                    _ => StatusCode::ACCEPTED,
                };
                (code, Json(outcome.to_json(&accepted.receipt_id))).into_response()
            }
        }
    }
}

// --------------------------------------------------------- landing ----
pub async fn root_get(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: Result<axum::extract::ws::WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> axum::response::Response {
    if headers.contains_key("upgrade") {
        return match ws {
            Ok(ws) => super::websocket::upgrade(State(app), ConnectInfo(peer), headers, ws).await.into_response(),
            Err(e) => e.into_response(),
        };
    }
    if headers.contains_key("mcp-protocol-version") {
        return super::mcp::get_not_allowed().await.into_response();
    }
    landing().await.into_response()
}

/// Human front door. What Antenna is and how to send to it.
pub async fn landing() -> impl IntoResponse {
    let html = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Antenna — LAB 8GB</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; max-width: 720px; margin: 3rem auto; padding: 0 1.5rem; line-height: 1.6; color: #222; }
  h1 { font-size: 1.8rem; margin-bottom: 0.2rem; }
  .tagline { color: #666; font-size: 0.95rem; margin-bottom: 2rem; }
  h2 { font-size: 1.2rem; margin-top: 2rem; border-bottom: 1px solid #ddd; padding-bottom: 0.3rem; }
  code { background: #f4f4f4; padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.9rem; }
  pre { background: #f4f4f4; padding: 1rem; border-radius: 6px; overflow-x: auto; font-size: 0.85rem; }
  .endpoint { background: #eef; padding: 0.8rem 1rem; border-radius: 6px; margin: 0.5rem 0; }
  .method { font-weight: bold; color: #2a5; }
  .note { color: #666; font-size: 0.9rem; margin-top: 0.5rem; }
  a { color: #36c; }
  .send-box { background: #f8f8f8; padding: 1rem; border-radius: 8px; border: 1px solid #ddd; margin: 1rem 0; }
  .send-box textarea { width: 100%; min-height: 80px; font-family: system-ui, sans-serif; font-size: 1rem; padding: 0.5rem; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; resize: vertical; }
  .send-box input[type="file"] { font-size: 0.95rem; margin: 0.5rem 0; }
  .send-box button { background: #2a5; color: #fff; border: none; padding: 0.5rem 1.2rem; border-radius: 4px; font-size: 1rem; cursor: pointer; margin-top: 0.5rem; }
  .send-box button:disabled { background: #999; }
  .result { margin-top: 0.8rem; padding: 0.6rem; border-radius: 4px; font-size: 0.9rem; }
  .result.ok { background: #dfd; }
  .result.err { background: #fdd; }
  .result pre { margin: 0.3rem 0 0 0; background: transparent; padding: 0; }
</style>
</head>
<body>
<h1>🔌 Antenna</h1>
<p class="tagline">A thin, durable ingress membrane — <em>receive faithfully · preserve durably · route cheaply</em></p>

<p>Antenna accepts signals from anywhere, commits them to SQLite before acknowledging,
and routes them deterministically. No model is required to boot.</p>

<h2>Send now</h2>

<div class="send-box">
  <strong>Message</strong>
  <textarea id="msg" placeholder="Type something..."></textarea><br>
  <button onclick="sendMsg()" id="msgBtn">Send message</button>
  <div id="msgRes" class="result" style="display:none;"></div>
</div>

<div class="send-box">
  <strong>File</strong><br>
  <input type="file" id="fileIn" onchange="sendFile()"><br>
  <div id="fileRes" class="result" style="display:none;"></div>
</div>

<script>
async function sendMsg() {
  const ta = document.getElementById('msg');
  const btn = document.getElementById('msgBtn');
  const res = document.getElementById('msgRes');
  const body = ta.value.trim();
  if (!body) { res.className='result err'; res.textContent='Empty message'; res.style.display='block'; return; }
  btn.disabled = true;
  try {
    const r = await fetch('/?wait=0', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'antenna-correlation-id': 'web-' + Date.now()
      },
      body: JSON.stringify({ message: body })
    });
    const j = await r.json();
    res.className = r.ok ? 'result ok' : 'result err';
    res.innerHTML = '<pre>' + JSON.stringify(j, null, 2) + '</pre>';
    if (r.ok) ta.value = '';
  } catch (e) {
    res.className = 'result err';
    res.textContent = 'Error: ' + e.message;
  }
  res.style.display = 'block';
  btn.disabled = false;
}

async function sendFile() {
  const inp = document.getElementById('fileIn');
  const res = document.getElementById('fileRes');
  if (!inp.files.length) return;
  const file = inp.files[0];
  res.className = 'result';
  res.textContent = 'Uploading ' + file.name + '...';
  res.style.display = 'block';
  try {
    const r = await fetch('/?wait=0', {
      method: 'POST',
      headers: {
        'content-type': file.type || 'application/octet-stream',
        'antenna-correlation-id': 'web-' + Date.now()
      },
      body: file
    });
    const j = await r.json();
    res.className = r.ok ? 'result ok' : 'result err';
    res.innerHTML = '<pre>' + JSON.stringify(j, null, 2) + '</pre>';
    if (r.ok) inp.value = '';
  } catch (e) {
    res.className = 'result err';
    res.textContent = 'Error: ' + e.message;
  }
}
</script>

<h2>Send programmatically</h2>

<div class="endpoint">
  <span class="method">POST</span> <code>/</code> — send anything (JSON, text, or file)
  <pre>curl -X POST https://antenna.minilab.work/ \
  -H 'content-type: application/json' \
  -H 'antenna-correlation-id: my-id-123' \
  -d '{"hello":"world"}'</pre>
  <p class="note">Antenna sorts it out: JSON/text → receipt, files → bucket. Add <code>?wait=0</code> to fire-and-forget.</p>
</div>

<div class="endpoint">
  <span class="method">POST</span> <code>/</code> — upload a file
  <pre>curl -X POST https://antenna.minilab.work/ \
  -H 'content-type: application/pdf' \
  -H 'antenna-correlation-id: my-id-124' \
  --data-binary @document.pdf</pre>
  <p class="note">Any non-text content-type streams to the bucket (up to 256 MiB).</p>

<div class="endpoint">
  <span class="method">WS</span> <code>/ws</code> — WebSocket stream
  <pre>websocat wss://antenna.minilab.work/ws</pre>
  <p class="note">Send JSON frames. Each frame becomes a receipt.</p>
</div>

<div class="endpoint">
  <span class="method">POST</span> <code>/mcp</code> — MCP tool call (stateless, 2026-07-28)
  <pre>curl -X POST https://antenna.minilab.work/mcp \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'</pre>
</div>

<h2>Inspect</h2>

<div class="endpoint">
  <span class="method">GET</span> <code>/health</code> — public liveness &amp; counts
</div>

<div class="endpoint">
  <span class="method">GET</span> <code>/status</code> — operational dashboard (auth required)
</div>

<div class="endpoint">
  <span class="method">GET</span> <code>/receipts</code> — recent receipts (auth required)
</div>

<div class="endpoint">
  <span class="method">GET</span> <code>/receipts/{id}/raw</code> — bytes exactly as received (auth required)
</div>

<div class="endpoint">
  <span class="method">GET</span> <code>/deliveries</code> — outbox with authority decisions (auth required)
</div>

<p class="note">Inspection endpoints require <code>Authorization: Bearer &lt;inspect_token&gt;</code>.</p>

<h2>What happens to your signal</h2>
<ol>
  <li>Antenna receives the bytes and computes a <code>blake3</code> digest.</li>
  <li>The receipt is committed to SQLite with <code>synchronous=FULL</code>.</li>
  <li>Only then is it acknowledged and routed.</li>
  <li>Routing is deterministic — no model needed for known shapes.</li>
  <li>Unknown input defers rather than failing; it can be re-routed later.</li>
  <li>Every outward effect is a Delivery, checked by the Gatekeeper before it leaves.</li>
</ol>

<p class="note">Antenna v"##;

    let version = env!("CARGO_PKG_VERSION");
    let tail = format!(r##"{} · <a href="/health">health</a> · <a href="https://github.com/openclaw/openclaw">source</a></p>
</body>
</html>"##, version);

    (StatusCode::OK, Html(format!("{}{}", html, tail)))
}

// --------------------------------------------------------- inspection ----
// Raw received material remains inspectable (spec §24). These are reads only.

pub async fn health(State(app): State<App>) -> impl IntoResponse {
    let counts = app
        .db
        .call(|c| {
            let receipts: i64 = c.query_row("SELECT COUNT(*) FROM receipts", [], |r| r.get(0))?;
            let pending: i64 = c.query_row(
                "SELECT COUNT(*) FROM deliveries WHERE status='pending'", [], |r| r.get(0))?;
            let uncertain: i64 = c.query_row(
                "SELECT COUNT(*) FROM deliveries WHERE resumed_uncertain=1", [], |r| r.get(0))?;
            Ok((receipts, pending, uncertain))
        })
        .await;

    match counts {
        Ok((receipts, pending, uncertain)) => (StatusCode::OK, Json(json!({
            "status": "alive",
            "version": env!("CARGO_PKG_VERSION"),
            "receipts": receipts,
            "deliveries_pending": pending,
            "deliveries_resumed_uncertain": uncertain,
            "capabilities": app.caps.names(),
            "talent_enabled": app.cfg.talent.enabled
        })))
        .into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "degraded", "error": e.to_string() }))).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct ListQuery { pub limit: Option<i64> }

/// Say only that a token is required. Never hint at what was wrong with the
/// one offered, and never reveal whether the id asked for exists.
fn unauthorized() -> axum::response::Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": "the inspection surface requires a bearer token",
            "hint": "Authorization: Bearer <server.inspect_token>"
        })),
    )
        .into_response()
}

pub async fn list_receipts(
    State(app): State<App>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    if !inspection_allowed(&app, &headers) { return unauthorized(); }
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match app.db.call(move |c| crate::receipt::recent(c, limit)).await {
        Ok(rows) => Json(json!({ "count": rows.len(), "receipts": rows })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() }))).into_response(),
    }
}

pub async fn get_receipt(
    State(app): State<App>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if !inspection_allowed(&app, &headers) { return unauthorized(); }
    let id2 = id.clone();
    let loaded = app
        .db
        .call(move |c| {
            let r = crate::receipt::load(c, &id2)?;
            let runs = crate::run::for_receipt(c, &id2)?;
            Ok((r, runs))
        })
        .await;

    match loaded {
        Ok((Some(r), runs)) => Json(json!({ "receipt": r, "runs": runs })).into_response(),
        Ok((None, _)) => (StatusCode::NOT_FOUND,
            Json(json!({ "error": "no such receipt", "id": id }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() }))).into_response(),
    }
}

/// The bytes exactly as they arrived.
pub async fn get_receipt_raw(
    State(app): State<App>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if !inspection_allowed(&app, &headers) { return unauthorized(); }
    let id2 = id.clone();
    let r = match app.db.call(move |c| crate::receipt::load(c, &id2)).await {
        Ok(Some(r)) => r,
        Ok(None) => return (StatusCode::NOT_FOUND, "no such receipt").into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match r.raw(&app.bucket).await {
        Ok(bytes) => {
            let mut h = HeaderMap::new();
            let ct = r.content_type.clone().unwrap_or_else(|| "application/octet-stream".into());
            if let Ok(v) = ct.parse() { h.insert("content-type", v); }
            if let Some(d) = &r.raw_digest {
                if let Ok(v) = format!("blake3={d}").parse() { h.insert("digest", v); }
            }
            (StatusCode::OK, h, bytes).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn list_deliveries(
    State(app): State<App>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    if !inspection_allowed(&app, &headers) { return unauthorized(); }
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match app.db.call(move |c| crate::delivery::recent(c, limit)).await {
        Ok(rows) => Json(json!({ "count": rows.len(), "deliveries": rows })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() }))).into_response(),
    }
}

/// Authenticated machine status. Counts and timestamps only; never payloads.
pub async fn operational_status(
    State(app): State<App>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !inspection_allowed(&app, &headers) { return unauthorized(); }
    let status = app.db.call(|c| {
        let receipt_count: i64 = c.query_row("SELECT COUNT(*) FROM receipts", [], |r| r.get(0))?;
        let pending_normalization: i64 = c.query_row(
            "SELECT COUNT(*) FROM github_deliveries WHERE processing_status='accepted'", [], |r| r.get(0))?;
        let pending_reconciliation: i64 = c.query_row(
            "SELECT COUNT(*) FROM observations WHERE reconciliation_status='pending'", [], |r| r.get(0))?;
        let running_reconciliation: i64 = c.query_row(
            "SELECT COUNT(*) FROM reconciliation_runs WHERE status='running'", [], |r| r.get(0))?;
        let pending_projection: i64 = c.query_row(
            "SELECT COUNT(*) FROM projection_records WHERE status IN ('pending','failed','uncertain')", [], |r| r.get(0))?;
        let duplicate_deliveries: i64 = c.query_row(
            "SELECT COALESCE(SUM(duplicate_count),0) FROM github_deliveries", [], |r| r.get(0))?;
        let unknown_events: i64 = c.query_row(
            "SELECT COUNT(*) FROM observations WHERE classification='UNKNOWN'", [], |r| r.get(0))?;
        let last_delivery: Option<String> = c.query_row(
            "SELECT MAX(last_received_at) FROM github_deliveries", [], |r| r.get(0))?;
        let last_census: Option<(String, i64)> = c.query_row(
            "SELECT completed_at,repository_count FROM census_runs
             WHERE status='completed' AND complete=1 ORDER BY completed_at DESC LIMIT 1",
            [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        let last_reconciliation: Option<(String, String, Option<String>)> = c.query_row(
            "SELECT id,status,result_json FROM reconciliation_runs ORDER BY started_at DESC LIMIT 1",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?;
        let last_projection: Option<String> = c.query_row(
            "SELECT MAX(completed_at) FROM projection_mutations WHERE status='completed'", [], |r| r.get(0))?;
        Ok(json!({
            "status": "alive",
            "receipt_count": receipt_count,
            "pending_normalization": pending_normalization,
            "pending_reconciliation": pending_reconciliation,
            "running_reconciliation": running_reconciliation,
            "pending_sheet_projection": pending_projection,
            "duplicate_delivery_count": duplicate_deliveries,
            "last_github_delivery_at": last_delivery,
            "last_successful_census_at": last_census.as_ref().map(|v| &v.0),
            "last_census_repository_count": last_census.as_ref().map(|v| v.1),
            "last_reconciliation": last_reconciliation.map(|v| json!({"id":v.0,"status":v.1,"result":v.2})),
            "last_successful_blueprint_projection_at": last_projection,
            "unknown_event_count": unknown_events
        }))
    }).await;
    match status {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"degraded","error":error.to_string()}))).into_response(),
    }
}
