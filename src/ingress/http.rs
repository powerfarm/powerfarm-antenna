//! HTTP ingress: journeys A (event/call) and B (blob).

use crate::app::App;
use crate::ingress::{header, inspection_allowed, principal_of, source_of, trace_id_from};
use crate::journey;
use crate::receipt::NewReceipt;
use crate::router::RouteMeta;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
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
