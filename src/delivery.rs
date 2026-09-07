//! The outbox (spec §14).
//!
//! Handlers never "send to the world". They request a Delivery, which is
//! committed to SQLite in the same breath as the run's completion. A worker
//! then attempts it. Retries are explicit rows, not hidden state.

use crate::app::App;
use crate::gatekeeper;
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryRow {
    pub id: String,
    pub receipt_id: Option<String>,
    pub run_id: Option<String>,
    pub correlation_id: Option<String>,
    pub destination: String,
    pub transport: String,
    pub idempotency_key: String,
    pub content_type: Option<String>,
    #[serde(skip)]
    pub payload_inline: Option<Vec<u8>>,
    pub attempt: i64,
    pub max_attempts: i64,
    pub status: String,
    pub resumed_uncertain: i64,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
    pub authority: Option<String>,
    pub telemetry: Option<String>,
}

fn from_row(r: &Row<'_>) -> rusqlite::Result<DeliveryRow> {
    Ok(DeliveryRow {
        id: r.get("id")?,
        receipt_id: r.get("receipt_id")?,
        run_id: r.get("run_id")?,
        correlation_id: r.get("correlation_id")?,
        destination: r.get("destination")?,
        transport: r.get("transport")?,
        idempotency_key: r.get("idempotency_key")?,
        content_type: r.get("content_type")?,
        payload_inline: r.get("payload_inline")?,
        attempt: r.get("attempt")?,
        max_attempts: r.get("max_attempts")?,
        status: r.get("status")?,
        resumed_uncertain: r.get("resumed_uncertain")?,
        created_at: r.get("created_at")?,
        completed_at: r.get("completed_at")?,
        last_error: r.get("last_error")?,
        authority: r.get("authority")?,
        telemetry: r.get("telemetry")?,
    })
}

/// What a capability asks for. It does not get to say "send"; it says "deliver".
#[derive(Debug, Clone)]
pub struct DeliveryRequest {
    pub destination: String,
    pub payload: Value,
    pub content_type: String,
    /// Stable across retries AND across restarts. This is the duplicate
    /// protection: the UNIQUE index makes a second enqueue a no-op.
    pub idempotency_key: String,
}

impl DeliveryRequest {
    pub fn to_return_path(return_path: &str, correlation_id: &str, payload: Value) -> Self {
        Self {
            destination: format!("return-path:{return_path}"),
            payload,
            content_type: "application/json".into(),
            // One answer per correlation. A retry cannot produce a second.
            idempotency_key: format!("return:{correlation_id}"),
        }
    }
}

fn transport_of(destination: &str) -> &'static str {
    if destination.starts_with("return-path:") {
        "return-path"
    } else if destination.starts_with("capability:") {
        "capability"
    } else if destination.starts_with("https://") || destination.starts_with("http://") {
        "http"
    } else {
        "unknown"
    }
}

/// Enqueue. Idempotent by key: re-enqueueing an existing logical effect
/// returns the existing row rather than creating a second one.
pub fn enqueue(
    conn: &Connection,
    cfg: &crate::config::Config,
    receipt_id: Option<&str>,
    run_id: Option<&str>,
    correlation_id: Option<&str>,
    req: &DeliveryRequest,
    telemetry: Option<&str>,
) -> Result<String> {
    if let Some(existing) = conn
        .query_row(
            "SELECT id FROM deliveries WHERE idempotency_key = ?1",
            params![req.idempotency_key],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        tracing::debug!(key = %req.idempotency_key, "delivery already enqueued; not duplicating");
        return Ok(existing);
    }

    let id = crate::ids::new_id("dlv");
    let now = crate::ids::now_rfc3339();
    let body = serde_json::to_vec(&req.payload)?;

    // The authority decision is taken and recorded at enqueue time, before the
    // effect is ever attempted, and re-checked before each attempt.
    let authority = gatekeeper::authorize(cfg, &req.destination);
    let status = if authority.is_granted() { "pending" } else { "denied" };

    conn.execute(
        "INSERT INTO deliveries (
            id, receipt_id, run_id, correlation_id, destination, transport,
            idempotency_key, payload_inline, content_type,
            attempt, max_attempts, status, next_attempt_at,
            created_at, authority, telemetry
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,0,?10,?11,?12,?12,?13,?14)",
        params![
            id, receipt_id, run_id, correlation_id, req.destination,
            transport_of(&req.destination), req.idempotency_key, body, req.content_type,
            cfg.delivery.max_attempts, status, now, authority.to_json(), telemetry
        ],
    )?;

    if !authority.is_granted() {
        tracing::warn!(
            delivery = %id, destination = %req.destination, reason = %authority.reason(),
            "delivery denied by gatekeeper before any effect"
        );
    }
    Ok(id)
}

pub fn get(conn: &Connection, id: &str) -> Result<Option<DeliveryRow>> {
    Ok(conn
        .query_row("SELECT * FROM deliveries WHERE id=?1", params![id], from_row)
        .optional()?)
}

pub fn recent(conn: &Connection, limit: i64) -> Result<Vec<DeliveryRow>> {
    let mut stmt =
        conn.prepare("SELECT * FROM deliveries ORDER BY created_at DESC, id DESC LIMIT ?1")?;
    let rows = stmt.query_map(params![limit], from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn claim_due(conn: &mut Connection, limit: i64) -> Result<Vec<DeliveryRow>> {
    let now = crate::ids::now_rfc3339();
    let tx = conn.transaction()?;
    let due: Vec<DeliveryRow> = {
        let mut stmt = tx.prepare(
            "SELECT * FROM deliveries
             WHERE status='pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?1)
             ORDER BY created_at ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![now, limit], from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for d in &due {
        tx.execute(
            "UPDATE deliveries SET status='in_flight', attempt=attempt+1, started_at=?2 WHERE id=?1",
            params![d.id, now],
        )?;
        tx.execute(
            "INSERT INTO delivery_attempts (delivery_id, attempt, started_at) VALUES (?1,?2,?3)",
            params![d.id, d.attempt + 1, now],
        )?;
    }
    tx.commit()?;
    Ok(due)
}

fn finish_attempt(
    conn: &Connection,
    d: &DeliveryRow,
    outcome: &str,
    detail: Option<&str>,
) -> Result<()> {
    let now = crate::ids::now_rfc3339();
    conn.execute(
        "UPDATE delivery_attempts SET completed_at=?3, outcome=?4, detail=?5
         WHERE delivery_id=?1 AND attempt=?2",
        params![d.id, d.attempt + 1, now, outcome, detail],
    )?;
    Ok(())
}

fn settle(conn: &Connection, d: &DeliveryRow, status: &str, err: Option<&str>) -> Result<()> {
    // `resumed_uncertain` is deliberately NOT cleared here. That a delivery
    // was once resumed across a crash with an unknown prior outcome is a
    // permanent fact about this record. Completing it later does not make the
    // earlier uncertainty untrue, and an audit needs to still see it.
    conn.execute(
        "UPDATE deliveries SET status=?2, completed_at=?3, last_error=?4 WHERE id=?1",
        params![d.id, status, crate::ids::now_rfc3339(), err],
    )?;
    Ok(())
}

fn reschedule(conn: &Connection, d: &DeliveryRow, backoff_ms: i64, err: &str) -> Result<()> {
    let attempts_used = d.attempt + 1;
    if attempts_used >= d.max_attempts {
        conn.execute(
            "UPDATE deliveries SET status='failed', completed_at=?2, last_error=?3 WHERE id=?1",
            params![d.id, crate::ids::now_rfc3339(), err],
        )?;
        tracing::error!(delivery = %d.id, attempts = attempts_used, "delivery exhausted its attempts");
    } else {
        // Exponential, capped. Explicit and inspectable: next_attempt_at is a
        // readable timestamp in the row, not a timer hidden in a process.
        let backoff = backoff_ms.saturating_mul(1i64 << attempts_used.min(6));
        conn.execute(
            "UPDATE deliveries SET status='pending', next_attempt_at=?2, last_error=?3 WHERE id=?1",
            params![d.id, crate::ids::in_ms(backoff), err],
        )?;
    }
    Ok(())
}

/// Crash recovery for the outbox (spec §19).
///
/// `in_flight` at boot means the process died mid-attempt. We do NOT know
/// whether the external effect happened. We say so — `resumed_uncertain` — and
/// retry with the SAME idempotency key so the receiver can collapse a duplicate.
/// That is the honest position; silently dropping or blindly resending are both
/// worse.
pub fn recover(conn: &Connection) -> Result<usize> {
    let n = conn.execute(
        "UPDATE deliveries
            SET status='pending', resumed_uncertain=1, next_attempt_at=?1,
                last_error=COALESCE(last_error,'') || ' | resumed after restart: outcome of previous attempt unknown'
          WHERE status='in_flight'",
        params![crate::ids::now_rfc3339()],
    )?;
    Ok(n)
}

/// The outbox worker. One task, polling plus a notify for latency.
pub async fn worker(app: App) {
    let interval = std::time::Duration::from_millis(app.cfg.delivery.worker_interval_ms);
    loop {
        match app.db.call(move |c| claim_due(c, 16)).await {
            Ok(due) => {
                for d in due {
                    let app = app.clone();
                    tokio::spawn(async move { attempt(app, d).await });
                }
            }
            Err(e) => tracing::error!(error = %e, "outbox claim failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = app.outbox_notify.notified() => {}
        }
    }
}

#[tracing::instrument(skip_all, fields(delivery = %d.id, destination = %d.destination, attempt = d.attempt + 1))]
async fn attempt(app: App, d: DeliveryRow) {
    // Re-check authority at the moment of effect. Policy may have changed
    // since enqueue, and the check that matters is the one before the action.
    let authority = gatekeeper::authorize(&app.cfg, &d.destination);
    if !authority.is_granted() {
        let reason = authority.reason().to_string();
        tracing::warn!(reason = %reason, "delivery refused at attempt time");
        let dd = d.clone();
        let r2 = reason.clone();
        let _ = app
            .db
            .call(move |c| {
                finish_attempt(c, &dd, "denied", Some(&r2))?;
                settle(c, &dd, "denied", Some(&r2))
            })
            .await;
        return;
    }

    let payload: Value = d
        .payload_inline
        .as_deref()
        .and_then(|b| serde_json::from_slice(b).ok())
        .unwrap_or(Value::Null);

    let result: std::result::Result<String, String> = match d.transport.as_str() {
        "return-path" => {
            let key = d.destination.trim_start_matches("return-path:").to_string();
            match app.returns.send(&key, payload) {
                crate::returnpath::Handback::Delivered => Ok("handed back to origin".into()),
                crate::returnpath::Handback::Gone => Err("__ORPHANED__".into()),
            }
        }
        "http" => {
            let mut req = app
                .http
                .post(&d.destination)
                .header("content-type", d.content_type.as_deref().unwrap_or("application/json"))
                // The receiver's half of duplicate protection.
                .header("Idempotency-Key", &d.idempotency_key)
                .header("Antenna-Delivery", &d.id);
            if d.resumed_uncertain == 1 {
                req = req.header("Antenna-Resumed", "1");
            }
            if let Some(t) = telemetry_traceparent(&d) {
                req = req.header("traceparent", t);
            }
            match req.json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => Ok(format!("http {}", resp.status())),
                Ok(resp) => Err(format!("http {}", resp.status())),
                Err(e) => Err(format!("http error: {e}")),
            }
        }
        "capability" => Ok("internal hand-off recorded".into()),
        other => Err(format!("no transport for {other}")),
    };

    let backoff = app.cfg.delivery.base_backoff_ms;
    let dd = d.clone();
    let outcome = app
        .db
        .call(move |c| {
            match result {
                Ok(detail) => {
                    finish_attempt(c, &dd, "completed", Some(&detail))?;
                    settle(c, &dd, "completed", None)?;
                    Ok::<_, anyhow::Error>("completed")
                }
                Err(e) if e == "__ORPHANED__" => {
                    // The origin is gone. Terminal: retrying has nowhere to go,
                    // and inventing a new destination would be a fabrication.
                    let msg = "return path no longer present (peer gone or process restarted)";
                    finish_attempt(c, &dd, "orphaned", Some(msg))?;
                    settle(c, &dd, "orphaned", Some(msg))?;
                    Ok("orphaned")
                }
                Err(e) => {
                    finish_attempt(c, &dd, "failed", Some(&e))?;
                    reschedule(c, &dd, backoff, &e)?;
                    Ok("retrying")
                }
            }
        })
        .await;

    match outcome {
        Ok(o) => tracing::info!(outcome = o, "delivery attempt settled"),
        Err(e) => tracing::error!(error = %e, "could not record delivery outcome"),
    }
}

/// Propagate the journey's trace to the receiving system, so an external
/// service's spans join the same trace as the receipt that caused them.
fn telemetry_traceparent(d: &DeliveryRow) -> Option<String> {
    let raw = d.telemetry.as_ref()?;
    let v: Value = serde_json::from_str(raw).ok()?;
    v.get("traceparent")?.as_str().map(|s| s.to_string())
}
