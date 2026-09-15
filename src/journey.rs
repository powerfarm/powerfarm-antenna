//! The shared core. HTTP, WebSocket and MCP all pass through exactly this
//! path — that shared spine is what makes Antenna one organism rather than
//! three adapters wearing a trenchcoat (spec §21).
//!
//!   receipt -> persist -> route -> capability -> run -> outbox -> delivery

use crate::app::App;
use crate::capability::{CapabilityInput, InvocationContext};
use crate::delivery::{self, DeliveryRequest};
use crate::receipt::{self, NewReceipt, Status};
use crate::router::{Decision, RouteMeta};
use crate::run;
use anyhow::Result;
use serde_json::{json, Value};

pub struct Accepted {
    pub receipt_id: String,
    pub trace_id: String,
    pub correlation_id: String,
}

#[derive(Debug, Clone)]
pub enum Outcome {
    Completed { run_id: String, result: Value },
    Deferred { reason: String },
    Quarantined { reason: String },
    Ignored,
    Failed { error: String },
}

impl Outcome {
    pub fn status(&self) -> &'static str {
        match self {
            Outcome::Completed { .. } => "completed",
            Outcome::Deferred { .. } => "deferred",
            Outcome::Quarantined { .. } => "quarantined",
            Outcome::Ignored => "ignored",
            Outcome::Failed { .. } => "failed",
        }
    }
    pub fn to_json(&self, receipt_id: &str) -> Value {
        match self {
            Outcome::Completed { run_id, result } => json!({
                "receipt_id": receipt_id, "status": "completed",
                "run_id": run_id, "result": result
            }),
            Outcome::Deferred { reason } => json!({
                "receipt_id": receipt_id, "status": "deferred", "reason": reason
            }),
            Outcome::Quarantined { reason } => json!({
                "receipt_id": receipt_id, "status": "quarantined", "reason": reason
            }),
            Outcome::Ignored => json!({ "receipt_id": receipt_id, "status": "ignored" }),
            Outcome::Failed { error } => json!({
                "receipt_id": receipt_id, "status": "failed", "error": error
            }),
        }
    }
}

/// Commit the receipt. Nothing is acknowledged to the world before this
/// returns; nothing is interpreted before this returns (spec §4).
pub async fn accept(app: &App, mut n: NewReceipt, trace_id: String) -> Result<Accepted> {
    let receipt_id = crate::ids::new_id("rcp");
    let span_id = crate::ids::new_span_id();
    let correlation_id = n
        .correlation_id
        .clone()
        .unwrap_or_else(|| crate::ids::new_id("cor"));
    n.correlation_id = Some(correlation_id.clone());
    n.telemetry = Some(crate::telemetry::stamp(&trace_id, &span_id));

    let recorded_at = crate::ids::now_rfc3339();
    let rid = receipt_id.clone();
    app.db
        .call(move |c| receipt::insert(c, &rid, &recorded_at, &n))
        .await?;

    tracing::info!(receipt = %receipt_id, trace = %trace_id, "receipt committed");
    Ok(Accepted { receipt_id, trace_id, correlation_id })
}

/// Route and, where the decision says so, invoke. Every branch is durable.
pub async fn process(app: &App, receipt_id: &str, meta: RouteMeta) -> Outcome {
    match process_inner(app, receipt_id, meta).await {
        Ok(o) => o,
        Err(e) => {
            let err = e.to_string();
            tracing::error!(receipt = %receipt_id, error = %err, "journey failed");
            let rid = receipt_id.to_string();
            let _ = app
                .db
                .call(move |c| receipt::set_status(c, &rid, Status::Failed))
                .await;
            Outcome::Failed { error: err }
        }
    }
}

async fn process_inner(app: &App, receipt_id: &str, meta: RouteMeta) -> Result<Outcome> {
    let rid = receipt_id.to_string();
    let r = app
        .db
        .call(move |c| receipt::load(c, &rid))
        .await?
        .ok_or_else(|| anyhow::anyhow!("receipt {receipt_id} not found"))?;

    let trace_id = r
        .telemetry
        .as_deref()
        .and_then(|t| serde_json::from_str::<Value>(t).ok())
        .and_then(|v| v.get("trace_id").and_then(|s| s.as_str()).map(String::from))
        .unwrap_or_else(crate::ids::new_trace_id);

    let (route_name, decision) = if r.transport!="mcp" && crate::services::context(&r)?.is_some() {
        (Some("contracted-service".into()),Decision::Invoke("service.invoke".into()))
    } else {app.router.decide(&r, &meta)};
    let rid = receipt_id.to_string();
    let rn = route_name.clone();
    let dec = decision.clone();
    app.db
        .call(move |c| crate::router::record(c, &rid, rn.as_deref(), &dec, None))
        .await?;

    tracing::info!(
        receipt = %receipt_id, route = ?route_name, decision = decision.action(),
        target = ?decision.target(), "routed"
    );

    let capability_name = match &decision {
        Decision::Invoke(c) | Decision::AskTalent(c) => c.clone(),
        Decision::Defer => {
            set(app, receipt_id, Status::Deferred).await;
            return Ok(Outcome::Deferred { reason: "route deferred this receipt".into() });
        }
        Decision::Quarantine => {
            set(app, receipt_id, Status::Quarantined).await;
            return Ok(Outcome::Quarantined { reason: "no route claimed this receipt".into() });
        }
        Decision::Ignore => {
            set(app, receipt_id, Status::Ignored).await;
            return Ok(Outcome::Ignored);
        }
        Decision::Fail(why) => {
            set(app, receipt_id, Status::Failed).await;
            return Ok(Outcome::Failed { error: why.clone() });
        }
    };

    let cap = app
        .caps
        .get(&capability_name)
        .ok_or_else(|| anyhow::anyhow!("route names unregistered capability {capability_name:?}"))?;

    let run_id = crate::ids::new_id("run");
    let span_id = crate::ids::new_span_id();
    let stamp = crate::telemetry::stamp(&trace_id, &span_id);
    {
        let (rid, rn, cn, st) = (
            run_id.clone(), receipt_id.to_string(), capability_name.clone(), stamp.clone(),
        );
        let service=crate::services::context(&r)?.is_some();
        let claimed=app.db.call(move |c| {
            let tx=c.transaction()?;
            if service && tx.query_row("SELECT EXISTS(SELECT 1 FROM runs WHERE receipt_id=?1 AND status='running')",[&rn],|r|r.get::<_,bool>(0))? {return Ok(false);}
            run::create(&tx,&rid,&rn,&cn,None,Some(&st))?;
            tx.commit()?;
            Ok(true)
        }).await?;
        if !claimed {return Ok(Outcome::Deferred{reason:"this receipt is already running".into()});}
    }

    let ctx = InvocationContext {
        app: app.clone(),
        receipt_id: receipt_id.to_string(),
        run_id: run_id.clone(),
        trace_id: trace_id.clone(),
        span_id,
        correlation_id: r.correlation_id.clone(),
    };
    let input = CapabilityInput { receipt: r.clone(), params: Value::Null };

    let invoked = cap.invoke(ctx, input).await;

    match invoked {
        Err(e) => {
            let err = e.to_string();
            let (rid, er) = (run_id.clone(), err.clone());
            app.db.call(move |c| run::fail(c, &rid, &er)).await?;
            set(app, receipt_id, Status::Failed).await;
            Ok(Outcome::Failed { error: err })
        }
        Ok(out) => {
            // Every outward effect becomes a Delivery — including the answer
            // owed to the origin. Nothing here sends anything itself.
            let mut requests = out.deliveries;
            if let (Some(return_path), Some(correlation_id)) =
                (r.return_path.clone(), r.correlation_id.clone())
            {
                // A correlated envelope, not a bare result: an answer may be
                // produced by a different Actor than the one that received the
                // signal, so it has to carry its own correlation (spec §15).
                let envelope = json!({
                    "receipt_id": receipt_id,
                    "correlation_id": correlation_id,
                    "run_id": run_id,
                    "status": "completed",
                    "result": out.result.clone(),
                });
                requests.push(DeliveryRequest::to_return_path(
                    &return_path,
                    &correlation_id,
                    envelope,
                ));
            }

            let cfg = app.cfg.clone();
            let (rid, run, cor, st) = (
                receipt_id.to_string(), run_id.clone(),
                r.correlation_id.clone(), stamp.clone(),
            );
            let result_json = serde_json::to_string(&out.result)?;
            app.db.call(move |c| {
                let tx = c.transaction()?;
                for req in requests {
                        delivery::enqueue(
                            &tx, &cfg, Some(&rid), Some(&run), cor.as_deref(), &req, Some(&st),
                        )?;
                }
                run::complete(&tx, &run, &result_json)?;
                receipt::set_status(&tx, &rid, Status::Routed)?;
                tx.commit()?;
                Ok(())
            }).await?;
            // Wake the outbox now rather than at the next tick, so a CALL
            // is not paying the poll interval as latency.
            app.outbox_notify.notify_waiters();

            Ok(Outcome::Completed { run_id, result: out.result })
        }
    }
}

async fn set(app: &App, receipt_id: &str, s: Status) {
    let rid = receipt_id.to_string();
    if let Err(e) = app.db.call(move |c| receipt::set_status(c, &rid, s)).await {
        tracing::error!(receipt = %receipt_id, error = %e, "could not update receipt status");
    }
}

/// Boot-time recovery (spec §19).
///
/// Three worklists, in order: runs abandoned mid-flight are closed and their
/// receipts re-queued; deliveries abandoned mid-attempt are marked uncertain
/// and re-attempted under the same idempotency key; receipts that were
/// committed but never routed are routed now.
pub async fn recover(app: App) -> Result<()> {
    let interrupted = app.db.call(|c| run::mark_interrupted(c)).await?;
    if !interrupted.is_empty() {
        tracing::warn!(count = interrupted.len(), "closed runs interrupted by restart");
        for rid in &interrupted {
            let rid = rid.clone();
            let _ = app
                .db
                .call(move |c| receipt::set_status(c, &rid, Status::Accepted))
                .await;
        }
    }

    let resumed = app.db.call(|c| delivery::recover(c)).await?;
    if resumed > 0 {
        tracing::warn!(count = resumed, "deliveries resumed after restart (outcome uncertain)");
    }

    let pending = app.db.call(|c| receipt::unfinished(c)).await?;
    if pending.is_empty() {
        tracing::info!("no unfinished receipts");
        return Ok(());
    }
    tracing::warn!(count = pending.len(), "resuming unfinished receipts");

    for r in pending {
        if let Some(context)=crate::services::context(&r)? {
            if app.services.authorize(&context).await.is_err() {continue;}
        }
        let app = app.clone();
        tokio::spawn(async move {
            // Re-derive MCP routing metadata from the stored body, so a
            // resumed MCP call routes the same way it would have live.
            let meta = if r.transport == "mcp" {
                let body = r.json(&app.bucket).await.unwrap_or(Value::Null);
                RouteMeta {
                    method: body.get("method").and_then(|m| m.as_str()).map(String::from),
                    tool: body.pointer("/params/name").and_then(|m| m.as_str()).map(String::from),
                }
            } else {
                RouteMeta::default()
            };
            let outcome = process(&app, &r.id, meta).await;
            tracing::info!(receipt = %r.id, outcome = outcome.status(), "resumed receipt settled");
        });
    }
    Ok(())
}
