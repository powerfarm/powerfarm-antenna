//! Narrow authenticated handoff from the Cloudflare GitHub receiver.

use crate::app::App;
use crate::ingress::{header, source_of};
use crate::observation::{self, DeliveryEvidence};
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use std::net::SocketAddr;

type HmacSha256 = Hmac<Sha256>;

fn reject(status: StatusCode, error: &'static str) -> axum::response::Response {
    (status, Json(json!({"error": error}))).into_response()
}

fn verify_handoff(
    app: &App,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(String, String), &'static str> {
    let secret = app.cfg.github.handoff_secret.as_deref().ok_or("handoff is not configured")?;
    let timestamp = header(headers, "x-antenna-timestamp").ok_or("missing handoff timestamp")?;
    let delivery = header(headers, "x-antenna-delivery").ok_or("missing delivery identity")?;
    let event = header(headers, "x-antenna-github-event").ok_or("missing GitHub event")?;
    let claimed_digest = header(headers, "x-antenna-body-sha256").ok_or("missing body digest")?;
    let signature = header(headers, "x-antenna-signature").ok_or("missing handoff signature")?;

    if delivery.len() > 200 || event.len() > 100 || timestamp.len() > 20 {
        return Err("invalid handoff metadata");
    }
    let timestamp_i64 = timestamp.parse::<i64>().map_err(|_| "invalid handoff timestamp")?;
    let now = chrono::Utc::now().timestamp();
    if (now - timestamp_i64).abs() > app.cfg.github.handoff_max_age_secs {
        return Err("stale handoff");
    }
    let actual_digest = observation::sha256_hex(body);
    if !constant_time_eq(claimed_digest.as_bytes(), actual_digest.as_bytes()) {
        return Err("body digest mismatch");
    }
    let offered_hex = signature.strip_prefix("v1=").ok_or("invalid handoff signature")?;
    let offered = hex::decode(offered_hex).map_err(|_| "invalid handoff signature")?;
    let canonical = format!("{timestamp}\n{delivery}\n{event}\n{actual_digest}\n");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| "invalid handoff configuration")?;
    mac.update(canonical.as_bytes());
    mac.verify_slice(&offered).map_err(|_| "invalid handoff signature")?;
    Ok((delivery, event))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) { diff |= left ^ right; }
    diff == 0
}

#[tracing::instrument(skip_all, fields(transport = "cloudflare-worker", interaction = "github-event"))]
pub async fn accept(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let (delivery_id, event_name) = match verify_handoff(&app, &headers, &body) {
        Ok(metadata) => metadata,
        Err("handoff is not configured") => return reject(StatusCode::SERVICE_UNAVAILABLE, "handoff unavailable"),
        Err("stale handoff") => return reject(StatusCode::UNAUTHORIZED, "stale handoff"),
        Err(_) => return reject(StatusCode::UNAUTHORIZED, "invalid handoff"),
    };
    let source = source_of(&headers, Some(peer));
    let evidence = DeliveryEvidence {
        delivery_id,
        event_name,
        body: body.to_vec(),
        source,
        received_at: crate::ids::now_rfc3339(),
    };
    let accepted = match observation::accept_github_delivery(&app.db, evidence).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "could not durably accept GitHub delivery");
            return reject(StatusCode::INTERNAL_SERVER_ERROR, "durable acceptance failed");
        }
    };

    if accepted.meaningful {
        let db = app.db.clone();
        let identity = accepted.delivery_id.clone();
        tokio::spawn(async move {
            if let Err(error) = crate::reconcile::run(&db, "github_webhook", Some(&identity)).await {
                tracing::error!(error = %error, delivery_id = %identity, "reconciliation failed");
            }
        });
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "durably_accepted",
            "receipt_id": accepted.receipt_id,
            "delivery_id": accepted.delivery_id,
            "duplicate": accepted.duplicate,
            "classification": accepted.classification
        })),
    ).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn signed_headers(secret: &str, delivery: &str, event: &str, body: &[u8], timestamp: i64) -> HeaderMap {
        let digest = observation::sha256_hex(body);
        let canonical = format!("{timestamp}\n{delivery}\n{event}\n{digest}\n");
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(canonical.as_bytes());
        let signature = format!("v1={}", hex::encode(mac.finalize().into_bytes()));
        let mut headers = HeaderMap::new();
        headers.insert("x-antenna-timestamp", timestamp.to_string().parse().unwrap());
        headers.insert("x-antenna-delivery", delivery.parse().unwrap());
        headers.insert("x-antenna-github-event", event.parse().unwrap());
        headers.insert("x-antenna-body-sha256", digest.parse().unwrap());
        headers.insert("x-antenna-signature", signature.parse().unwrap());
        headers
    }

    #[test]
    fn valid_and_tampered_handoffs_are_distinguished() {
        let mut cfg = Config::default();
        cfg.storage.data_dir = std::env::temp_dir().join(format!("antenna-github-unit-{}", uuid::Uuid::new_v4()));
        cfg.github.handoff_secret = Some("test-secret".into());
        let app = crate::build(cfg, &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("routes/default.toml")).unwrap();
        let now = chrono::Utc::now().timestamp();
        let headers = signed_headers("test-secret", "delivery-1", "push", b"{}", now);
        assert!(verify_handoff(&app, &headers, b"{}").is_ok());
        assert_eq!(verify_handoff(&app, &headers, b"{\"tampered\":true}"), Err("body digest mismatch"));
    }

    #[test]
    fn stale_handoff_is_rejected() {
        let mut cfg = Config::default();
        cfg.storage.data_dir = std::env::temp_dir().join(format!("antenna-github-stale-{}", uuid::Uuid::new_v4()));
        cfg.github.handoff_secret = Some("test-secret".into());
        cfg.github.handoff_max_age_secs = 10;
        let app = crate::build(cfg, &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("routes/default.toml")).unwrap();
        let old = chrono::Utc::now().timestamp() - 30;
        let headers = signed_headers("test-secret", "delivery-1", "push", b"{}", old);
        assert_eq!(verify_handoff(&app, &headers, b"{}"), Err("stale handoff"));
    }
}
