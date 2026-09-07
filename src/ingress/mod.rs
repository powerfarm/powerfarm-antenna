//! Ingress profiles. Three doors, one corridor.
//!
//! Each module's only job is to turn a transport's arrival into a NewReceipt
//! and hand it to `journey`. No module here interprets a payload, and none of
//! them may cause an effect: that is the router's and the outbox's business.

pub mod http;
pub mod github;
pub mod mcp;
pub mod websocket;

use axum::http::HeaderMap;

/// Bound what we will believe from the transport (spec §4).
pub fn source_of(headers: &HeaderMap, peer: Option<std::net::SocketAddr>) -> String {
    let via = headers
        .get("cf-connecting-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string());
    match (via, peer) {
        (Some(ip), Some(p)) => format!("{ip} (via {p})"),
        (Some(ip), None) => ip,
        (None, Some(p)) => p.to_string(),
        (None, None) => "unknown".into(),
    }
}

pub fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

/// Continue an external trace where one was offered; otherwise start one.
pub fn trace_id_from(headers: &HeaderMap) -> String {
    header(headers, "traceparent")
        .as_deref()
        .and_then(crate::ids::trace_id_from_traceparent)
        .unwrap_or_else(crate::ids::new_trace_id)
}

/// What Antenna is willing to say about who is calling. Cloudflare Access
/// puts a verified identity here when a hostname is behind a policy; without
/// that we record the absence rather than inventing a principal.
pub fn principal_of(headers: &HeaderMap) -> Option<String> {
    header(headers, "cf-access-authenticated-user-email")
}

/// Guard the inspection surface.
///
/// Antenna is reachable from the world by design, so the endpoints that
/// REPLAY what the world sent must not be. Constant-time compare: this is a
/// secret, and a timing oracle on it is still a leak.
pub fn inspection_allowed(app: &crate::app::App, headers: &HeaderMap) -> bool {
    let Some(expected) = app.cfg.server.inspect_token.as_deref() else {
        return true; // no token configured: open, for loopback-only installs
    };
    let Some(offered) = header(headers, "authorization") else { return false };
    let Some(offered) = offered.strip_prefix("Bearer ").map(str::to_string) else { return false };

    let a = offered.as_bytes();
    let b = expected.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
