//! Identity and time. Small on purpose.

use chrono::{DateTime, SecondsFormat, Utc};

/// Sortable, prefixed id. UUIDv7 keeps rows in arrival order in SQLite.
pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::now_v7().simple())
}

/// 16 bytes of hex — a W3C trace-context trace-id.
pub fn new_trace_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// 8 bytes of hex — a W3C trace-context span-id.
pub fn new_span_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_string()
}

/// Parse an inbound `traceparent` so an external journey keeps its trace.
/// Format: `00-<32 hex>-<16 hex>-<flags>`.
pub fn trace_id_from_traceparent(header: &str) -> Option<String> {
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() < 4 || parts[1].len() != 32 || !parts[1].chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if parts[1].chars().all(|c| c == '0') {
        return None;
    }
    Some(parts[1].to_ascii_lowercase())
}

pub fn traceparent(trace_id: &str, span_id: &str) -> String {
    format!("00-{trace_id}-{span_id}-01")
}

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

pub fn now_rfc3339() -> String {
    now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub fn in_ms(ms: i64) -> String {
    rfc3339(now() + chrono::Duration::milliseconds(ms))
}
