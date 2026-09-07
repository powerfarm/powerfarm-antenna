//! Logs are not the source of truth. SQLite is. (spec §20)
//!
//! We emit tracing spans carrying W3C trace-context ids, and store those same
//! ids on the durable rows, so a journey can be reassembled from either side.

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init() {
    let filter = EnvFilter::try_from_env("ANTENNA_LOG")
        .unwrap_or_else(|_| EnvFilter::new("antenna=info,tower_http=warn,info"));

    let json = std::env::var("ANTENNA_LOG_FORMAT").as_deref() == Ok("json");

    let registry = tracing_subscriber::registry().with(filter);
    if json {
        registry.with(fmt::layer().json().with_current_span(true)).init();
    } else {
        registry.with(fmt::layer().compact()).init();
    }
}

/// The telemetry stamp written onto receipts / runs / deliveries.
pub fn stamp(trace_id: &str, span_id: &str) -> String {
    serde_json::json!({
        "trace_id": trace_id,
        "span_id": span_id,
        "traceparent": crate::ids::traceparent(trace_id, span_id),
    })
    .to_string()
}
