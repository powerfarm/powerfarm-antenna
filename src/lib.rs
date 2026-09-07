//! Antenna — a thin, durable ingress/egress membrane.
//!
//!   receive faithfully · preserve durably · route cheaply
//!   delegate intelligently · authorize explicitly · return reliably

pub mod app;
pub mod capability;
pub mod census;
pub mod config;
pub mod delivery;
pub mod gatekeeper;
pub mod ids;
pub mod ingress;
pub mod journey;
pub mod observation;
pub mod receipt;
pub mod reconcile;
pub mod returnpath;
pub mod router;
pub mod run;
pub mod sheet;
pub mod storage;
pub mod telemetry;

use anyhow::{Context, Result};
use app::{App, AppInner};
use axum::routing::{get, post};
use axum::Router;
use config::Config;
use std::path::Path;
use std::sync::Arc;

/// Assemble the organism. SQLite and the bucket initialize themselves; no
/// model is loaded, and none is required.
pub fn build(cfg: Config, routes_path: &Path) -> Result<App> {
    std::fs::create_dir_all(&cfg.storage.data_dir)
        .with_context(|| format!("creating data dir {}", cfg.storage.data_dir.display()))?;

    let db = storage::Db::open(&cfg.db_path()).context("opening sqlite")?;
    let bucket = storage::Bucket::open(&cfg.objects_path()).context("opening bucket")?;
    let router = router::Router::load(routes_path).context("loading routes")?;

    let mut caps = capability::Registry::new();
    caps.register(Arc::new(capability::builtins::Echo));
    caps.register(Arc::new(capability::builtins::DocumentInspect));
    caps.register(Arc::new(capability::builtins::ObjectStore));
    caps.register(Arc::new(capability::builtins::DeliveryCreate));
    caps.register(Arc::new(capability::builtins::CapabilitiesList));
    caps.register(Arc::new(capability::builtins::ReceiptsQuery));
    caps.register(Arc::new(capability::builtins::McpDispatch));
    caps.register(Arc::new(capability::talent::IngressInterpreter));

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(cfg.delivery.http_timeout_ms))
        .user_agent(concat!("antenna/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building http client")?;

    Ok(App::new(AppInner {
        cfg,
        router,
        db,
        bucket,
        caps,
        returns: returnpath::ReturnPaths::new(),
        outbox_notify: tokio::sync::Notify::new(),
        http,
    }))
}

/// One router shared by every ingress form (spec §24).
pub fn http_router(app: App) -> Router {
    use tower_http::limit::RequestBodyLimitLayer;

    let max_event = app.cfg.server.max_event_bytes;

    // /blob streams to disk and enforces its own cap, so the buffering limit
    // layer must not apply to it. Everything else is bounded here.
    let bounded = Router::new()
        .route("/ingress", post(ingress::http::ingress))
        .route("/internal/github", post(ingress::github::accept))
        .route("/mcp", post(ingress::mcp::post).get(ingress::mcp::get_not_allowed))
        .layer(RequestBodyLimitLayer::new(max_event));

    let streaming = Router::new()
        .route("/blob", post(ingress::http::blob))
        .layer(axum::extract::DefaultBodyLimit::disable());

    let inspect = Router::new()
        .route("/health", get(ingress::http::health))
        .route("/status", get(ingress::http::operational_status))
        .route("/receipts", get(ingress::http::list_receipts))
        .route("/receipts/{id}", get(ingress::http::get_receipt))
        .route("/receipts/{id}/raw", get(ingress::http::get_receipt_raw))
        .route("/deliveries", get(ingress::http::list_deliveries))
        .route("/ws", get(ingress::websocket::upgrade));

    bounded
        .merge(streaming)
        .merge(inspect)
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app)
}
