//! One binary. It starts, it initializes its own storage, it recovers whatever
//! the last run left unfinished, and only then does it open the door.

use anyhow::{Context, Result};
use antenna::{build, config::Config, http_router, journey};
use std::net::SocketAddr;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    antenna::telemetry::init();

    let config_path = std::env::var("ANTENNA_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("antenna.toml"));
    let routes_path = std::env::var("ANTENNA_ROUTES")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("routes/default.toml"));

    let cfg = if config_path.exists() {
        Config::load(&config_path)?
    } else {
        tracing::warn!(path = %config_path.display(), "no config file; using defaults");
        Config::default()
    };

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        data_dir = %cfg.storage.data_dir.display(),
        bind = %cfg.server.bind,
        "mounting antenna"
    );

    let bind: SocketAddr = cfg.server.bind.parse()
        .with_context(|| format!("parsing bind address {:?}", cfg.server.bind))?;

    let app = build(cfg, &routes_path)?;
    if app.cfg.services.enabled {
        // A recovered service run must consult current authority before resuming.
        if let Err(error)=antenna::services::refresh(&app).await {
            tracing::warn!(error=%error,"contract authority unavailable; contracted work waits for refresh");
        }
        tokio::spawn(antenna::services::worker(app.clone()));
    }

    // Recover BEFORE serving. Work accepted by a previous process is finished
    // (or honestly marked uncertain) before new work is admitted.
    journey::recover(app.clone()).await.context("crash recovery")?;

    tokio::spawn(antenna::delivery::worker(app.clone()));
    if app.cfg.github.census_enabled {
        tokio::spawn(antenna::census::worker(app.clone()));
    }
    if app.cfg.google.enabled {
        tokio::spawn(antenna::sheet::worker(app.clone()));
    }

    let listener = tokio::net::TcpListener::bind(bind).await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(addr = %listener.local_addr()?, "antenna is listening");

    axum::serve(
        listener,
        http_router(app).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown())
    .await
    .context("serving")?;

    tracing::info!("antenna stopped");
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.expect("install ctrl-c handler") };
    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("SIGINT received; draining"),
        _ = term  => tracing::info!("SIGTERM received; draining"),
    }
}
