//! Shared harness: a real Antenna on an ephemeral port, over a temp data dir.

#![allow(dead_code)]

use antenna::config::Config;
use std::net::SocketAddr;
use std::path::PathBuf;

pub struct Harness {
    pub addr: SocketAddr,
    pub base: String,
    pub dir: PathBuf,
    pub app: antenna::app::App,
}

pub fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "antenna-test-{tag}-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub fn config_for(dir: &PathBuf, allow: &[&str]) -> Config {
    let mut cfg = Config::default();
    cfg.storage.data_dir = dir.clone();
    cfg.server.call_timeout_ms = 10_000;
    cfg.delivery.worker_interval_ms = 200;
    cfg.delivery.base_backoff_ms = 50;
    cfg.gatekeeper.allow_destinations = allow.iter().map(|s| s.to_string()).collect();
    cfg
}

pub fn routes_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("routes/default.toml")
}

/// Boot Antenna against `dir`, recovering first, exactly as main does.
pub async fn start_in(dir: PathBuf, allow: &[&str]) -> Harness {
    let cfg = config_for(&dir, allow);
    start_with_config(dir, cfg).await
}

pub async fn start_with_config(dir: PathBuf, cfg: Config) -> Harness {
    let app = antenna::build(cfg, &routes_path()).expect("build antenna");
    antenna::journey::recover(app.clone()).await.expect("recover");
    tokio::spawn(antenna::delivery::worker(app.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = antenna::http_router(app.clone());
    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
    });

    Harness { addr, base: format!("http://{addr}"), dir, app }
}

pub async fn start(tag: &str, allow: &[&str]) -> Harness {
    start_in(temp_dir(tag), allow).await
}

pub fn client() -> reqwest::Client {
    reqwest::Client::new()
}
