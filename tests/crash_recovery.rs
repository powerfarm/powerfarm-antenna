//! Spec §19. Crash testing is part of construction, not polish.
//!
//!   1. receive request      5. discover unfinished work
//!   2. commit Receipt       6. continue processing
//!   3. SIGKILL Antenna      7. produce Delivery
//!   4. restart              8. avoid duplicate external effect

mod common;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Sink {
    hits: Arc<AtomicUsize>,
    keys: Arc<Mutex<Vec<String>>>,
}

async fn sink_handler(State(s): State<Sink>, headers: HeaderMap, body: String) -> &'static str {
    let _ = body;
    s.hits.fetch_add(1, Ordering::SeqCst);
    if let Some(k) = headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
        s.keys.lock().unwrap().push(k.to_string());
    }
    "ok"
}

/// A stand-in for "the world". It counts every request it receives.
async fn start_sink() -> (Sink, u16) {
    let sink = Sink::default();
    let app = axum::Router::new()
        .route("/sink", post(sink_handler))
        .with_state(sink.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    (sink, port)
}

fn write_config(dir: &std::path::Path, bind_port: u16) -> std::path::PathBuf {
    let cfg = format!(
        r#"
[server]
bind = "127.0.0.1:{bind_port}"
[storage]
data_dir = "{}"
[delivery]
worker_interval_ms = 200
base_backoff_ms = 50
[gatekeeper]
allow_destinations = ["127.0.0.1"]
[talent]
enabled = false
"#,
        dir.display()
    );
    let p = dir.join("antenna.toml");
    std::fs::write(&p, cfg).unwrap();
    p
}

fn spawn_antenna(config: &std::path::Path) -> std::process::Child {
    std::process::Command::new(env!("CARGO_BIN_EXE_antenna"))
        .env("ANTENNA_CONFIG", config)
        .env(
            "ANTENNA_ROUTES",
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("routes/crash-test.toml"),
        )
        .env("ANTENNA_LOG", "antenna=info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn antenna")
}

async fn wait_alive(base: &str) {
    for _ in 0..200 {
        if common::client().get(format!("{base}/health")).send().await
            .map(|r| r.status().is_success()).unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("antenna did not come up at {base}");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_work_survives_sigkill_and_is_delivered_exactly_once() {
    let (sink, sink_port) = start_sink().await;
    let dir = common::temp_dir("crash");
    let port = free_port();
    let config = write_config(&dir, port);
    let base = format!("http://127.0.0.1:{port}");

    // ---- 1 & 2. receive, and commit the Receipt -------------------------
    let mut child = spawn_antenna(&config);
    wait_alive(&base).await;

    let accepted: serde_json::Value = common::client()
        .post(format!("{base}/ingress?wait=0"))
        .json(&json!({
            "destination": format!("http://127.0.0.1:{sink_port}/sink"),
            "payload": { "work": "must happen exactly once" }
        }))
        .send().await.unwrap().json().await.unwrap();

    let receipt_id = accepted["receipt_id"].as_str().unwrap().to_string();
    assert_eq!(accepted["status"], "accepted");

    // ---- 3. SIGKILL. No draining, no cleanup, no chance to finish. ------
    // Straight after acknowledgement, which is the worst moment: the receipt
    // is committed but the outward effect may or may not have started.
    unsafe { libc_kill(child.id() as i32) };
    let _ = child.wait();

    // ---- 4. restart -----------------------------------------------------
    let mut child2 = spawn_antenna(&config);
    wait_alive(&base).await;

    // ---- 5, 6 & 7. discover, continue, deliver --------------------------
    let mut delivered = false;
    for _ in 0..100 {
        if sink.hits.load(Ordering::SeqCst) > 0 { delivered = true; break; }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(delivered, "work accepted before the crash was never delivered after restart");

    // Let any duplicate that is going to happen, happen.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // ---- 8. exactly once ------------------------------------------------
    let hits = sink.hits.load(Ordering::SeqCst);
    assert_eq!(hits, 1, "the external effect must happen exactly once, saw {hits}");

    let keys = sink.keys.lock().unwrap().clone();
    assert!(
        keys[0].starts_with(&format!("proposed:{receipt_id}")),
        "the idempotency key must be derived from the receipt, not the run: {:?}", keys[0]
    );

    // The receipt is settled and the delivery is completed, in the record.
    let one: serde_json::Value = common::client()
        .get(format!("{base}/receipts/{receipt_id}")).send().await.unwrap().json().await.unwrap();
    assert_eq!(one["receipt"]["status"], "routed");

    let deliveries: serde_json::Value = common::client()
        .get(format!("{base}/deliveries?limit=50")).send().await.unwrap().json().await.unwrap();
    let d = deliveries["deliveries"].as_array().unwrap().iter()
        .find(|d| d["destination"].as_str().unwrap().contains("/sink"))
        .expect("the delivery must exist in the record");
    assert_eq!(d["status"], "completed");

    let _ = child2.kill();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_delivery_is_marked_uncertain_not_forgotten() {
    // A delivery left in_flight by a crash: we do not know whether the world
    // saw it. Antenna must say so rather than guess.
    let dir = common::temp_dir("uncertain");
    {
        let db = antenna::storage::Db::open(&dir.join("antenna.db")).unwrap();
        db.with(|c| {
            c.execute(
                "INSERT INTO receipts (id,recorded_at,transport,raw_storage,interaction,status)
                 VALUES ('rcp_x', ?1, 'http', 'none', 'event', 'routed')",
                rusqlite::params![antenna::ids::now_rfc3339()],
            )?;
            c.execute(
                "INSERT INTO deliveries
                   (id,receipt_id,destination,transport,idempotency_key,status,
                    attempt,max_attempts,created_at)
                 VALUES ('dlv_x','rcp_x','http://127.0.0.1:1/sink','http','k1','in_flight',
                         1,5,?1)",
                rusqlite::params![antenna::ids::now_rfc3339()],
            )?;
            Ok(())
        }).unwrap();
    }

    let h = common::start_in(dir, &["127.0.0.1"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let (status, uncertain, attempts) = h.app.db.call(|c| {
        let r: (String, i64) = c.query_row(
            "SELECT status, resumed_uncertain FROM deliveries WHERE id='dlv_x'",
            [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let n: i64 = c.query_row(
            "SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id='dlv_x'", [], |r| r.get(0))?;
        Ok((r.0, r.1, n))
    }).await.unwrap();

    // It is retried (port 1 refuses, so it fails and reschedules)...
    assert_ne!(status, "in_flight", "a stale in_flight delivery must not stay stuck");
    // ...and the uncertainty is a durable flag on the row, not a log line that
    // the next attempt's error overwrites.
    assert_eq!(uncertain, 1, "the record must still say the prior outcome is unknown");
    assert!(attempts >= 1, "the retry must be recorded as an inspectable attempt");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receipt_committed_but_never_routed_is_routed_on_boot() {
    let dir = common::temp_dir("unrouted");
    {
        let db = antenna::storage::Db::open(&dir.join("antenna.db")).unwrap();
        db.with(|c| {
            c.execute(
                "INSERT INTO receipts (id,recorded_at,transport,content_type,raw_storage,
                                       raw_body,interaction,correlation_id,status)
                 VALUES ('rcp_orphan', ?1, 'http', 'application/json', 'sqlite',
                         ?2, 'event', 'cor_orphan', 'accepted')",
                rusqlite::params![antenna::ids::now_rfc3339(), br#"{"left":"unfinished"}"#.to_vec()],
            )?;
            Ok(())
        }).unwrap();
    }

    let h = common::start_in(dir, &[]).await;
    for _ in 0..50 {
        let one: serde_json::Value = common::client()
            .get(format!("{}/receipts/rcp_orphan", h.base))
            .send().await.unwrap().json().await.unwrap();
        if one["receipt"]["status"] != "accepted" {
            assert_eq!(one["runs"][0]["status"], "completed");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("a receipt committed but never routed was not picked up at boot");
}

/// SIGKILL. Deliberately the un-catchable one: no destructor, no flush, no
/// graceful shutdown path gets to run.
unsafe fn libc_kill(pid: i32) {
    extern "C" { fn kill(pid: i32, sig: i32) -> i32; }
    kill(pid, 9);
}
