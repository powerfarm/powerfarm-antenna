mod common;

use antenna::census::{CensusInput, CensusResult};
use antenna::observation::RepositorySnapshot;
use axum::{extract::{Query, State}, routing::get, Json, Router};
use serde_json::{json, Value};
use std::collections::HashMap;

fn repo(id: i64, name: &str, archived: bool) -> RepositorySnapshot {
    RepositorySnapshot {
        id,
        full_name: name.into(),
        owner_login: Some("powerfarm".into()),
        visibility: Some("private".into()),
        archived: Some(archived),
        default_branch: Some("main".into()),
        html_url: Some(format!("https://github.com/{name}")),
        github_updated_at: Some("2026-09-03T00:00:00Z".into()),
    }
}

async fn record(h: &common::Harness, repositories: Vec<RepositorySnapshot>, complete: bool) -> CensusResult {
    antenna::census::record(&h.app.db, CensusInput {
        organization: "powerfarm".into(), installation_id: Some(99),
        started_at: antenna::ids::now_rfc3339(), repositories,
        page_count: 1, api_rate_remaining: Some(4999), api_rate_reset: None,
        complete, error: (!complete).then(|| "simulated API failure".into()),
    }).await.unwrap()
}

#[tokio::test]
async fn repository_rename_preserves_identity_and_archive_round_trips() {
    let h = common::start("census-identity", &[]).await;
    record(&h, vec![repo(100, "powerfarm/old-name", false)], true).await;
    record(&h, vec![repo(100, "powerfarm/new-name", true)], true).await;
    record(&h, vec![repo(100, "powerfarm/new-name", false)], true).await;
    let state: (i64, String, i64, i64) = h.app.db.with(|c| Ok(c.query_row(
        "SELECT repository_id,full_name,archived,(SELECT COUNT(*) FROM repository_registry) FROM repository_registry WHERE repository_id=100",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?)).unwrap();
    assert_eq!(state, (100, "powerfarm/new-name".into(), 0, 1));
}

#[tokio::test]
async fn appearing_and_missing_repositories_are_observations_not_inferred_deletions() {
    let h = common::start("census-missing", &[]).await;
    let first = record(&h, vec![repo(101, "powerfarm/appeared", false)], true).await;
    assert_eq!(first.observations_emitted, 1);
    let second = record(&h, Vec::new(), true).await;
    assert_eq!(second.observations_emitted, 1);
    let state: (String, String) = h.app.db.with(|c| Ok(c.query_row(
        "SELECT presence_state,drift_state FROM repository_registry WHERE repository_id=101",
        [], |r| Ok((r.get(0)?,r.get(1)?)))?)).unwrap();
    assert_eq!(state.0, "not_observed_in_latest_complete_census");
    assert!(!state.0.contains("deleted"));
}

#[tokio::test]
async fn failed_census_leaves_prior_projection_intact() {
    let h = common::start("census-failure", &[]).await;
    record(&h, vec![repo(102, "powerfarm/stable", false)], true).await;
    let failed = record(&h, Vec::new(), false).await;
    assert!(!failed.complete);
    let presence: String = h.app.db.with(|c| Ok(c.query_row(
        "SELECT presence_state FROM repository_registry WHERE repository_id=102", [], |r| r.get(0))?)).unwrap();
    assert_eq!(presence, "present");
}

#[tokio::test]
async fn unchanged_complete_census_does_not_rewrite_sheet_projection() {
    let h = common::start("census-coalescing", &[]).await;
    let first = record(&h, vec![repo(103, "powerfarm/stable", false)], true).await;
    let first_reconciliation = antenna::reconcile::run(&h.app.db, "census", Some(&first.run_id))
        .await
        .unwrap();
    assert!(first_reconciliation.projection_item_count > 0);

    let second = record(&h, vec![repo(103, "powerfarm/stable", false)], true).await;
    assert_eq!(second.observations_emitted, 0);
    let second_reconciliation = antenna::reconcile::run(&h.app.db, "census", Some(&second.run_id))
        .await
        .unwrap();
    assert_eq!(second_reconciliation.observation_count, 0);
    assert_eq!(second_reconciliation.projection_item_count, 0);
}

async fn pages(
    State(all): State<Vec<Value>>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let page: usize = query.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let start = (page - 1) * 100;
    let end = (start + 100).min(all.len());
    let repositories = if start < all.len() { all[start..end].to_vec() } else { Vec::new() };
    Json(json!({"total_count": all.len(), "repositories": repositories}))
}

#[tokio::test]
async fn census_fetches_every_page_and_proves_completeness() {
    let all: Vec<Value> = (0..101).map(|i| json!({
        "id": 10_000 + i,
        "full_name": format!("powerfarm/repo-{i}"),
        "owner": {"login":"powerfarm"},
        "visibility":"private", "archived":false, "default_branch":"main"
    })).collect();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/installation/repositories", get(pages)).with_state(all)).await.unwrap();
    });
    let (repositories, pages, _, _) = antenna::census::fetch_all_repositories(
        &reqwest::Client::new(), &format!("http://{addr}"), "test-token", "powerfarm"
    ).await.unwrap();
    assert_eq!(repositories.len(), 101);
    assert_eq!(pages, 2);
}

#[test]
fn migration_preserves_existing_receipts() {
    let dir = common::temp_dir("migration-existing");
    let db_path = dir.join("antenna.db");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(include_str!("../migrations/0001_init.sql")).unwrap();
        conn.execute(
            "INSERT INTO receipts(id,recorded_at,transport,raw_storage,interaction,status)
             VALUES ('legacy','2026-09-03T00:00:00Z','http','none','event','accepted')", [],
        ).unwrap();
    }
    let db = antenna::storage::Db::open(&db_path).unwrap();
    let counts: (i64, i64) = db.with(|c| Ok((
        c.query_row("SELECT COUNT(*) FROM receipts WHERE id='legacy'", [], |r| r.get(0))?,
        c.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='observations'", [], |r| r.get(0))?,
    ))).unwrap();
    assert_eq!(counts, (1, 1));
}
