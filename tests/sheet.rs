use antenna::reconcile::projection_is_safe;
use antenna::sheet::{field_map_is_safe, resolve_record_rows};
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn stable_record_id_resolution_is_not_row_memory() {
    let resolved = resolve_record_rows(&[
        json!("LB-004"), json!("unrelated"), json!("LB-005")
    ]).unwrap();
    assert_eq!(resolved["LB-004"], 2);
    assert_eq!(resolved["LB-005"], 4);
}

#[test]
fn target_and_constitutional_fields_are_forbidden() {
    assert!(projection_is_safe(&json!({"observed_state":"eight repositories"})));
    assert!(!projection_is_safe(&json!({"Target Requirement":"rewrite me"})));
    assert!(!projection_is_safe(&json!({"authority":"machine"})));

    let mut safe = BTreeMap::new();
    safe.insert("observed_state".into(), "Observed state".into());
    assert!(field_map_is_safe(&safe));
    safe.insert("attention_state".into(), "Governing Principles".into());
    assert!(!field_map_is_safe(&safe));
}

#[test]
fn duplicate_record_ids_are_rejected() {
    assert!(resolve_record_rows(&[json!("LB-004"), json!("LB-004")]).is_err());
}

#[tokio::test]
async fn google_failure_leaves_projection_safely_pending() {
    let dir = common::temp_dir("sheet-failure");
    let mut cfg = common::config_for(&dir, &[]);
    cfg.google.enabled = true;
    cfg.google.credentials_path = Some(dir.join("missing-service-account.json"));
    cfg.google.spreadsheet_id = Some("canonical-sheet".into());
    cfg.google.sheet_name = Some("Blueprint".into());
    cfg.google.field_map.insert("observed_state".into(), "Observed state".into());
    let h = common::start_with_config(dir, cfg).await;
    h.app.db.with(|c| {
        c.execute(
            "INSERT INTO observations(
                id,evidence_key,source,source_identity,observed_at,kind,classification,
                normalized_metadata,reconciliation_status
             ) VALUES ('obs-test','evidence-test','test','test','2026-09-03T00:00:00Z',
                'unknown_github_event','UNKNOWN','{}','pending')", [],
        )?;
        Ok(())
    }).unwrap();
    antenna::reconcile::run(&h.app.db, "test", Some("evidence-test")).await.unwrap();
    assert!(antenna::sheet::project_once(&h.app).await.is_err());
    let pending: i64 = h.app.db.with(|c| Ok(c.query_row(
        "SELECT COUNT(*) FROM projection_records WHERE status='pending'", [], |r| r.get(0))?)).unwrap();
    assert!(pending > 0);
}

#[tokio::test]
async fn repeated_reconciliation_without_new_evidence_is_idempotent() {
    let h = common::start("reconcile-idempotent", &[]).await;
    h.app.db.with(|c| {
        c.execute(
            "INSERT INTO observations(
                id,evidence_key,source,source_identity,observed_at,kind,classification,
                normalized_metadata,reconciliation_status
             ) VALUES ('obs-once','evidence-once','test','test','2026-09-03T00:00:00Z',
                'unknown_github_event','UNKNOWN','{}','pending')", [],
        )?;
        Ok(())
    }).unwrap();
    let first = antenna::reconcile::run(&h.app.db, "test", Some("once")).await.unwrap();
    let second = antenna::reconcile::run(&h.app.db, "test", Some("twice")).await.unwrap();
    assert!(first.projection_item_count > 0);
    assert_eq!(second.observation_count, 0);
    assert_eq!(second.projection_item_count, 0);
}
mod common;
