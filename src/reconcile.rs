//! Deterministic reconciliation of append-only evidence into current
//! operational projections. This module never writes Target, principle,
//! authority, or constitutional fields.

use crate::observation::sha256_hex;
use crate::storage::Db;
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};

const FORBIDDEN_KEYS: &[&str] = &[
    "target",
    "target_requirement",
    "governing_principle",
    "constitutional_decision",
    "authority",
    "institutional_membership",
];

#[derive(Debug, Clone)]
pub struct ReconciliationResult {
    pub run_id: String,
    pub observation_count: i64,
    pub meaningful_count: i64,
    pub unknown_count: i64,
    pub projection_item_count: i64,
}

pub fn projection_is_safe(value: &Value) -> bool {
    let Some(object) = value.as_object() else { return false };
    object.keys().all(|key| {
        let normalized = key.to_ascii_lowercase().replace([' ', '-'], "_");
        !FORBIDDEN_KEYS.iter().any(|forbidden| normalized == *forbidden)
    })
}

pub async fn run(
    db: &Db,
    trigger_kind: &str,
    trigger_identity: Option<&str>,
) -> Result<ReconciliationResult> {
    let run_id = crate::ids::new_id("rec");
    let started_at = crate::ids::now_rfc3339();
    let run_id_out = run_id.clone();
    let trigger_kind = trigger_kind.to_string();
    let trigger_identity = trigger_identity.map(str::to_string);

    db.call(move |conn| {
        let tx = conn.transaction().context("beginning reconciliation")?;
        tx.execute(
            "INSERT INTO reconciliation_runs(id,trigger_kind,trigger_identity,started_at,status)
             VALUES (?1,?2,?3,?4,'running')",
            params![run_id, trigger_kind, trigger_identity, started_at],
        )?;

        let (observation_count, meaningful_count, unknown_count): (i64, i64, i64) = tx.query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN kind NOT IN ('repository_activity','transport_ping') THEN 1 ELSE 0 END),0),
                    COALESCE(SUM(CASE WHEN classification='UNKNOWN' THEN 1 ELSE 0 END),0)
             FROM observations WHERE reconciliation_status='pending'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let registry_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM repository_registry WHERE presence_state='present'",
            [],
            |row| row.get(0),
        )?;
        let unresolved_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM repository_registry
             WHERE membership_treatment='unresolved' AND presence_state='present'",
            [],
            |row| row.get(0),
        )?;
        let total_unknown: i64 = tx.query_row(
            "SELECT COUNT(*) FROM observations WHERE classification='UNKNOWN'",
            [],
            |row| row.get(0),
        )?;
        let last_census: Option<(String, i64)> = tx.query_row(
            "SELECT completed_at, repository_count FROM census_runs
             WHERE status='completed' AND complete=1
             ORDER BY completed_at DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;

        tx.execute(
            "UPDATE observations SET reconciliation_status='reconciled',reconciliation_run_id=?1
             WHERE reconciliation_status='pending'",
            params![run_id],
        )?;

        let now = crate::ids::now_rfc3339();
        let mut projections = Vec::new();
        // A successful unchanged census is operational evidence in census_runs,
        // but it is not a meaningful projection change. Keeping volatile run
        // timestamps out of that case prevents hourly no-op Sheet rewrites.
        if meaningful_count > 0 {
            projections.push(("LB-004".to_string(), json!({
                "observed_state": format!("{} repositories currently observed in powerfarm scope", registry_count),
                "attention_state": if unresolved_count > 0 { "membership treatment unresolved" } else { "observed" },
                "last_observed_at": last_census.as_ref().map(|c| c.0.clone()).unwrap_or_else(|| now.clone()),
                "machine_source": "LAB-8GB Antenna"
            })));
            projections.push(("LB-005".to_string(), json!({
                "observed_state": format!("{} UNKNOWN observations retained", total_unknown),
                "attention_state": if total_unknown > 0 { "interpretation required" } else { "clear" },
                "last_observed_at": now,
                "machine_source": "LAB-8GB Antenna"
            })));
            projections.push(("LB-006".to_string(), json!({
                "observed_state": format!("reconciliation {} completed; {} meaningful observations", run_id, meaningful_count),
                "attention_state": if unknown_count > 0 || unresolved_count > 0 { "treatment required" } else { "reconciled" },
                "last_observed_at": now,
                "machine_source": "LAB-8GB Antenna"
            })));
            projections.push(("LB-007".to_string(), json!({
                "observed_state": "Antenna durable GitHub observation substrate healthy",
                "attention_state": "operational observation",
                "last_observed_at": now,
                "machine_source": "LAB-8GB Antenna"
            })));
            projections.push(("LB-008".to_string(), json!({
                "observed_state": "verified edge handoff to durable Antenna receipt deployed",
                "attention_state": "deployment observation",
                "last_observed_at": now,
                "machine_source": "LAB-8GB Antenna"
            })));

            let mut stmt = tx.prepare(
                "SELECT repository_id,full_name,visibility,archived,default_branch,
                        presence_state,membership_treatment,drift_state,last_observed_at
                 FROM repository_registry ORDER BY repository_id",
            )?;
            let rows = stmt.query_map([], |row| {
                let repo_id: i64 = row.get(0)?;
                let full_name: String = row.get(1)?;
                let visibility: Option<String> = row.get(2)?;
                let archived = row.get::<_, Option<i64>>(3)?.map(|value| value != 0);
                let default_branch: Option<String> = row.get(4)?;
                let presence: String = row.get(5)?;
                let membership: String = row.get(6)?;
                let drift: String = row.get(7)?;
                let last_observed_at: String = row.get(8)?;
                let observed_state = format!(
                    "GitHub repository ID {repo_id}; visibility {}; archived {}; default branch {}; presence {presence}",
                    visibility.as_deref().unwrap_or("UNKNOWN"),
                    archived
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "UNKNOWN".into()),
                    default_branch.as_deref().unwrap_or("UNKNOWN"),
                );
                Ok((format!("GH-REPO-{repo_id}"), json!({
                    "observed_repository_id": repo_id,
                    "observed_repository_name": full_name,
                    "observed_visibility": visibility,
                    "observed_archived": archived,
                    "observed_default_branch": default_branch,
                    "observed_presence": presence,
                    "observed_state": observed_state,
                    "membership_treatment": membership,
                    "attention_state": drift,
                    "last_observed_at": last_observed_at,
                    "machine_source": "LAB-8GB Antenna"
                })))
            })?;
            for row in rows { projections.push(row?); }
        }

        let mut projection_item_count = 0i64;
        for (record_id, desired) in projections {
            if !projection_is_safe(&desired) {
                anyhow::bail!("unsafe projection for {record_id}");
            }
            let desired_json = serde_json::to_string(&desired)?;
            let desired_sha = sha256_hex(desired_json.as_bytes());
            let changed = tx.execute(
                "INSERT INTO projection_records(
                    record_id,desired_json,desired_sha256,reconciliation_run_id,status,updated_at
                 ) VALUES (?1,?2,?3,?4,'pending',?5)
                 ON CONFLICT(record_id) DO UPDATE SET
                    desired_json=excluded.desired_json,
                    desired_sha256=excluded.desired_sha256,
                    reconciliation_run_id=excluded.reconciliation_run_id,
                    status='pending',mutation_id=NULL,updated_at=excluded.updated_at,
                    completed_at=NULL,last_error=NULL
                 WHERE projection_records.desired_sha256<>excluded.desired_sha256",
                params![record_id, desired_json, desired_sha, run_id, now],
            )?;
            projection_item_count += changed as i64;
        }

        let completed_at = crate::ids::now_rfc3339();
        let result_json = json!({
            "observations": observation_count,
            "meaningful": meaningful_count,
            "unknown": unknown_count,
            "registry_current": registry_count,
            "membership_unresolved": unresolved_count,
            "projection_items_changed": projection_item_count
        }).to_string();
        tx.execute(
            "UPDATE reconciliation_runs SET completed_at=?2,status='completed',
                observation_count=?3,meaningful_count=?4,unknown_count=?5,
                projection_item_count=?6,result_json=?7 WHERE id=?1",
            params![run_id, completed_at, observation_count, meaningful_count,
                    unknown_count, projection_item_count, result_json],
        )?;
        tx.commit()?;
        Ok(ReconciliationResult {
            run_id,
            observation_count,
            meaningful_count,
            unknown_count,
            projection_item_count,
        })
    }).await.map(|mut result| {
        result.run_id = run_id_out;
        result
    })
}
