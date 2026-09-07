//! GitHub transport evidence, append-only observations, and the current
//! repository projection. A verified delivery is evidence, never authority to
//! rewrite institutional Target state.

use crate::receipt::{self, NewReceipt, Status as ReceiptStatus};
use crate::storage::Db;
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositorySnapshot {
    pub id: i64,
    pub full_name: String,
    pub owner_login: Option<String>,
    pub visibility: Option<String>,
    pub archived: Option<bool>,
    pub default_branch: Option<String>,
    pub html_url: Option<String>,
    pub github_updated_at: Option<String>,
}

impl RepositorySnapshot {
    pub fn from_value(value: &Value) -> Option<Self> {
        Some(Self {
            id: value.get("id")?.as_i64()?,
            full_name: value.get("full_name")?.as_str()?.to_string(),
            owner_login: value.pointer("/owner/login").and_then(Value::as_str).map(str::to_string),
            visibility: value.get("visibility").and_then(Value::as_str).map(str::to_string),
            archived: value.get("archived").and_then(Value::as_bool),
            default_branch: value.get("default_branch").and_then(Value::as_str).map(str::to_string),
            html_url: value.get("html_url").and_then(Value::as_str).map(str::to_string),
            github_updated_at: value.get("updated_at").and_then(Value::as_str).map(str::to_string),
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeliveryEvidence {
    pub delivery_id: String,
    pub event_name: String,
    pub body: Vec<u8>,
    pub source: String,
    pub received_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AcceptResult {
    pub receipt_id: String,
    pub delivery_id: String,
    pub duplicate: bool,
    pub observation_id: Option<String>,
    pub classification: String,
    pub meaningful: bool,
}

#[derive(Debug)]
struct ParsedDelivery {
    action: Option<String>,
    installation_id: Option<i64>,
    repository: Option<RepositorySnapshot>,
    ref_name: Option<String>,
    before_sha: Option<String>,
    after_sha: Option<String>,
    classification: &'static str,
    kind: &'static str,
    meaningful: bool,
    normalized: Value,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn classify(event: &str, action: Option<&str>) -> (&'static str, &'static str, bool) {
    match (event, action) {
        ("repository", Some("created" | "deleted" | "archived" | "unarchived" | "renamed" | "transferred" | "publicized" | "privatized" | "edited")) =>
            ("KNOWN", "repository_lifecycle", true),
        ("push", None) => ("KNOWN", "repository_activity", false),
        ("ping", None) => ("KNOWN", "transport_ping", false),
        _ => ("UNKNOWN", "unknown_github_event", true),
    }
}

fn parse_delivery(event_name: &str, body: &[u8]) -> ParsedDelivery {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let action = parsed.as_ref()
        .and_then(|v| v.get("action"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let repository = parsed.as_ref()
        .and_then(|v| v.get("repository"))
        .and_then(RepositorySnapshot::from_value);
    let installation_id = parsed.as_ref()
        .and_then(|v| v.pointer("/installation/id"))
        .and_then(Value::as_i64);
    let ref_name = parsed.as_ref()
        .and_then(|v| v.get("ref"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let before_sha = parsed.as_ref()
        .and_then(|v| v.get("before"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let after_sha = parsed.as_ref()
        .and_then(|v| v.get("after"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let (classification, kind, meaningful) = classify(event_name, action.as_deref());
    let sender = parsed.as_ref().and_then(|v| v.get("sender")).map(|v| json!({
        "id": v.get("id").and_then(Value::as_i64),
        "login": v.get("login").and_then(Value::as_str),
    }));
    let normalized = json!({
        "event": event_name,
        "action": action,
        "installation_id": installation_id,
        "repository": repository,
        "ref": ref_name,
        "before_sha": before_sha,
        "after_sha": after_sha,
        "sender": sender,
        "json_valid": parsed.is_some(),
    });
    ParsedDelivery {
        action,
        installation_id,
        repository,
        ref_name,
        before_sha,
        after_sha,
        classification,
        kind,
        meaningful,
        normalized,
    }
}

pub async fn accept_github_delivery(db: &Db, evidence: DeliveryEvidence) -> Result<AcceptResult> {
    let receipt_id = crate::ids::new_id("rcp");
    let observation_id = crate::ids::new_id("obs");
    let body_sha256 = sha256_hex(&evidence.body);
    let parsed = parse_delivery(&evidence.event_name, &evidence.body);
    let receipt_id_out = receipt_id.clone();
    let delivery_id_out = evidence.delivery_id.clone();
    let classification_out = parsed.classification.to_string();
    let meaningful = parsed.meaningful;

    db.call(move |conn| {
        let tx = conn.transaction().context("beginning GitHub receipt transaction")?;
        let new_receipt = NewReceipt {
            scope: Some("github:powerfarm".into()),
            actor: Some("github".into()),
            principal: Some("cloudflare:verified-edge".into()),
            provenance: Some("github-webhook".into()),
            transport: "github-edge".into(),
            source: Some(evidence.source.clone()),
            content_type: Some("application/json".into()),
            content_length: Some(evidence.body.len() as i64),
            raw_storage: "sqlite".into(),
            raw_body: Some(evidence.body.clone()),
            raw_digest: Some(body_sha256.clone()),
            interaction: "event".into(),
            correlation_id: Some(evidence.delivery_id.clone()),
            route_hint: Some("github.observation".into()),
            relationships: Some(json!({"github_delivery_id": evidence.delivery_id}).to_string()),
            telemetry: Some(json!({"verification": "edge_hmac_verified"}).to_string()),
            ..Default::default()
        };
        receipt::insert(&tx, &receipt_id, &evidence.received_at, &new_receipt)?;

        let repo_id = parsed.repository.as_ref().map(|r| r.id);
        let repo_name = parsed.repository.as_ref().map(|r| r.full_name.as_str());
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO github_deliveries (
                delivery_id, first_receipt_id, event_name, action, received_at,
                last_received_at, verification_status, body_sha256, body_size,
                installation_id, repository_id, repository_full_name, ref_name,
                before_sha, after_sha, classification, processing_status
             ) VALUES (?1,?2,?3,?4,?5,?5,'edge_hmac_verified',?6,?7,?8,?9,?10,?11,?12,?13,?14,'accepted')",
            params![
                evidence.delivery_id, receipt_id, evidence.event_name, parsed.action,
                evidence.received_at, body_sha256, evidence.body.len() as i64,
                parsed.installation_id, repo_id, repo_name, parsed.ref_name,
                parsed.before_sha, parsed.after_sha, parsed.classification,
            ],
        )?;

        if inserted == 0 {
            tx.execute(
                "UPDATE github_deliveries
                 SET duplicate_count=duplicate_count+1, last_received_at=?2
                 WHERE delivery_id=?1",
                params![evidence.delivery_id, evidence.received_at],
            )?;
            tx.execute(
                "INSERT INTO github_delivery_receipts(receipt_id,delivery_id,duplicate,recorded_at)
                 VALUES (?1,?2,1,?3)",
                params![receipt_id, evidence.delivery_id, evidence.received_at],
            )?;
            receipt::set_status(&tx, &receipt_id, ReceiptStatus::Routed)?;
            tx.commit()?;
            return Ok(AcceptResult {
                receipt_id,
                delivery_id: evidence.delivery_id,
                duplicate: true,
                observation_id: None,
                classification: parsed.classification.into(),
                meaningful: false,
            });
        }

        tx.execute(
            "INSERT INTO github_delivery_receipts(receipt_id,delivery_id,duplicate,recorded_at)
             VALUES (?1,?2,0,?3)",
            params![receipt_id, evidence.delivery_id, evidence.received_at],
        )?;
        tx.execute(
            "INSERT INTO observations (
                id,evidence_key,source,source_identity,source_receipt_id,
                github_delivery_id,observed_at,kind,classification,event_name,
                action,repository_id,repository_full_name,body_sha256,
                normalized_metadata,reconciliation_status
             ) VALUES (?1,?2,'github_webhook',?3,?4,?3,?5,?6,?7,?8,?9,?10,?11,?12,?13,'pending')",
            params![
                observation_id,
                format!("github:{}", evidence.delivery_id),
                evidence.delivery_id,
                receipt_id,
                evidence.received_at,
                parsed.kind,
                parsed.classification,
                evidence.event_name,
                parsed.action,
                repo_id,
                repo_name,
                body_sha256,
                parsed.normalized.to_string(),
            ],
        )?;

        if parsed.classification == "KNOWN" && parsed.kind == "repository_lifecycle" {
            if let Some(repo) = parsed.repository.as_ref() {
                upsert_repository_from_event(
                    &tx,
                    repo,
                    &observation_id,
                    &evidence.received_at,
                    &evidence.event_name,
                    parsed.action.as_deref(),
                )?;
            }
        } else if parsed.kind == "repository_activity" {
            if let Some(repo) = parsed.repository.as_ref() {
                tx.execute(
                    "UPDATE repository_registry SET
                        last_observed_at=?2,last_observation_id=?3,
                        last_event_name=?4,last_event_action=?5
                     WHERE repository_id=?1",
                    params![repo.id, evidence.received_at, observation_id, evidence.event_name, parsed.action],
                )?;
            }
        }

        tx.execute(
            "UPDATE github_deliveries SET processing_status='observed' WHERE delivery_id=?1",
            params![evidence.delivery_id],
        )?;
        receipt::set_status(&tx, &receipt_id, ReceiptStatus::Routed)?;
        tx.commit()?;
        Ok(AcceptResult {
            receipt_id,
            delivery_id: evidence.delivery_id,
            duplicate: false,
            observation_id: Some(observation_id),
            classification: parsed.classification.into(),
            meaningful: parsed.meaningful,
        })
    }).await
    .map(|mut result| {
        result.receipt_id = receipt_id_out;
        result.delivery_id = delivery_id_out;
        result.classification = classification_out;
        result.meaningful = meaningful && !result.duplicate;
        result
    })
}

fn upsert_repository_from_event(
    tx: &Transaction<'_>,
    repo: &RepositorySnapshot,
    observation_id: &str,
    observed_at: &str,
    event_name: &str,
    action: Option<&str>,
) -> Result<()> {
    let presence = match action {
        Some("deleted") => "deleted_evidenced",
        Some("transferred") if !repo.full_name.starts_with("powerfarm/") => "transferred_out_evidenced",
        _ => "present",
    };
    let in_scope = presence == "present";
    tx.execute(
        "INSERT INTO repository_registry (
            repository_id,full_name,owner_login,visibility,archived,default_branch,
            html_url,github_updated_at,observed_scope,presence_state,
            membership_treatment,drift_state,first_observed_at,last_observed_at,
            last_observation_id,last_event_name,last_event_action
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'powerfarm',?9,'unresolved',?10,?11,?11,?12,?13,?14)
         ON CONFLICT(repository_id) DO UPDATE SET
            full_name=excluded.full_name,owner_login=excluded.owner_login,
            visibility=excluded.visibility,archived=excluded.archived,
            default_branch=excluded.default_branch,html_url=excluded.html_url,
            github_updated_at=excluded.github_updated_at,
            presence_state=excluded.presence_state,drift_state=excluded.drift_state,
            last_observed_at=excluded.last_observed_at,
            last_observation_id=excluded.last_observation_id,
            last_event_name=excluded.last_event_name,last_event_action=excluded.last_event_action",
        params![
            repo.id, repo.full_name, repo.owner_login, repo.visibility,
            repo.archived.map(i64::from), repo.default_branch, repo.html_url,
            repo.github_updated_at, presence,
            if in_scope { "materially_changed" } else { presence },
            observed_at, observation_id, event_name, action,
        ],
    )?;
    Ok(())
}

pub fn load_repository(conn: &rusqlite::Connection, repository_id: i64) -> Result<Option<RepositorySnapshot>> {
    conn.query_row(
        "SELECT repository_id,full_name,owner_login,visibility,archived,default_branch,html_url,github_updated_at
         FROM repository_registry WHERE repository_id=?1",
        params![repository_id],
        |row| Ok(RepositorySnapshot {
            id: row.get(0)?,
            full_name: row.get(1)?,
            owner_login: row.get(2)?,
            visibility: row.get(3)?,
            archived: row.get::<_, Option<i64>>(4)?.map(|v| v != 0),
            default_branch: row.get(5)?,
            html_url: row.get(6)?,
            github_updated_at: row.get(7)?,
        }),
    ).optional().map_err(Into::into)
}

pub fn repository_materially_changed(a: &RepositorySnapshot, b: &RepositorySnapshot) -> bool {
    a.full_name != b.full_name
        || a.visibility != b.visibility
        || a.archived != b.archived
        || a.default_branch != b.default_branch
        || a.owner_login != b.owner_login
}
