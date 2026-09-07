//! antenna:receipt — the durable statement that Antenna received something.
//!
//! A receipt asserts ONE thing: "Antenna received these bytes, at this time,
//! over this transport, from this source." It never asserts that the content
//! is true. A payload saying "Mars owes me five euros" produces a receipt whose
//! truth is only that the payload arrived. (spec §2)
//!
//! Raw material is immutable. Interpretation adds runs and observations; it
//! never rewrites the receipt's raw body or digest.

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Interaction {
    Event,
    Blob,
    Stream,
    Call,
}

impl Interaction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Interaction::Event => "event",
            Interaction::Blob => "blob",
            Interaction::Stream => "stream",
            Interaction::Call => "call",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Accepted,
    Routed,
    Deferred,
    Quarantined,
    Failed,
    Ignored,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Accepted => "accepted",
            Status::Routed => "routed",
            Status::Deferred => "deferred",
            Status::Quarantined => "quarantined",
            Status::Failed => "failed",
            Status::Ignored => "ignored",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Receipt {
    pub id: String,
    pub kind: String,
    pub recorded_at: String,

    pub scope: Option<String>,
    pub actor: Option<String>,
    pub principal: Option<String>,
    pub provenance: Option<String>,

    pub transport: String,
    pub source: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i64>,

    pub raw_storage: String,
    #[serde(skip)]
    pub raw_body: Option<Vec<u8>>,
    pub raw_ref: Option<String>,
    pub raw_digest: Option<String>,

    pub interaction: String,

    pub correlation_id: Option<String>,
    pub return_path: Option<String>,
    pub route_hint: Option<String>,

    pub relationships: Option<String>,
    pub telemetry: Option<String>,

    pub status: String,
}

impl Receipt {
    pub fn interaction_enum(&self) -> Interaction {
        match self.interaction.as_str() {
            "blob" => Interaction::Blob,
            "stream" => Interaction::Stream,
            "call" => Interaction::Call,
            _ => Interaction::Event,
        }
    }

    /// The received bytes, wherever they live. Raw material stays inspectable.
    pub async fn raw(&self, bucket: &crate::storage::Bucket) -> Result<Vec<u8>> {
        match self.raw_storage.as_str() {
            "sqlite" => Ok(self.raw_body.clone().unwrap_or_default()),
            "object" => {
                let digest = self
                    .raw_ref
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("receipt {} has object storage but no ref", self.id))?;
                Ok(bucket.get(digest).await?.to_vec())
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Best-effort parse of the raw body as JSON. Never fails the journey:
    /// unparseable input is still faithfully received.
    pub async fn json(&self, bucket: &crate::storage::Bucket) -> Option<serde_json::Value> {
        let raw = self.raw(bucket).await.ok()?;
        serde_json::from_slice(&raw).ok()
    }

    fn from_row(row: &Row<'_>) -> rusqlite::Result<Receipt> {
        Ok(Receipt {
            id: row.get("id")?,
            kind: row.get("kind")?,
            recorded_at: row.get("recorded_at")?,
            scope: row.get("scope")?,
            actor: row.get("actor")?,
            principal: row.get("principal")?,
            provenance: row.get("provenance")?,
            transport: row.get("transport")?,
            source: row.get("source")?,
            content_type: row.get("content_type")?,
            content_length: row.get("content_length")?,
            raw_storage: row.get("raw_storage")?,
            raw_body: row.get("raw_body")?,
            raw_ref: row.get("raw_ref")?,
            raw_digest: row.get("raw_digest")?,
            interaction: row.get("interaction")?,
            correlation_id: row.get("correlation_id")?,
            return_path: row.get("return_path")?,
            route_hint: row.get("route_hint")?,
            relationships: row.get("relationships")?,
            telemetry: row.get("telemetry")?,
            status: row.get("status")?,
        })
    }
}

/// What ingress hands to the receipt layer. Everything else is derived.
#[derive(Debug, Clone, Default)]
pub struct NewReceipt {
    pub scope: Option<String>,
    pub actor: Option<String>,
    pub principal: Option<String>,
    pub provenance: Option<String>,
    pub transport: String,
    pub source: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i64>,
    pub raw_storage: String,
    pub raw_body: Option<Vec<u8>>,
    pub raw_ref: Option<String>,
    pub raw_digest: Option<String>,
    pub interaction: String,
    pub correlation_id: Option<String>,
    pub return_path: Option<String>,
    pub route_hint: Option<String>,
    pub relationships: Option<String>,
    pub telemetry: Option<String>,
}

/// Commit the receipt. This is the durability boundary: nothing is
/// acknowledged to the world before this returns (spec §4).
pub fn insert(conn: &Connection, id: &str, recorded_at: &str, n: &NewReceipt) -> Result<()> {
    conn.execute(
        "INSERT INTO receipts (
            id, kind, recorded_at, scope, actor, principal, provenance,
            transport, source, content_type, content_length,
            raw_storage, raw_body, raw_ref, raw_digest,
            interaction, correlation_id, return_path, route_hint,
            relationships, telemetry, status
         ) VALUES (
            ?1, 'antenna:receipt', ?2, ?3, ?4, ?5, ?6,
            ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14,
            ?15, ?16, ?17, ?18,
            ?19, ?20, 'accepted'
         )",
        params![
            id, recorded_at, n.scope, n.actor, n.principal, n.provenance,
            n.transport, n.source, n.content_type, n.content_length,
            n.raw_storage, n.raw_body, n.raw_ref, n.raw_digest,
            n.interaction, n.correlation_id, n.return_path, n.route_hint,
            n.relationships, n.telemetry,
        ],
    )?;
    Ok(())
}

pub fn load(conn: &Connection, id: &str) -> Result<Option<Receipt>> {
    let r = conn
        .query_row("SELECT * FROM receipts WHERE id = ?1", params![id], Receipt::from_row)
        .optional()?;
    Ok(r)
}

pub fn set_status(conn: &Connection, id: &str, status: Status) -> Result<()> {
    conn.execute(
        "UPDATE receipts SET status = ?2 WHERE id = ?1",
        params![id, status.as_str()],
    )?;
    Ok(())
}

pub fn recent(conn: &Connection, limit: i64) -> Result<Vec<Receipt>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM receipts ORDER BY recorded_at DESC, id DESC LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit], Receipt::from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Receipts committed but never routed — the crash-recovery worklist (spec §19).
pub fn unfinished(conn: &Connection) -> Result<Vec<Receipt>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM receipts WHERE status = 'accepted' ORDER BY recorded_at ASC",
    )?;
    let rows = stmt.query_map([], Receipt::from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}
