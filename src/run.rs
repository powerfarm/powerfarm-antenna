//! A Run is the durable record of one capability invocation.

use anyhow::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Run {
    pub id: String,
    pub receipt_id: String,
    pub capability: String,
    pub status: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

pub fn create(
    conn: &Connection,
    id: &str,
    receipt_id: &str,
    capability: &str,
    input: Option<&str>,
    telemetry: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO runs (id, receipt_id, capability, status, input, created_at, started_at, telemetry)
         VALUES (?1, ?2, ?3, 'running', ?4, ?5, ?5, ?6)",
        params![id, receipt_id, capability, input, crate::ids::now_rfc3339(), telemetry],
    )?;
    Ok(())
}

pub fn complete(conn: &Connection, id: &str, output: &str) -> Result<()> {
    conn.execute(
        "UPDATE runs SET status='completed', output=?2, completed_at=?3 WHERE id=?1",
        params![id, output, crate::ids::now_rfc3339()],
    )?;
    Ok(())
}

pub fn fail(conn: &Connection, id: &str, error: &str) -> Result<()> {
    conn.execute(
        "UPDATE runs SET status='failed', error=?2, completed_at=?3 WHERE id=?1",
        params![id, error, crate::ids::now_rfc3339()],
    )?;
    Ok(())
}

/// A run left 'running' by a crash. The process that owned it is gone, so the
/// run is closed as interrupted; its receipt goes back on the routing worklist.
pub fn mark_interrupted(conn: &Connection) -> Result<Vec<String>> {
    let receipt_ids: Vec<String> = {
        let mut stmt = conn.prepare("SELECT receipt_id FROM runs WHERE status='running'")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    conn.execute(
        "UPDATE runs SET status='interrupted', error='interrupted by restart', completed_at=?1
         WHERE status='running'",
        params![crate::ids::now_rfc3339()],
    )?;
    Ok(receipt_ids)
}

pub fn for_receipt(conn: &Connection, receipt_id: &str) -> Result<Vec<Run>> {
    let mut stmt = conn.prepare(
        "SELECT id, receipt_id, capability, status, output, error, created_at, completed_at
         FROM runs WHERE receipt_id = ?1 ORDER BY created_at ASC",
    )?;
    let rows = stmt.query_map(params![receipt_id], |r| {
        Ok(Run {
            id: r.get(0)?,
            receipt_id: r.get(1)?,
            capability: r.get(2)?,
            status: r.get(3)?,
            output: r.get(4)?,
            error: r.get(5)?,
            created_at: r.get(6)?,
            completed_at: r.get(7)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}
