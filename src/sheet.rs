//! Narrow, idempotent projection into the one canonical Live Blueprint.
//! SQLite remains evidence/supporting state; this writer never creates a
//! competing plan and never writes Target or constitutional columns.

use crate::app::App;
use crate::observation::sha256_hex;
use anyhow::{Context, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

const SHEETS_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets";
const ALLOWED_SEMANTIC_KEYS: &[&str] = &[
    "observed_state", "attention_state", "last_observed_at", "machine_source",
    "drift_treatment",
    "observed_repository_id", "observed_repository_name", "observed_visibility",
    "observed_archived", "observed_default_branch", "observed_presence",
    "membership_treatment",
];

#[derive(Debug, Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

#[derive(Serialize)]
struct ServiceClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Clone)]
struct PendingRecord {
    record_id: String,
    desired: Map<String, Value>,
    reconciliation_run_id: String,
}

#[derive(Debug, Clone)]
pub struct ProjectionResult {
    pub mutation_id: String,
    pub affected_record_ids: Vec<String>,
    pub response_ranges: Vec<String>,
}

fn default_token_uri() -> String { "https://oauth2.googleapis.com/token".into() }

fn quoted_sheet(name: &str) -> String { format!("'{}'", name.replace('\'', "''")) }

fn column_name(mut index: usize) -> String {
    let mut out = String::new();
    loop {
        out.insert(0, (b'A' + (index % 26) as u8) as char);
        if index < 26 { break; }
        index = index / 26 - 1;
    }
    out
}

pub fn resolve_record_rows(record_id_column: &[Value]) -> Result<HashMap<String, usize>> {
    let mut rows = HashMap::new();
    for (offset, value) in record_id_column.iter().enumerate() {
        let Some(id) = value.as_str().filter(|v| !v.trim().is_empty()) else { continue };
        let row = offset + 2;
        if rows.insert(id.to_string(), row).is_some() {
            anyhow::bail!("duplicate Record ID {id}");
        }
    }
    Ok(rows)
}

pub fn field_map_is_safe(map: &BTreeMap<String, String>) -> bool {
    map.iter().all(|(semantic, header)| {
        ALLOWED_SEMANTIC_KEYS.contains(&semantic.as_str()) && {
            let normalized = header.to_ascii_lowercase();
            !normalized.contains("target")
                && !normalized.contains("governing principle")
                && !normalized.contains("constitutional")
                && !normalized.contains("decision log")
                && !normalized.contains("authority")
        }
    })
}

async fn access_token(app: &App) -> Result<String> {
    let path = app.cfg.google.credentials_path.as_ref()
        .context("google.credentials_path is missing")?;
    let bytes = tokio::fs::read(path).await
        .with_context(|| format!("reading Google credential {}", path.display()))?;
    let credential: ServiceAccount = serde_json::from_slice(&bytes)
        .context("parsing Google service-account credential")?;
    let now = chrono::Utc::now().timestamp();
    let claims = ServiceClaims {
        iss: &credential.client_email,
        scope: SHEETS_SCOPE,
        aud: &credential.token_uri,
        iat: now - 30,
        exp: now + 3600,
    };
    let assertion = encode(
        &Header::new(Algorithm::RS256),
        &claims,
        &EncodingKey::from_rsa_pem(credential.private_key.as_bytes())
            .context("reading Google service-account private key")?,
    ).context("signing Google OAuth assertion")?;
    let response = app.http.post(&credential.token_uri).form(&[
        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
        ("assertion", assertion.as_str()),
    ]).send().await.context("requesting Google access token")?;
    let status = response.status();
    let value: Value = response.json().await.context("decoding Google token response")?;
    if !status.is_success() { anyhow::bail!("Google token request failed with status {status}"); }
    value.get("access_token").and_then(Value::as_str).map(str::to_string)
        .context("Google token response omitted access_token")
}

async fn get_values(app: &App, token: &str, spreadsheet_id: &str, range: &str) -> Result<Vec<Vec<Value>>> {
    let mut url = reqwest::Url::parse("https://sheets.googleapis.com/v4/spreadsheets")?;
    url.path_segments_mut().map_err(|_| anyhow::anyhow!("invalid Sheets base URL"))?
        .push(spreadsheet_id).push("values").push(range);
    let response = app.http.get(url).bearer_auth(token).send().await.context("reading Sheet range")?;
    let status = response.status();
    let value: Value = response.json().await.context("decoding Sheet range")?;
    if !status.is_success() { anyhow::bail!("Google Sheets range read failed with status {status}"); }
    Ok(value.get("values").and_then(Value::as_array).map(|rows| {
        rows.iter().map(|row| row.as_array().cloned().unwrap_or_default()).collect()
    }).unwrap_or_default())
}

#[cfg(test)]
mod url_tests {
    #[test]
    fn sheet_value_range_is_one_encoded_path_segment() {
        let spreadsheet_id = "spreadsheet-id";
        let range = "'Live Blueprint'!A2:A200";
        let mut url = reqwest::Url::parse("https://sheets.googleapis.com/v4/spreadsheets")
            .unwrap();
        url.path_segments_mut()
            .unwrap()
            .push(spreadsheet_id)
            .push("values")
            .push(range);

        assert_eq!(
            url.as_str(),
            "https://sheets.googleapis.com/v4/spreadsheets/spreadsheet-id/values/'Live%20Blueprint'!A2:A200"
        );
        assert!(!url.path().contains("spreadsheets//"));
    }

    #[test]
    fn repository_append_leaves_target_column_unset() {
        let record = super::PendingRecord {
            record_id: "GH-REPO-42".into(),
            desired: serde_json::json!({
                "observed_repository_name": "powerfarm/example",
                "observed_state": "repository present"
            })
            .as_object()
            .unwrap()
            .clone(),
            reconciliation_run_id: "rec_test".into(),
        };
        let headers = std::collections::HashMap::from([
            ("ID", 0usize),
            ("Board object", 1usize),
            ("Target / declared state", 4usize),
            ("Observed / current state", 5usize),
        ]);
        let field_map = std::collections::BTreeMap::from([
            ("observed_repository_name".into(), "Board object".into()),
            ("observed_state".into(), "Observed / current state".into()),
        ]);

        let values = super::appended_row_values(&record, 0, &headers, &field_map).unwrap();
        assert_eq!(
            values[0],
            serde_json::json!({"userEnteredValue":{"stringValue":"GH-REPO-42"}})
        );
        assert_eq!(
            values[1],
            serde_json::json!({"userEnteredValue":{"stringValue":"powerfarm/example"}})
        );
        assert_eq!(values[4], serde_json::json!({}));
        assert_eq!(
            values[5],
            serde_json::json!({"userEnteredValue":{"stringValue":"repository present"}})
        );
    }
}

fn cell_value(value: &Value) -> Value {
    match value {
        Value::Bool(value) => json!({"boolValue": value}),
        Value::Number(value) => json!({"numberValue": value.as_f64().unwrap_or_default()}),
        Value::Null => json!({"stringValue": ""}),
        Value::String(value) => json!({"stringValue": value}),
        other => json!({"stringValue": other.to_string()}),
    }
}

fn appended_row_values(
    record: &PendingRecord,
    record_id_col: usize,
    header_by_name: &HashMap<&str, usize>,
    field_map: &BTreeMap<String, String>,
) -> Result<Vec<Value>> {
    let mut mapped = vec![(
        record_id_col,
        json!({"userEnteredValue": {"stringValue": record.record_id}}),
    )];
    for (semantic, value) in &record.desired {
        let Some(header) = field_map.get(semantic) else {
            continue;
        };
        let column = *header_by_name
            .get(header.as_str())
            .with_context(|| format!("configured Sheet header {header:?} is absent"))?;
        mapped.push((column, json!({"userEnteredValue": cell_value(value)})));
    }
    let width = mapped
        .iter()
        .map(|(column, _)| *column)
        .max()
        .unwrap_or(record_id_col)
        + 1;
    let mut values = vec![json!({}); width];
    for (column, value) in mapped {
        values[column] = value;
    }
    Ok(values)
}

async fn load_pending(app: &App) -> Result<Vec<PendingRecord>> {
    app.db.call(|conn| {
        let mut stmt = conn.prepare(
            "SELECT record_id,desired_json,reconciliation_run_id
             FROM projection_records WHERE status IN ('pending','failed')
             ORDER BY updated_at,record_id LIMIT 100",
        )?;
        let rows = stmt.query_map([], |row| Ok((
            row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?,
        )))?;
        let mut pending = Vec::new();
        for row in rows {
            let (record_id, desired_json, reconciliation_run_id) = row?;
            let desired = serde_json::from_str::<Value>(&desired_json)?
                .as_object().cloned().context("projection is not an object")?;
            pending.push(PendingRecord { record_id, desired, reconciliation_run_id });
        }
        Ok(pending)
    }).await
}

pub async fn project_once(app: &App) -> Result<Option<ProjectionResult>> {
    if !app.cfg.google.enabled { return Ok(None); }
    if !field_map_is_safe(&app.cfg.google.field_map) {
        anyhow::bail!("google.field_map contains forbidden or unsupported projection fields");
    }
    let spreadsheet_id = app.cfg.google.spreadsheet_id.as_deref()
        .context("google.spreadsheet_id is missing")?;
    let sheet_name = app.cfg.google.sheet_name.as_deref()
        .context("google.sheet_name is missing; refusing to guess a tab")?;
    let pending = load_pending(app).await?;
    if pending.is_empty() { return Ok(None); }
    let token = access_token(app).await?;

    // Resolve the tab and its bounded grid before reading values. This makes
    // metadata the first Sheets read and avoids guessing either sheetId or a
    // remembered maximum row.
    let metadata_url = format!(
        "https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}?fields=sheets.properties(sheetId,title,gridProperties(rowCount))"
    );
    let metadata_response = app.http.get(metadata_url).bearer_auth(&token).send().await
        .context("reading Sheet metadata")?;
    let metadata_status = metadata_response.status();
    let metadata: Value = metadata_response.json().await.context("decoding Sheet metadata")?;
    if !metadata_status.is_success() { anyhow::bail!("Google Sheets metadata read failed with status {metadata_status}"); }
    let sheet = metadata.get("sheets").and_then(Value::as_array)
        .and_then(|sheets| sheets.iter().find(|sheet| {
            sheet.pointer("/properties/title").and_then(Value::as_str) == Some(sheet_name)
        }))
        .context("configured Sheet tab not found in metadata")?;
    let sheet_id = sheet.pointer("/properties/sheetId").and_then(Value::as_i64)
        .context("configured Sheet tab omitted sheetId")?;
    let row_count = sheet.pointer("/properties/gridProperties/rowCount").and_then(Value::as_u64)
        .filter(|count| *count >= 2)
        .context("configured Sheet tab has no data rows")?;

    let header_range = format!("{}!1:1", quoted_sheet(sheet_name));
    let header_rows = get_values(app, &token, spreadsheet_id, &header_range).await?;
    let headers = header_rows.first().context("Sheet header row is empty")?;
    let record_id_col = headers.iter().position(|v| v.as_str() == Some(&app.cfg.google.record_id_header))
        .context("stable Record ID header not found")?;
    let id_column = column_name(record_id_col);
    let id_range = format!("{}!{}2:{}{}", quoted_sheet(sheet_name), id_column, id_column, row_count);
    let id_rows = get_values(app, &token, spreadsheet_id, &id_range).await?;
    let id_values: Vec<Value> = id_rows.into_iter().map(|row| row.into_iter().next().unwrap_or(Value::Null)).collect();
    let row_by_id = resolve_record_rows(&id_values)?;
    let header_by_name: HashMap<&str, usize> = headers.iter().enumerate()
        .filter_map(|(index, value)| value.as_str().map(|name| (name, index))).collect();

    let mut requests = Vec::new();
    let mut response_ranges = Vec::new();
    let mut affected = Vec::new();
    for record in &pending {
        let Some(row) = row_by_id.get(&record.record_id).copied() else {
            // appendCells chooses the physical row. The immutable GitHub ID is
            // written in the same atomic request and becomes its durable address.
            if record.record_id.starts_with("GH-REPO-") {
                let values = appended_row_values(
                    record,
                    record_id_col,
                    &header_by_name,
                    &app.cfg.google.field_map,
                )?;
                requests.push(json!({
                    "appendCells": {
                        "sheetId": sheet_id,
                        "rows": [{"values": values}],
                        "fields": "userEnteredValue"
                    }
                }));
                affected.push(record.record_id.clone());
            }
            continue;
        };
        let mut wrote = false;
        for (semantic, value) in &record.desired {
            let Some(header) = app.cfg.google.field_map.get(semantic) else { continue };
            let column = *header_by_name.get(header.as_str())
                .with_context(|| format!("configured Sheet header {header:?} is absent"))?;
            requests.push(json!({
                "updateCells": {
                    "start": {"sheetId": sheet_id, "rowIndex": row - 1, "columnIndex": column},
                    "rows": [{"values": [{"userEnteredValue": cell_value(value)}]}],
                    "fields": "userEnteredValue"
                }
            }));
            response_ranges.push(format!("{}!{}{}", quoted_sheet(sheet_name), column_name(column), row));
            wrote = true;
        }
        if wrote { affected.push(record.record_id.clone()); }
    }
    if requests.is_empty() {
        anyhow::bail!("no pending projection Record IDs were resolvable in the configured Sheet tab");
    }

    let mutation_id = crate::ids::new_id("mut");
    let reconciliation_run_id = pending.first().map(|p| p.reconciliation_run_id.clone()).unwrap();
    let payload = json!({
        "requests": requests,
        "includeSpreadsheetInResponse": true,
        "responseIncludeGridData": false,
        "responseRanges": response_ranges,
    });
    let payload_sha256 = sha256_hex(serde_json::to_vec(&payload)?.as_slice());
    let affected_json = serde_json::to_string(&affected)?;
    let started_at = crate::ids::now_rfc3339();
    let mutation_id_for_db = mutation_id.clone();
    let affected_for_db = affected.clone();
    app.db.call(move |conn| {
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO projection_mutations(
                mutation_id,reconciliation_run_id,affected_record_ids,payload_sha256,status,started_at
             ) VALUES (?1,?2,?3,?4,'in_flight',?5)",
            params![mutation_id_for_db,reconciliation_run_id,affected_json,payload_sha256,started_at],
        )?;
        for record_id in affected_for_db {
            tx.execute(
                "UPDATE projection_records SET status='in_flight',mutation_id=?2 WHERE record_id=?1",
                params![record_id, mutation_id_for_db],
            )?;
        }
        tx.commit()?;
        Ok(())
    }).await?;

    let url = format!("https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate");
    let response = app.http.post(url).bearer_auth(&token).json(&payload).send().await;
    let (status_kind, effect_receipt, error) = match response {
        Ok(response) => {
            let status = response.status();
            let value: Value = response.json().await.unwrap_or_else(|_| json!({"status": status.as_u16()}));
            if status.is_success() { ("completed", Some(value), None) }
            else if status.is_client_error() { ("failed", Some(value), Some(format!("Sheets rejected mutation with status {status}"))) }
            else { ("uncertain", Some(value), Some(format!("Sheets mutation outcome uncertain after status {status}"))) }
        }
        Err(error) => ("uncertain", None, Some(format!("Sheets mutation outcome uncertain: {error}"))),
    };
    let completed_at = crate::ids::now_rfc3339();
    let receipt_json = effect_receipt.map(|v| v.to_string());
    let mutation_id_for_db = mutation_id.clone();
    let affected_for_db = affected.clone();
    let status_for_db = status_kind.to_string();
    let error_for_db = error.clone();
    app.db.call(move |conn| {
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE projection_mutations SET status=?2,completed_at=?3,effect_receipt_json=?4,error=?5
             WHERE mutation_id=?1",
            params![mutation_id_for_db,status_for_db,completed_at,receipt_json,error_for_db],
        )?;
        for record_id in affected_for_db {
            tx.execute(
                "UPDATE projection_records SET status=?2,completed_at=CASE WHEN ?2='completed' THEN ?3 ELSE NULL END,
                    last_error=?4 WHERE record_id=?1 AND mutation_id=?5",
                params![record_id,status_for_db,completed_at,error_for_db,mutation_id_for_db],
            )?;
        }
        tx.commit()?;
        Ok(())
    }).await?;
    if status_kind != "completed" { anyhow::bail!(error.unwrap_or_else(|| "Sheet mutation failed".into())); }
    Ok(Some(ProjectionResult { mutation_id, affected_record_ids: affected, response_ranges }))
}

pub async fn worker(app: App) {
    let interval = std::time::Duration::from_secs(app.cfg.google.projection_interval_secs.max(30));
    loop {
        if let Err(error) = project_once(&app).await {
            tracing::error!(error = %error, "Live Blueprint projection failed");
        }
        tokio::time::sleep(interval).await;
    }
}
