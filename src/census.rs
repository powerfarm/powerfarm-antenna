//! Complete, paginated GitHub App repository census. The projection changes
//! only after a complete successful run; absence is recorded as absence from
//! this scope/run, never inferred deletion.

use crate::app::App;
use crate::observation::{repository_materially_changed, sha256_hex, RepositorySnapshot};
use crate::storage::Db;
use anyhow::{Context, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rusqlite::params;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct CensusInput {
    pub organization: String,
    pub installation_id: Option<i64>,
    pub started_at: String,
    pub repositories: Vec<RepositorySnapshot>,
    pub page_count: i64,
    pub api_rate_remaining: Option<i64>,
    pub api_rate_reset: Option<String>,
    pub complete: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CensusResult {
    pub run_id: String,
    pub complete: bool,
    pub repository_count: i64,
    pub observations_emitted: i64,
}

#[derive(Serialize)]
struct AppClaims {
    iat: i64,
    exp: i64,
    iss: String,
}

pub async fn record(db: &Db, input: CensusInput) -> Result<CensusResult> {
    let run_id = crate::ids::new_id("cen");
    let run_id_out = run_id.clone();
    db.call(move |conn| {
        let tx = conn.transaction().context("beginning census commit")?;
        let status = if input.complete { "completed" } else { "failed" };
        let completed_at = crate::ids::now_rfc3339();
        tx.execute(
            "INSERT INTO census_runs(
                id,organization,installation_id,started_at,completed_at,status,
                complete,repository_count,page_count,api_rate_remaining,
                api_rate_reset,error
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                run_id, input.organization, input.installation_id, input.started_at,
                completed_at, status, i64::from(input.complete),
                input.repositories.len() as i64, input.page_count,
                input.api_rate_remaining, input.api_rate_reset, input.error,
            ],
        )?;
        if !input.complete {
            tx.commit()?;
            return Ok(CensusResult {
                run_id,
                complete: false,
                repository_count: input.repositories.len() as i64,
                observations_emitted: 0,
            });
        }

        let mut prior = HashMap::<i64, (RepositorySnapshot, String, String)>::new();
        {
            let mut stmt = tx.prepare(
                "SELECT repository_id,full_name,owner_login,visibility,archived,
                        default_branch,html_url,github_updated_at,last_observation_id,presence_state
                 FROM repository_registry",
            )?;
            let rows = stmt.query_map([], |row| Ok((
                row.get::<_, i64>(0)?,
                RepositorySnapshot {
                    id: row.get(0)?, full_name: row.get(1)?, owner_login: row.get(2)?,
                    visibility: row.get(3)?, archived: row.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                    default_branch: row.get(5)?, html_url: row.get(6)?, github_updated_at: row.get(7)?,
                },
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
            )))?;
            for row in rows {
                let (id, snapshot, obs, presence) = row?;
                prior.insert(id, (snapshot, obs, presence));
            }
        }

        let mut seen = HashSet::new();
        let mut observations_emitted = 0i64;
        for repo in &input.repositories {
            if !seen.insert(repo.id) {
                anyhow::bail!("duplicate repository id {} in census", repo.id);
            }
            let metadata = serde_json::to_string(repo)?;
            tx.execute(
                "INSERT INTO census_repositories(census_run_id,repository_id,full_name,metadata_json)
                 VALUES (?1,?2,?3,?4)",
                params![run_id, repo.id, repo.full_name, metadata],
            )?;
            let kind = match prior.get(&repo.id) {
                None => Some("repository_newly_observed"),
                Some((old, _, presence)) if repository_materially_changed(old, repo) || presence != "present" =>
                    Some("repository_materially_changed"),
                _ => None,
            };
            let observation_id = if let Some(kind) = kind {
                let observation_id = crate::ids::new_id("obs");
                let normalized = json!({
                    "census_run_id": run_id,
                    "repository": repo,
                    "comparison": kind,
                    "absence_does_not_imply_deletion": true
                }).to_string();
                tx.execute(
                    "INSERT INTO observations(
                        id,evidence_key,source,source_identity,observed_at,kind,
                        classification,repository_id,repository_full_name,
                        normalized_metadata,reconciliation_status
                     ) VALUES (?1,?2,'github_census',?3,?4,?5,'KNOWN',?6,?7,?8,'pending')",
                    params![
                        observation_id,
                        format!("census:{}:repo:{}:{}", run_id, repo.id, kind),
                        run_id,
                        completed_at,
                        kind,
                        repo.id,
                        repo.full_name,
                        normalized,
                    ],
                )?;
                observations_emitted += 1;
                observation_id
            } else {
                prior.get(&repo.id).map(|(_, obs, _)| obs.clone())
                    .context("known repository missing prior observation")?
            };

            tx.execute(
                "INSERT INTO repository_registry(
                    repository_id,full_name,owner_login,visibility,archived,default_branch,
                    html_url,github_updated_at,observed_scope,presence_state,
                    membership_treatment,drift_state,first_observed_at,last_observed_at,
                    last_observation_id,last_census_run_id,last_event_name,last_event_action
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'present','unresolved',?10,?11,?11,?12,?13,'census',NULL)
                 ON CONFLICT(repository_id) DO UPDATE SET
                    full_name=excluded.full_name,owner_login=excluded.owner_login,
                    visibility=excluded.visibility,archived=excluded.archived,
                    default_branch=excluded.default_branch,html_url=excluded.html_url,
                    github_updated_at=excluded.github_updated_at,observed_scope=excluded.observed_scope,
                    presence_state='present',drift_state=excluded.drift_state,
                    last_observed_at=excluded.last_observed_at,
                    last_observation_id=excluded.last_observation_id,
                    last_census_run_id=excluded.last_census_run_id,
                    last_event_name='census',last_event_action=NULL",
                params![
                    repo.id, repo.full_name, repo.owner_login, repo.visibility,
                    repo.archived.map(i64::from), repo.default_branch, repo.html_url,
                    repo.github_updated_at, input.organization,
                    if kind.is_some() { kind.unwrap() } else { "present_and_previously_known" },
                    completed_at, observation_id, run_id,
                ],
            )?;
        }

        for (repo_id, (old, last_observation_id, presence)) in prior {
            if seen.contains(&repo_id) || presence != "present" { continue; }
            let observation_id = crate::ids::new_id("obs");
            let normalized = json!({
                "census_run_id": run_id,
                "repository_id": repo_id,
                "last_known_full_name": old.full_name,
                "classification": "not_observed_in_current_scope_run",
                "deletion_inferred": false
            }).to_string();
            tx.execute(
                "INSERT INTO observations(
                    id,evidence_key,source,source_identity,observed_at,kind,
                    classification,repository_id,repository_full_name,
                    normalized_metadata,reconciliation_status
                 ) VALUES (?1,?2,'github_census',?3,?4,'repository_no_longer_observed','KNOWN',?5,?6,?7,'pending')",
                params![
                    observation_id,
                    format!("census:{}:repo:{}:missing", run_id, repo_id),
                    run_id, completed_at, repo_id, old.full_name, normalized,
                ],
            )?;
            tx.execute(
                "UPDATE repository_registry SET
                    presence_state='not_observed_in_latest_complete_census',
                    drift_state='no_longer_observed_in_current_scope',
                    last_observed_at=?2,last_observation_id=?3,last_census_run_id=?4,
                    last_event_name='census',last_event_action='missing'
                 WHERE repository_id=?1",
                params![repo_id, completed_at, observation_id, run_id],
            )?;
            let _ = last_observation_id;
            observations_emitted += 1;
        }

        tx.commit()?;
        Ok(CensusResult {
            run_id,
            complete: true,
            repository_count: input.repositories.len() as i64,
            observations_emitted,
        })
    }).await.map(|mut result| {
        result.run_id = run_id_out;
        result
    })
}

fn app_jwt(app_id: u64, private_key_pem: &[u8]) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    let claims = AppClaims { iat: now - 60, exp: now + 540, iss: app_id.to_string() };
    encode(
        &Header::new(Algorithm::RS256),
        &claims,
        &EncodingKey::from_rsa_pem(private_key_pem).context("reading GitHub App RSA key")?,
    ).context("signing GitHub App JWT")
}

async fn installation_token(app: &App) -> Result<String> {
    let app_id = app.cfg.github.app_id.context("github.app_id is missing")?;
    let installation_id = app.cfg.github.installation_id.context("github.installation_id is missing")?;
    let private_key_path = app.cfg.github.private_key_path.as_ref().context("github.private_key_path is missing")?;
    let private_key = tokio::fs::read(private_key_path).await
        .with_context(|| format!("reading GitHub App key {}", private_key_path.display()))?;
    let jwt = app_jwt(app_id, &private_key)?;
    let url = format!("{}/app/installations/{}/access_tokens", app.cfg.github.api_base, installation_id);
    let response = app.http.post(url)
        .header("accept", "application/vnd.github+json")
        .header("x-github-api-version", "2022-11-28")
        .bearer_auth(jwt)
        .send().await.context("requesting GitHub installation token")?;
    let status = response.status();
    let value: Value = response.json().await.context("decoding GitHub installation token response")?;
    if !status.is_success() {
        anyhow::bail!("GitHub installation token request failed with status {status}");
    }
    value.get("token").and_then(Value::as_str).map(str::to_string)
        .context("GitHub installation token response omitted token")
}

pub async fn fetch_all_repositories(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    organization: &str,
) -> Result<(Vec<RepositorySnapshot>, i64, Option<i64>, Option<String>)> {
    let mut page = 1i64;
    let mut total_expected: Option<usize> = None;
    let mut repositories = Vec::new();
    let mut rate_remaining = None;
    let mut rate_reset = None;
    loop {
        let url = format!("{api_base}/installation/repositories?per_page=100&page={page}");
        let response = client.get(url)
            .header("accept", "application/vnd.github+json")
            .header("x-github-api-version", "2022-11-28")
            .bearer_auth(token)
            .send().await.context("fetching GitHub installation repositories")?;
        let status = response.status();
        rate_remaining = response.headers().get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()).or(rate_remaining);
        rate_reset = response.headers().get("x-ratelimit-reset")
            .and_then(|v| v.to_str().ok()).map(str::to_string).or(rate_reset);
        let value: Value = response.json().await.context("decoding GitHub repositories page")?;
        if !status.is_success() {
            anyhow::bail!("GitHub repository census failed with status {status}");
        }
        let expected = value.get("total_count").and_then(Value::as_u64)
            .context("GitHub page omitted total_count")? as usize;
        match total_expected {
            Some(previous) if previous != expected => anyhow::bail!("GitHub total_count changed during pagination"),
            None => total_expected = Some(expected),
            _ => {}
        }
        let page_items = value.get("repositories").and_then(Value::as_array)
            .context("GitHub page omitted repositories")?;
        for value in page_items {
            let Some(repo) = RepositorySnapshot::from_value(value) else {
                anyhow::bail!("GitHub repository response omitted stable identity");
            };
            if repo.owner_login.as_deref() == Some(organization) {
                repositories.push(repo);
            }
        }
        if page_items.len() < 100 { break; }
        page += 1;
        if page > 10_000 { anyhow::bail!("GitHub pagination exceeded safety bound"); }
    }
    let unique: HashSet<i64> = repositories.iter().map(|repo| repo.id).collect();
    if unique.len() != repositories.len() {
        anyhow::bail!("GitHub census returned duplicate repository identities");
    }
    let expected = total_expected.unwrap_or(0);
    if repositories.len() != expected {
        anyhow::bail!(
            "GitHub census incomplete for organization: expected {expected}, retained {}",
            repositories.len()
        );
    }
    Ok((repositories, page, rate_remaining, rate_reset))
}

pub async fn run_once(app: &App) -> Result<CensusResult> {
    let started_at = crate::ids::now_rfc3339();
    let token = match installation_token(app).await {
        Ok(token) => token,
        Err(error) => {
            let message = error.to_string();
            record(&app.db, CensusInput {
                organization: app.cfg.github.organization.clone(),
                installation_id: app.cfg.github.installation_id.map(|v| v as i64),
                started_at,
                repositories: Vec::new(),
                page_count: 0,
                api_rate_remaining: None,
                api_rate_reset: None,
                complete: false,
                error: Some(message.clone()),
            }).await?;
            anyhow::bail!(message);
        }
    };
    match fetch_all_repositories(
        &app.http,
        &app.cfg.github.api_base,
        &token,
        &app.cfg.github.organization,
    ).await {
        Ok((repositories, page_count, remaining, reset)) => {
            let result = record(&app.db, CensusInput {
                organization: app.cfg.github.organization.clone(),
                installation_id: app.cfg.github.installation_id.map(|v| v as i64),
                started_at,
                repositories,
                page_count,
                api_rate_remaining: remaining,
                api_rate_reset: reset,
                complete: true,
                error: None,
            }).await?;
            crate::reconcile::run(&app.db, "census", Some(&result.run_id)).await?;
            Ok(result)
        }
        Err(error) => {
            let message = error.to_string();
            record(&app.db, CensusInput {
                organization: app.cfg.github.organization.clone(),
                installation_id: app.cfg.github.installation_id.map(|v| v as i64),
                started_at,
                repositories: Vec::new(),
                page_count: 0,
                api_rate_remaining: None,
                api_rate_reset: None,
                complete: false,
                error: Some(message.clone()),
            }).await?;
            anyhow::bail!(message)
        }
    }
}

pub async fn worker(app: App) {
    let interval = std::time::Duration::from_secs(app.cfg.github.census_interval_secs.max(60));
    loop {
        if let Err(error) = run_once(&app).await {
            tracing::error!(error = %error, "GitHub census failed");
        }
        tokio::time::sleep(interval).await;
    }
}

pub fn census_payload_digest(repositories: &[RepositorySnapshot]) -> Result<String> {
    let json = serde_json::to_vec(repositories)?;
    Ok(sha256_hex(&json))
}
