//! One-shot, local import of a complete GitHub organization API census.
//!
//! This is an operational bootstrap/recovery tool, not the scheduled path.
//! Scheduled operation uses the GitHub App installation in `census::run_once`.

use anyhow::{Context, Result};
use antenna::census::{self, CensusInput};
use antenna::config::Config;
use antenna::observation::RepositorySnapshot;
use serde_json::Value;
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args_os().nth(1).map(PathBuf::from)
        .context("usage: census-import COMPLETE_GITHUB_REPOSITORIES_JSON")?;
    let config_path = std::env::var("ANTENNA_CONFIG")
        .map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("antenna.toml"));
    let routes_path = std::env::var("ANTENNA_ROUTES")
        .map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("routes/default.toml"));
    let cfg = Config::load(&config_path)?;
    let bytes = if path.as_os_str() == "-" {
        let mut bytes = Vec::new();
        std::io::stdin().read_to_end(&mut bytes).context("reading repository JSON from stdin")?;
        bytes
    } else {
        std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?
    };
    let values: Vec<Value> = serde_json::from_slice(&bytes).context("parsing repository JSON array")?;
    let repositories: Vec<RepositorySnapshot> = values.iter()
        .map(RepositorySnapshot::from_value)
        .collect::<Option<Vec<_>>>()
        .context("repository response omitted a stable identity")?;
    if repositories.iter().any(|repo| repo.owner_login.as_deref() != Some(&cfg.github.organization)) {
        anyhow::bail!("census contains a repository outside configured organization scope");
    }
    let unique: HashSet<i64> = repositories.iter().map(|repo| repo.id).collect();
    if unique.len() != repositories.len() {
        anyhow::bail!("census contains duplicate repository IDs");
    }

    let app = antenna::build(cfg, &routes_path)?;
    let result = census::record(&app.db, CensusInput {
        organization: app.cfg.github.organization.clone(),
        installation_id: None,
        started_at: antenna::ids::now_rfc3339(),
        repositories,
        page_count: 1,
        api_rate_remaining: None,
        api_rate_reset: None,
        complete: true,
        error: Some("provisional bootstrap using authenticated organization API; not scheduled GitHub App census".into()),
    }).await?;
    let reconciled = antenna::reconcile::run(&app.db, "provisional_census", Some(&result.run_id)).await?;
    println!("{}", serde_json::json!({
        "census_run_id": result.run_id,
        "complete": result.complete,
        "repository_count": result.repository_count,
        "observations_emitted": result.observations_emitted,
        "reconciliation_run_id": reconciled.run_id,
        "projection_items_changed": reconciled.projection_item_count,
        "credential_class": "provisional_authenticated_org_read"
    }));
    Ok(())
}
