//! Configuration is STATE (spec §13): routing policy, authority policy,
//! delivery policy. It is read at boot and never mutated by traffic.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub delivery: Delivery,
    #[serde(default)]
    pub gatekeeper: Gatekeeper,
    #[serde(default)]
    pub talent: Talent,
    #[serde(default)]
    pub github: Github,
    #[serde(default)]
    pub google: Google,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    #[serde(default = "d_bind")]
    pub bind: String,
    #[serde(default = "d_max_event")]
    pub max_event_bytes: usize,
    #[serde(default = "d_max_blob")]
    pub max_blob_bytes: u64,
    #[serde(default = "d_call_timeout")]
    pub call_timeout_ms: u64,
    /// Bearer token guarding the READ surface (/receipts, /deliveries).
    /// Ingress endpoints stay open — a membrane that refuses to receive is
    /// not a membrane. But the record of what was received is operator-only.
    /// `None` leaves inspection open; only sane when bound to loopback.
    #[serde(default)]
    pub inspect_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    #[serde(default = "d_data_dir")]
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delivery {
    #[serde(default = "d_max_attempts")]
    pub max_attempts: i64,
    #[serde(default = "d_backoff")]
    pub base_backoff_ms: i64,
    #[serde(default = "d_worker_interval")]
    pub worker_interval_ms: u64,
    #[serde(default = "d_http_timeout")]
    pub http_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gatekeeper {
    /// Hosts Antenna may deliver to. `*.example.com` matches subdomains.
    /// Empty means: no external delivery at all.
    #[serde(default)]
    pub allow_destinations: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Talent {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "d_talent_timeout")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Github {
    #[serde(default = "d_github_org")]
    pub organization: String,
    #[serde(default)]
    pub handoff_secret: Option<String>,
    #[serde(default = "d_handoff_age")]
    pub handoff_max_age_secs: i64,
    #[serde(default)]
    pub census_enabled: bool,
    #[serde(default)]
    pub app_id: Option<u64>,
    #[serde(default)]
    pub installation_id: Option<u64>,
    #[serde(default)]
    pub private_key_path: Option<PathBuf>,
    #[serde(default = "d_census_interval")]
    pub census_interval_secs: u64,
    #[serde(default = "d_github_api")]
    pub api_base: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Google {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    #[serde(default)]
    pub spreadsheet_id: Option<String>,
    #[serde(default)]
    pub sheet_name: Option<String>,
    #[serde(default = "d_record_id_header")]
    pub record_id_header: String,
    #[serde(default = "d_projection_interval")]
    pub projection_interval_secs: u64,
    /// Semantic projection key -> exact existing Sheet header. Empty until
    /// the canonical Sheet has been read and its headers grounded.
    #[serde(default)]
    pub field_map: BTreeMap<String, String>,
}

fn d_bind() -> String { "127.0.0.1:8799".into() }
fn d_max_event() -> usize { 1024 * 1024 }
fn d_max_blob() -> u64 { 256 * 1024 * 1024 }
fn d_call_timeout() -> u64 { 30_000 }
fn d_data_dir() -> PathBuf { PathBuf::from("data") }
fn d_max_attempts() -> i64 { 5 }
fn d_backoff() -> i64 { 500 }
fn d_worker_interval() -> u64 { 2000 }
fn d_http_timeout() -> u64 { 15_000 }
fn d_talent_timeout() -> u64 { 60_000 }
fn d_github_org() -> String { "powerfarm".into() }
fn d_handoff_age() -> i64 { 300 }
fn d_census_interval() -> u64 { 3600 }
fn d_github_api() -> String { "https://api.github.com".into() }
fn d_record_id_header() -> String { "Record ID".into() }
fn d_projection_interval() -> u64 { 60 }

impl Default for Server {
    fn default() -> Self {
        Self {
            bind: d_bind(), max_event_bytes: d_max_event(), max_blob_bytes: d_max_blob(),
            call_timeout_ms: d_call_timeout(), inspect_token: None,
        }
    }
}
impl Default for Storage {
    fn default() -> Self { Self { data_dir: d_data_dir() } }
}
impl Default for Delivery {
    fn default() -> Self {
        Self { max_attempts: d_max_attempts(), base_backoff_ms: d_backoff(), worker_interval_ms: d_worker_interval(), http_timeout_ms: d_http_timeout() }
    }
}
impl Default for Gatekeeper {
    fn default() -> Self { Self { allow_destinations: Vec::new() } }
}
impl Default for Talent {
    fn default() -> Self { Self { enabled: false, endpoint: None, model: None, timeout_ms: d_talent_timeout() } }
}
impl Default for Github {
    fn default() -> Self {
        Self {
            organization: d_github_org(),
            handoff_secret: None,
            handoff_max_age_secs: d_handoff_age(),
            census_enabled: false,
            app_id: None,
            installation_id: None,
            private_key_path: None,
            census_interval_secs: d_census_interval(),
            api_base: d_github_api(),
        }
    }
}
impl Default for Google {
    fn default() -> Self {
        Self {
            enabled: false,
            credentials_path: None,
            spreadsheet_id: None,
            sheet_name: None,
            record_id_header: d_record_id_header(),
            projection_interval_secs: d_projection_interval(),
            field_map: BTreeMap::new(),
        }
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            server: Default::default(),
            storage: Default::default(),
            delivery: Default::default(),
            gatekeeper: Default::default(),
            talent: Default::default(),
            github: Default::default(),
            google: Default::default(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        Ok(cfg)
    }

    pub fn db_path(&self) -> PathBuf { self.storage.data_dir.join("antenna.db") }
    pub fn objects_path(&self) -> PathBuf { self.storage.data_dir.join("objects") }
}
