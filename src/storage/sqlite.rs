//! SQLite with a dedicated access discipline (spec §17).
//!
//! One connection, one lock, all work handed to the blocking pool. Boring and
//! inspectable: `sqlite3 data/antenna.db` from another shell still reads it
//! (WAL), which is the point — the durable record must be readable by a human
//! without going through Antenna.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

const MIGRATION_0001: &str = include_str!("../../migrations/0001_init.sql");
const MIGRATION_0002: &str = include_str!("../../migrations/0002_github_observation.sql");

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening sqlite at {}", path.display()))?;

        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;

        conn.execute_batch(MIGRATION_0001).context("applying migration 0001")?;
        conn.execute_batch(MIGRATION_0002).context("applying migration 0002")?;

        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Run a closure against the connection on the blocking pool.
    pub async fn call<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock().map_err(|_| anyhow::anyhow!("sqlite mutex poisoned"))?;
            f(&mut guard)
        })
        .await
        .context("sqlite blocking task panicked")?
    }

    /// Synchronous access, for boot and tests.
    pub fn with<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T>,
    {
        let mut guard = self.conn.lock().map_err(|_| anyhow::anyhow!("sqlite mutex poisoned"))?;
        f(&mut guard)
    }
}
