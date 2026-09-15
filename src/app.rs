//! The one shared handle. Everything durable hangs off here; everything
//! ephemeral is explicitly marked as such.

use crate::capability::Registry;
use crate::config::Config;
use crate::returnpath::ReturnPaths;
use crate::router::Router;
use crate::storage::{Bucket, Db};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Notify;

pub struct AppInner {
    pub cfg: Config,
    pub router: Router,
    pub db: Db,
    pub bucket: Bucket,
    pub caps: Registry,
    /// EXECUTION state. Mortal. Not in SQLite, on purpose.
    pub returns: ReturnPaths,
    /// Wakes the outbox the moment work is enqueued, so a CALL does not wait
    /// for the next poll tick.
    pub outbox_notify: Notify,
    pub http: reqwest::Client,
    pub services: crate::services::ServiceRegistry,
}

#[derive(Clone)]
pub struct App(pub Arc<AppInner>);

impl Deref for App {
    type Target = AppInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl App {
    pub fn new(inner: AppInner) -> Self {
        App(Arc::new(inner))
    }
}
