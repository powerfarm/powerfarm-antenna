//! Return paths are EXECUTION state (spec §13, §15).
//!
//! A live HTTP response, an open WebSocket — these are in-memory and mortal.
//! They are deliberately NOT in SQLite. The durable half is the
//! `return_path` string on the receipt and the delivery; this registry is the
//! ephemeral half that can be missing after a restart, which is exactly the
//! condition the outbox has to reason about honestly.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

pub enum Channel {
    /// A request still holding its response open.
    Once(oneshot::Sender<Value>),
    /// A connection that may receive many messages.
    Many(mpsc::UnboundedSender<Value>),
}

#[derive(Default)]
pub struct ReturnPaths {
    inner: Mutex<HashMap<String, Channel>>,
}

/// Result of trying to hand a payload back to its origin.
pub enum Handback {
    Delivered,
    /// The channel is gone — the process restarted, or the peer left.
    /// This is terminal, not retryable: resending has nowhere to go.
    Gone,
}

impl ReturnPaths {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_once(&self, key: &str) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.inner.lock().unwrap().insert(key.to_string(), Channel::Once(tx));
        rx
    }

    pub fn register_many(&self, key: &str) -> mpsc::UnboundedReceiver<Value> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.lock().unwrap().insert(key.to_string(), Channel::Many(tx));
        rx
    }

    pub fn forget(&self, key: &str) {
        self.inner.lock().unwrap().remove(key);
    }

    pub fn send(&self, key: &str, value: Value) -> Handback {
        let mut guard = self.inner.lock().unwrap();
        match guard.get(key) {
            None => Handback::Gone,
            Some(Channel::Many(tx)) => {
                if tx.send(value).is_ok() {
                    Handback::Delivered
                } else {
                    guard.remove(key);
                    Handback::Gone
                }
            }
            Some(Channel::Once(_)) => {
                // A oneshot sender must be consumed to fire.
                match guard.remove(key) {
                    Some(Channel::Once(tx)) => {
                        if tx.send(value).is_ok() { Handback::Delivered } else { Handback::Gone }
                    }
                    _ => Handback::Gone,
                }
            }
        }
    }
}
