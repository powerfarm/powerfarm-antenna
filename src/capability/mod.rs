//! The capability boundary (spec §9).
//!
//! A Capability is callable machinery. Internally it may be a Rust function,
//! an HTTP API, an MCP tool, a subprocess, a local model, or another Antenna.
//! The router must not care, so this trait says nothing about any of that.

pub mod builtins;
pub mod talent;

use crate::app::App;
use crate::delivery::DeliveryRequest;
use crate::receipt::Receipt;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct InvocationContext {
    pub app: App,
    pub receipt_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub correlation_id: Option<String>,
}

pub struct CapabilityInput {
    pub receipt: Receipt,
    /// Structured parameters, where the transport supplied them
    /// (e.g. MCP `tools/call` arguments). Otherwise null.
    pub params: Value,
}

pub struct CapabilityOutput {
    pub result: Value,
    /// A capability may REQUEST an outward effect. It cannot perform one.
    /// Everything here still crosses the Gatekeeper.
    pub deliveries: Vec<DeliveryRequest>,
}

impl CapabilityOutput {
    pub fn just(result: Value) -> Self {
        Self { result, deliveries: Vec::new() }
    }
}

#[async_trait]
pub trait Capability: Send + Sync {
    fn name(&self) -> &str;
    /// One line, shown through `list_capabilities` and MCP `tools/list`.
    fn description(&self) -> &str {
        ""
    }
    /// JSON Schema for MCP tool exposure. `None` means "not an MCP tool".
    fn input_schema(&self) -> Option<Value> {
        None
    }
    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput>;
}

#[derive(Default)]
pub struct Registry {
    map: HashMap<String, Arc<dyn Capability>>,
    order: Vec<String>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, c: Arc<dyn Capability>) {
        let name = c.name().to_string();
        if self.map.insert(name.clone(), c).is_none() {
            self.order.push(name);
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Capability>> {
        self.map.get(name).cloned()
    }

    pub fn names(&self) -> &[String] {
        &self.order
    }

    pub fn all(&self) -> impl Iterator<Item = &Arc<dyn Capability>> {
        self.order.iter().filter_map(|n| self.map.get(n))
    }
}
