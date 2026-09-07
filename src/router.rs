//! The router is almost blank on purpose (spec §7).
//!
//! It knows: receipt metadata, declared policy, available capabilities.
//! It does NOT know: invoices, PDFs, GitHub, audio, microscopy, finance,
//! email, or any vendor's sludge. Capabilities understand domains.

use crate::receipt::Receipt;
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct RouteFile {
    #[serde(default)]
    pub route: Vec<Route>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Route {
    pub name: String,
    #[serde(default)]
    pub when: When,
    #[serde(rename = "do")]
    pub act: Do,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct When {
    pub transport: Option<String>,
    pub interaction: Option<String>,
    pub content_type: Option<String>,
    /// MCP JSON-RPC method, taken from transport metadata where possible.
    pub method: Option<String>,
    /// MCP tool name.
    pub tool: Option<String>,
    pub route_hint: Option<String>,
    /// `known = false` marks a fallback: it is considered only when no
    /// ordinary route matched.
    pub known: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Do {
    pub invoke: Option<String>,
    pub ask_talent: Option<String>,
    #[serde(default)]
    pub defer: bool,
    #[serde(default)]
    pub quarantine: bool,
    #[serde(default)]
    pub ignore: bool,
}

/// The only decisions the router is allowed to reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Invoke(String),
    AskTalent(String),
    Defer,
    Quarantine,
    Ignore,
    Fail(String),
}

impl Decision {
    pub fn action(&self) -> &'static str {
        match self {
            Decision::Invoke(_) => "invoke",
            Decision::AskTalent(_) => "ask_talent",
            Decision::Defer => "defer",
            Decision::Quarantine => "quarantine",
            Decision::Ignore => "ignore",
            Decision::Fail(_) => "fail",
        }
    }
    pub fn target(&self) -> Option<&str> {
        match self {
            Decision::Invoke(t) | Decision::AskTalent(t) => Some(t),
            Decision::Fail(t) => Some(t),
            _ => None,
        }
    }
}

pub struct Router {
    routes: Vec<Route>,
}

/// Metadata a transport can offer the router before any body is parsed.
/// MCP fills `method`/`tool` from headers (spec §16).
#[derive(Debug, Clone, Default)]
pub struct RouteMeta {
    pub method: Option<String>,
    pub tool: Option<String>,
}

impl Router {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading routes {}", path.display()))?;
        let file: RouteFile = toml::from_str(&text)
            .with_context(|| format!("parsing routes {}", path.display()))?;
        Ok(Self { routes: file.route })
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    fn matches(w: &When, r: &Receipt, m: &RouteMeta) -> bool {
        if let Some(t) = &w.transport {
            if !t.eq_ignore_ascii_case(&r.transport) { return false; }
        }
        if let Some(i) = &w.interaction {
            if !i.eq_ignore_ascii_case(&r.interaction) { return false; }
        }
        if let Some(ct) = &w.content_type {
            // Compare the media type only; parameters like `; charset=utf-8`
            // must not defeat a route.
            let got = r.content_type.as_deref().unwrap_or("");
            let got = got.split(';').next().unwrap_or("").trim();
            if !ct.eq_ignore_ascii_case(got) { return false; }
        }
        if let Some(meth) = &w.method {
            match &m.method {
                Some(got) if got == meth => {}
                _ => return false,
            }
        }
        if let Some(tool) = &w.tool {
            match &m.tool {
                Some(got) if got == tool => {}
                _ => return false,
            }
        }
        if let Some(hint) = &w.route_hint {
            match &r.route_hint {
                Some(got) if got.eq_ignore_ascii_case(hint) => {}
                _ => return false,
            }
        }
        true
    }

    /// One receipt in, one typed decision out.
    pub fn decide(&self, r: &Receipt, m: &RouteMeta) -> (Option<String>, Decision) {
        // Ordinary routes first, in declaration order.
        for route in self.routes.iter().filter(|x| x.when.known != Some(false)) {
            if Self::matches(&route.when, r, m) {
                return (Some(route.name.clone()), Self::act(&route.act));
            }
        }
        // Then fallbacks: nothing deterministic claimed this.
        for route in self.routes.iter().filter(|x| x.when.known == Some(false)) {
            return (Some(route.name.clone()), Self::act(&route.act));
        }
        (None, Decision::Quarantine)
    }

    fn act(d: &Do) -> Decision {
        if d.ignore { return Decision::Ignore; }
        if d.quarantine { return Decision::Quarantine; }
        if d.defer { return Decision::Defer; }
        if let Some(c) = &d.invoke { return Decision::Invoke(c.clone()); }
        if let Some(t) = &d.ask_talent { return Decision::AskTalent(t.clone()); }
        Decision::Fail("route declares no action".into())
    }
}

/// Every decision is durably recorded. Routing is inspectable after the fact.
pub fn record(
    conn: &Connection,
    receipt_id: &str,
    route_name: Option<&str>,
    decision: &Decision,
    detail: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO route_events (receipt_id, decided_at, route_name, action, target, detail)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            receipt_id,
            crate::ids::now_rfc3339(),
            route_name,
            decision.action(),
            decision.target(),
            detail
        ],
    )?;
    Ok(())
}
