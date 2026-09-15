//! The authority boundary (spec §12).
//!
//! Incoming text is data, never authority. A model may PROPOSE an effect; it
//! cannot grant itself permission to cause one. Every outward delivery passes
//! through here before it happens, and the decision is written onto the
//! delivery row so it can be audited later.

use crate::config::Config;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "decision", rename_all = "lowercase")]
pub enum Authority {
    Granted { reason: String },
    Denied { reason: String },
}

impl Authority {
    pub fn is_granted(&self) -> bool {
        matches!(self, Authority::Granted { .. })
    }
    pub fn reason(&self) -> &str {
        match self {
            Authority::Granted { reason } | Authority::Denied { reason } => reason,
        }
    }
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }
}

fn host_allowed(host: &str, allow: &[String]) -> bool {
    allow.iter().any(|pattern| {
        if let Some(suffix) = pattern.strip_prefix("*.") {
            host == suffix || host.ends_with(&format!(".{suffix}"))
        } else {
            host.eq_ignore_ascii_case(pattern)
        }
    })
}

/// Decide whether this destination may be reached.
pub fn authorize(cfg: &Config, destination: &str) -> Authority {
    // Answering an origin Antenna already accepted is inherent, not granted:
    // the correlation itself is the authorization.
    if destination.starts_with("return-path:") {
        return Authority::Granted { reason: "return path for an accepted receipt".into() };
    }
    // Internal hand-off. Never leaves the process.
    if destination.starts_with("capability:") {
        return Authority::Granted { reason: "internal capability hand-off".into() };
    }

    if destination.starts_with("https://") || destination.starts_with("http://") {
        let url=match reqwest::Url::parse(destination) {
            Ok(url) if url.username().is_empty() && url.password().is_none() && url.fragment().is_none()=>url,
            _=>return Authority::Denied{reason:"invalid destination URL or embedded credentials".into()},
        };
        let Some(host)=url.host_str() else {return Authority::Denied{reason:"destination has no host".into()};};
        if cfg.gatekeeper.allow_destinations.is_empty() {
            return Authority::Denied {
                reason: "no destinations are allowed by policy".into(),
            };
        }
        return if host_allowed(host, &cfg.gatekeeper.allow_destinations) {
            Authority::Granted { reason: format!("host {host} is in allow_destinations") }
        } else {
            Authority::Denied { reason: format!("host {host} is not in allow_destinations") }
        };
    }

    // Anything else — including anything that smells like shell — is refused.
    // Antenna exposes bounded actions, never unrestricted execution.
    Authority::Denied {
        reason: format!("unsupported destination scheme in {destination:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(hosts: &[&str]) -> Config {
        let mut c = Config::default();
        c.gatekeeper.allow_destinations = hosts.iter().map(|s| s.to_string()).collect();
        c
    }

    #[test]
    fn return_paths_are_inherently_authorized() {
        assert!(authorize(&cfg_with(&[]), "return-path:websocket:abc").is_granted());
    }

    #[test]
    fn unlisted_hosts_are_denied() {
        assert!(!authorize(&cfg_with(&["example.com"]), "https://evil.test/x").is_granted());
    }

    #[test]
    fn listed_hosts_are_granted() {
        assert!(authorize(&cfg_with(&["127.0.0.1"]), "http://127.0.0.1:9/sink").is_granted());
    }

    #[test]
    fn wildcards_match_subdomains_only_under_the_suffix() {
        let c = cfg_with(&["*.minilab.work"]);
        assert!(authorize(&c, "https://a.minilab.work/x").is_granted());
        assert!(authorize(&c, "https://minilab.work/x").is_granted());
        assert!(!authorize(&c, "https://notminilab.work/x").is_granted());
    }

    #[test]
    fn empty_policy_denies_all_external() {
        assert!(!authorize(&cfg_with(&[]), "https://example.com/x").is_granted());
    }

    #[test]
    fn shell_shaped_destinations_are_refused() {
        assert!(!authorize(&cfg_with(&["*"]), "sh:rm -rf /").is_granted());
    }

    #[test]
    fn userinfo_cannot_disguise_an_unlisted_destination() {
        assert!(!authorize(&cfg_with(&["allowed.test"]),"https://allowed.test:password@evil.test/path").is_granted());
    }
}
