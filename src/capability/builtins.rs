//! Deterministic capabilities.
//!
//! These are the "code absorbs the known" half of the metabolism (spec §11).
//! None of them calls a model. None of them can cause an effect directly.

use super::{Capability, CapabilityInput, CapabilityOutput, InvocationContext};
use crate::delivery::DeliveryRequest;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{json, Value};

/// Sniff a media type from leading bytes. Deliberately small: this answers
/// "what mechanically is this", not "what does it mean".
fn sniff(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"%PDF-") { return "application/pdf"; }
    if bytes.starts_with(&[0x89, b'P', b'N', b'G']) { return "image/png"; }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) { return "image/jpeg"; }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") { return "image/gif"; }
    if bytes.starts_with(b"PK\x03\x04") { return "application/zip"; }
    if bytes.len() > 11 && &bytes[4..8] == b"ftyp" { return "video/mp4"; }
    if bytes.starts_with(b"\x1f\x8b") { return "application/gzip"; }
    let head = &bytes[..bytes.len().min(512)];
    if serde_json::from_slice::<Value>(bytes).is_ok() { return "application/json"; }
    if std::str::from_utf8(head).is_ok() { return "text/plain"; }
    "application/octet-stream"
}

// ---------------------------------------------------------------- echo ----

/// The smallest honest capability: it reports faithfully what arrived.
pub struct Echo;

#[async_trait]
impl Capability for Echo {
    fn name(&self) -> &str { "echo" }
    fn description(&self) -> &str { "Report faithfully what was received, without interpreting it." }

    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput> {
        let raw = input.receipt.raw(&ctx.app.bucket).await?;
        // The payload's *content* is never asserted as true. We report only
        // that these bytes arrived, and what they parse as.
        let parsed: Option<Value> = serde_json::from_slice(&raw).ok();
        Ok(CapabilityOutput::just(json!({
            "capability": "echo",
            "received_bytes": raw.len(),
            "digest": input.receipt.raw_digest,
            "content_type": input.receipt.content_type,
            "parsed_as": if parsed.is_some() { "json" } else { "opaque" },
            "payload": match parsed {
                Some(v) => v,
                None => Value::String(String::from_utf8_lossy(&raw[..raw.len().min(4096)]).to_string()),
            },
        })))
    }
}

// ---------------------------------------------------- document.inspect ----

/// Observe a stored object. Mechanics only — size, digest, shape.
pub struct DocumentInspect;

#[async_trait]
impl Capability for DocumentInspect {
    fn name(&self) -> &str { "document.inspect" }
    fn description(&self) -> &str { "Observe a stored object: size, digest, sniffed type, coarse structure." }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": { "digest": { "type": "string", "description": "blake3 digest of a stored object" } },
            "required": []
        }))
    }

    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput> {
        let digest = input
            .params
            .get("digest")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| input.receipt.raw_digest.clone())
            .ok_or_else(|| anyhow!("no digest to inspect"))?;

        let bucket = &ctx.app.bucket;
        let size = bucket.size_of(&digest).unwrap_or(0);
        let head = bucket.head_bytes(&digest, 8192).unwrap_or_default();
        let sniffed = sniff(&head);

        let declared = input.receipt.content_type.clone();
        let mut observations = json!({
            "digest": digest,
            "size_bytes": size,
            "sniffed_content_type": sniffed,
            "declared_content_type": declared,
        });

        // Coarse structure. Cheap, deterministic, no model.
        match sniffed {
            "application/pdf" => {
                let whole = bucket.get(&digest).await.map(|b| b.to_vec()).unwrap_or_default();
                let page_markers = count_occurrences(&whole, b"/Type /Page")
                    + count_occurrences(&whole, b"/Type/Page");
                let version = String::from_utf8_lossy(&head[..head.len().min(8)]).to_string();
                observations["structure"] = json!({
                    "kind": "pdf",
                    "header": version.trim().to_string(),
                    "page_markers": page_markers,
                    // Said plainly, because the number is a scan, not a parse.
                    // A real page count needs a PDF capability, not the router.
                    "note": "page_markers is a byte-scan heuristic, not a parsed page count"
                });
            }
            "application/json" => {
                let whole = bucket.get(&digest).await.map(|b| b.to_vec()).unwrap_or_default();
                let v: Value = serde_json::from_slice(&whole).unwrap_or(Value::Null);
                observations["structure"] = json!({
                    "kind": "json",
                    "top_level": match &v {
                        Value::Object(m) => json!(m.keys().collect::<Vec<_>>()),
                        Value::Array(a) => json!({ "array_len": a.len() }),
                        _ => json!(v),
                    }
                });
            }
            "text/plain" => {
                let text = String::from_utf8_lossy(&head);
                observations["structure"] = json!({
                    "kind": "text",
                    "sampled_bytes": head.len(),
                    "sampled_lines": text.lines().count(),
                    "first_line": text.lines().next().unwrap_or("").chars().take(200).collect::<String>(),
                });
            }
            _ => {
                observations["structure"] = json!({ "kind": "opaque" });
            }
        }

        Ok(CapabilityOutput::just(json!({
            "capability": "document.inspect",
            "observation": observations
        })))
    }
}

fn count_occurrences(hay: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() || hay.len() < needle.len() { return 0; }
    hay.windows(needle.len()).filter(|w| *w == needle).count()
}

// -------------------------------------------------------- object.store ----

pub struct ObjectStore;

#[async_trait]
impl Capability for ObjectStore {
    fn name(&self) -> &str { "object.store" }
    fn description(&self) -> &str { "Store a payload in the bucket and return its content address." }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "content": { "type": "string" },
                "content_type": { "type": "string" }
            },
            "required": ["content"]
        }))
    }

    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput> {
        let content = input
            .params
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("object.store requires `content`"))?;
        let content_type = input
            .params
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("text/plain")
            .to_string();

        let bytes = bytes::Bytes::from(content.as_bytes().to_vec());
        let size = bytes.len() as i64;
        let digest = ctx.app.bucket.put_bytes(bytes).await?;

        let d2 = digest.clone();
        let ct = content_type.clone();
        ctx.app
            .db
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO objects (digest, path, size, content_type, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        d2,
                        crate::storage::objects::object_path(&d2).to_string(),
                        size,
                        ct,
                        crate::ids::now_rfc3339()
                    ],
                )?;
                Ok(())
            })
            .await?;

        Ok(CapabilityOutput::just(json!({
            "capability": "object.store",
            "digest": digest,
            "size_bytes": size,
            "content_type": content_type
        })))
    }
}

// ------------------------------------------------------ delivery.create ----

/// A bounded outward action. This is where the authority boundary is most
/// visible: anything — including a model — may PROPOSE a delivery here, and
/// the Gatekeeper still decides whether it may happen (spec §12).
pub struct DeliveryCreate;

#[async_trait]
impl Capability for DeliveryCreate {
    fn name(&self) -> &str { "delivery.create" }
    fn description(&self) -> &str { "Propose an outward delivery. Subject to the authority boundary." }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "destination": { "type": "string", "description": "http(s) URL" },
                "payload": { "type": "object" }
            },
            "required": ["destination", "payload"]
        }))
    }

    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput> {
        // Structured params where the transport supplied them (MCP), the
        // received body otherwise (plain HTTP). Same capability either way.
        let params = if input.params.is_null() {
            input.receipt.json(&ctx.app.bucket).await.unwrap_or(Value::Null)
        } else {
            input.params.clone()
        };

        let destination = params
            .get("destination")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("delivery.create requires `destination`"))?
            .to_string();
        let payload = params.get("payload").cloned().unwrap_or(Value::Null);

        // The decision is reported back, but it is NOT taken here — the
        // outbox re-checks before the effect. This is only a preview.
        let preview = crate::gatekeeper::authorize(&ctx.app.cfg, &destination);
        let destination_key = destination.clone();

        Ok(CapabilityOutput {
            result: json!({
                "capability": "delivery.create",
                "destination": destination,
                "authority_preview": preview,
                "note": "authority is re-checked at attempt time; this is a proposal, not a grant"
            }),
            deliveries: vec![DeliveryRequest {
                destination,
                payload,
                content_type: "application/json".into(),
                // Keyed on the RECEIPT, not the run. A restart re-routes the
                // receipt and creates a new run; keying on the run would let
                // recovery duplicate the outward effect. The receipt is the
                // thing that happened once.
                idempotency_key: format!(
                    "proposed:{}:{}",
                    ctx.receipt_id,
                    &blake3::hash(destination_key.as_bytes()).to_hex()[..16]
                ),
            }],
        })
    }
}

// ---------------------------------------------------- capabilities.list ----

pub struct CapabilitiesList;

#[async_trait]
impl Capability for CapabilitiesList {
    fn name(&self) -> &str { "capabilities.list" }
    fn description(&self) -> &str { "List the capabilities this Antenna can invoke." }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({ "type": "object", "properties": {} }))
    }

    async fn invoke(&self, ctx: InvocationContext, _input: CapabilityInput) -> Result<CapabilityOutput> {
        let caps: Vec<Value> = ctx
            .app
            .caps
            .all()
            .map(|c| json!({ "name": c.name(), "description": c.description() }))
            .collect();
        Ok(CapabilityOutput::just(json!({
            "capability": "capabilities.list",
            "capabilities": caps
        })))
    }
}

// ------------------------------------------------------- receipts.query ----

pub struct ReceiptsQuery;

#[async_trait]
impl Capability for ReceiptsQuery {
    fn name(&self) -> &str { "receipts.query" }
    fn description(&self) -> &str { "Query recent receipts. Bounded: metadata only, capped count." }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": { "limit": { "type": "integer", "minimum": 1, "maximum": 200 } }
        }))
    }

    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput> {
        let limit = input
            .params
            .get("limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(20)
            .clamp(1, 200);
        let rows = ctx.app.db.call(move |c| crate::receipt::recent(c, limit)).await?;
        let out: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "id": r.id, "recorded_at": r.recorded_at, "transport": r.transport,
                    "interaction": r.interaction, "content_type": r.content_type,
                    "content_length": r.content_length, "digest": r.raw_digest,
                    "status": r.status
                })
            })
            .collect();
        Ok(CapabilityOutput::just(json!({
            "capability": "receipts.query", "count": out.len(), "receipts": out
        })))
    }
}

// -------------------------------------------------------- mcp.dispatch ----

/// Maps an MCP tool name onto a capability. The router reached here from
/// transport metadata alone; this is the first place the body is read.
pub struct McpDispatch;

/// The bounded action surface exposed over MCP (spec §12).
/// Note what is absent: anything resembling shell execution.
pub fn mcp_tool_map(tool: &str) -> Option<&'static str> {
    Some(match tool {
        "invoke_service" => "service.invoke",
        "inspect_document" => "document.inspect",
        "store_object" => "object.store",
        "create_delivery" => "delivery.create",
        "list_capabilities" => "capabilities.list",
        "query_receipts" => "receipts.query",
        "request_analysis" => "ingress.interpreter",
        _ => return None,
    })
}

pub const MCP_TOOLS: &[(&str, &str)] = &[
    ("invoke_service", "service.invoke"),
    ("inspect_document", "document.inspect"),
    ("store_object", "object.store"),
    ("create_delivery", "delivery.create"),
    ("list_capabilities", "capabilities.list"),
    ("query_receipts", "receipts.query"),
    ("request_analysis", "ingress.interpreter"),
];

#[async_trait]
impl Capability for McpDispatch {
    fn name(&self) -> &str { "mcp.dispatch" }
    fn description(&self) -> &str { "Route an MCP tools/call onto a capability." }

    async fn invoke(&self, ctx: InvocationContext, input: CapabilityInput) -> Result<CapabilityOutput> {
        let body = input
            .receipt
            .json(&ctx.app.bucket)
            .await
            .ok_or_else(|| anyhow!("mcp.dispatch: body is not JSON"))?;

        let tool = body
            .pointer("/params/name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("mcp.dispatch: missing params.name"))?;
        let args = body
            .pointer("/params/arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let cap_name = mcp_tool_map(tool)
            .ok_or_else(|| anyhow!("unknown tool {tool:?}"))?;
        let cap = ctx
            .app
            .caps
            .get(cap_name)
            .ok_or_else(|| anyhow!("tool {tool:?} maps to unregistered capability {cap_name:?}"))?;

        let inner = InvocationContext {
            app: ctx.app.clone(),
            receipt_id: ctx.receipt_id.clone(),
            run_id: ctx.run_id.clone(),
            trace_id: ctx.trace_id.clone(),
            span_id: crate::ids::new_span_id(),
            correlation_id: ctx.correlation_id.clone(),
        };
        let out = cap
            .invoke(inner, CapabilityInput { receipt: input.receipt, params: args })
            .await?;

        Ok(CapabilityOutput {
            result: json!({ "capability": "mcp.dispatch", "tool": tool, "result": out.result }),
            deliveries: out.deliveries,
        })
    }
}
