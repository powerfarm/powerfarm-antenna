//! Contract admission and the adapter to the supplied Continuity graph compiler.
//! Registry owns contracts. This process holds only expiring compiled authority.
use crate::{
    app::App,
    capability::{Capability, CapabilityInput, CapabilityOutput, InvocationContext},
    delivery::DeliveryRow,
    receipt::Receipt,
};
use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, process::Stdio};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};

const VERIFIED: &str = "antenna-verified-service";

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    #[serde(default)]
    pub enabled: bool,
    pub registry_url: Option<String>,
    pub publishable_key: Option<String>,
    pub credential_path: Option<PathBuf>,
    pub identity_id: Option<String>,
    pub python: Option<PathBuf>,
    pub worker_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Terms {
    pub max_bytes: usize,
    pub destinations: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub schema: String,
    pub title: String,
    pub transport: String,
    pub capabilities: Vec<String>,
    pub graph: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub sha256: String,
    pub valid_until: Option<DateTime<Utc>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub contract_id: String,
    pub name: String,
    pub terms_sha256: String,
    pub service_id: String,
    pub service_sha256: String,
    pub client_id: String,
    pub terms: Terms,
    pub definition: Definition,
    pub definition_sha256: String,
    pub valid_until: DateTime<Utc>,
    pub credentials: Vec<Credential>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema: String,
    pub audience: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub bindings: Vec<Binding>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceContext {
    pub contract_id: String,
    pub terms_sha256: String,
    pub definition_sha256: String,
    pub credential_sha256: String,
}

pub struct ServiceRegistry {
    snapshot: RwLock<Option<Snapshot>>,
    workers: tokio::sync::Semaphore,
}
impl Default for ServiceRegistry {
    fn default()->Self {Self{snapshot:RwLock::new(None),workers:tokio::sync::Semaphore::new(4)}}
}
impl ServiceRegistry {
    pub async fn health(&self)->Value {
        let snapshot=self.snapshot.read().await;
        match snapshot.as_ref() {
            Some(s)=>json!({"status":if s.expires_at>Utc::now(){"current"}else{"expired"},"active_client_contracts":s.bindings.len(),"expires_at":s.expires_at}),
            None=>json!({"status":"unavailable","active_client_contracts":0}),
        }
    }
    pub async fn install(
        &self,
        payload: &str,
        signature: &str,
        token: &str,
        audience: &str,
    ) -> Result<()> {
        let mut mac = Hmac::<Sha256>::new_from_slice(token.as_bytes())?;
        mac.update(payload.as_bytes());
        mac.verify_slice(&hex::decode(signature)?)?;
        let next: Snapshot = serde_json::from_str(payload)?;
        if next.schema != "antenna.snapshot.v1"
            || next.audience != audience
            || next.expires_at <= Utc::now()
            || next.expires_at - next.issued_at > chrono::Duration::seconds(60)
            || next.issued_at > Utc::now() + chrono::Duration::seconds(5)
        {
            bail!("invalid snapshot audience or lifetime");
        }
        for b in &next.bindings {
            if b.terms.max_bytes == 0
                || b.terms.max_bytes > 1048576
                || b.definition.schema != "antenna.service.v1"
                || !["http", "webhook", "websocket", "sse"]
                    .contains(&b.definition.transport.as_str())
                || b.definition.capabilities.iter().any(|c| {
                    ![
                        "echo",
                        "object.store",
                        "document.inspect",
                        "delivery.create",
                    ]
                    .contains(&c.as_str())
                })
            {
                bail!("unsupported contract definition or limits");
            }
            for destination in &b.terms.destinations {
                let u = reqwest::Url::parse(destination)?;
                if !["http", "https"].contains(&u.scheme())
                    || u.host_str().is_none()
                    || !u.username().is_empty()
                    || u.password().is_some()
                    || u.fragment().is_some()
                {
                    bail!("invalid contract destination");
                }
            }
        }
        let mut current = self.snapshot.write().await;
        if current
            .as_ref()
            .is_some_and(|s| s.issued_at > next.issued_at)
        {
            bail!("stale snapshot");
        }
        *current = Some(next);
        Ok(())
    }
    async fn binding(&self, name: &str, digest: &str) -> Result<Binding> {
        let guard = self.snapshot.read().await;
        let s = guard
            .as_ref()
            .ok_or_else(|| anyhow!("contracts unavailable"))?;
        let now = Utc::now();
        if s.expires_at <= now {
            bail!("contract snapshot expired");
        }
        s.bindings
            .iter()
            .find(|b| {
                (b.name == name || b.contract_id == name)
                    && b.valid_until > now
                    && b.credentials
                        .iter()
                        .any(|k| k.sha256 == digest && k.valid_until.is_none_or(|t| t > now))
            })
            .cloned()
            .ok_or_else(|| anyhow!("contract absent, expired, revoked or credential not admitted"))
    }
    pub async fn authorize(&self, ctx: &ServiceContext) -> Result<Binding> {
        let b = self
            .binding(&ctx.contract_id, &ctx.credential_sha256)
            .await?;
        if b.terms_sha256 != ctx.terms_sha256 || b.definition_sha256 != ctx.definition_sha256 {
            bail!("contract or program revision changed");
        }
        Ok(b)
    }
}

pub async fn refresh(app: &App) -> Result<()> {
    let cfg = &app.cfg.services;
    let token = tokio::fs::read_to_string(
        cfg.credential_path
            .as_ref()
            .context("service credential_path required")?,
    )
    .await?;
    let token = token.trim();
    let url = cfg
        .registry_url
        .as_deref()
        .context("service registry_url required")?;
    if !url.starts_with("https://") {
        bail!("registry_url requires HTTPS");
    }
    let response = app
        .http
        .post(format!(
            "{}/rest/v1/rpc/powerfarm_antenna_snapshot",
            url.trim_end_matches('/')
        ))
        .header(
            "apikey",
            cfg.publishable_key
                .as_deref()
                .context("publishable_key required")?,
        )
        .json(&json!({"p_token":token}))
        .send()
        .await?
        .error_for_status()?;
    let envelope: Value = response.json().await?;
    app.services
        .install(
            envelope["payload"]
                .as_str()
                .context("snapshot payload missing")?,
            envelope["hmac_sha256"]
                .as_str()
                .context("snapshot signature missing")?,
            token,
            cfg.identity_id.as_deref().context("identity_id required")?,
        )
        .await
}
pub async fn worker(app: App) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        if let Err(error) = refresh(&app).await {
            tracing::warn!(error=%error,"service snapshot refresh failed; cached authority expires within 60 seconds");
            continue;
        }
        // Recover admitted work which waited for Registry availability at boot.
        if let Ok(pending)=app.db.call(|c|crate::receipt::unfinished(c)).await {
            for receipt in pending {
                let Ok(Some(context))=context(&receipt) else {continue;};
                if app.services.authorize(&context).await.is_err() {continue;}
                let worker_app=app.clone();
                tokio::spawn(async move {
                    let meta=if receipt.transport=="mcp" {
                        let body=receipt.json(&worker_app.bucket).await.unwrap_or(Value::Null);
                        crate::router::RouteMeta{method:body["method"].as_str().map(String::from),tool:body["params"]["name"].as_str().map(String::from)}
                    } else {Default::default()};
                    crate::journey::process(&worker_app,&receipt.id,meta).await;
                });
            }
        }
    }
}

pub async fn admit(State(app): State<App>, mut req: Request, next: Next) -> Response {
    // Never accept a caller-supplied proof header, even on compatibility aliases.
    req.headers_mut().remove(VERIFIED);
    let Some(name) = crate::ingress::header(req.headers(), "antenna-contract") else {
        return next.run(req).await;
    };
    let offered_token = crate::ingress::header(req.headers(), "authorization");
    let websocket = req
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let wants_sse = req
        .headers()
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|s| s.trim() == "text/event-stream"));
    if !websocket && req.method() != axum::http::Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if !["/", "/ingress", "/mcp", "/ws"].contains(&req.uri().path()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"use the contract service root"})),
        )
            .into_response();
    }
    let result = async {
        if !app.cfg.services.enabled {
            bail!("contract services are disabled");
        }
        let token = offered_token.context("client credential required")?;
        let token = token
            .strip_prefix("Bearer ")
            .context("Bearer credential required")?;
        let digest = hex::encode(Sha256::digest(token.as_bytes()));
        let b = app.services.binding(&name, &digest).await?;
        if websocket != (b.definition.transport == "websocket") {
            bail!("transport does not match service contract");
        }
        if (b.definition.transport == "sse") != wants_sse {
            bail!("streaming service requires Accept: text/event-stream");
        }
        let context = ServiceContext {
            contract_id: b.contract_id,
            terms_sha256: b.terms_sha256,
            definition_sha256: b.definition_sha256,
            credential_sha256: digest,
        };
        Ok::<_, anyhow::Error>((context, b.terms.max_bytes))
    }
    .await;
    let (context, limit) = match result {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::FORBIDDEN, Json(json!({"error":e.to_string()}))).into_response()
        }
    };
    let (mut parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, limit).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({"error":"contract payload limit exceeded"})),
            )
                .into_response()
        }
    };
    parts.headers.insert(
        VERIFIED,
        HeaderValue::from_str(&serde_json::to_string(&context).unwrap()).unwrap(),
    );
    let request = Request::from_parts(parts, axum::body::Body::from(body));
    if wants_sse {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            let response = next.run(request).await;
            let status = response.status().as_u16();
            let value = match axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024).await {
                Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                    .unwrap_or(json!({"error":"non-JSON service response"})),
                Err(_) => json!({"error":"service result exceeds streaming limit"}),
            };
            let _ = tx
                .send(json!({"http_status":status,"response":value}))
                .await;
        });
        let stream = futures_util::stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|value| {
                let event = axum::response::sse::Event::default()
                    .event("result")
                    .data(value.to_string());
                (Ok::<_, std::convert::Infallible>(event), rx)
            })
        });
        return axum::response::Sse::new(stream)
            .keep_alive(axum::response::sse::KeepAlive::default())
            .into_response();
    }
    next.run(request).await
}

pub fn relationships(headers: &HeaderMap, existing: Value) -> Option<String> {
    let mut value = existing;
    if !value.is_object() {
        value = json!({});
    }
    if let Some(context) = crate::ingress::header(headers, VERIFIED)
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
    {
        value["service"] = context;
    }
    (value != json!({})).then(|| value.to_string())
}
pub fn context(receipt: &Receipt) -> Result<Option<ServiceContext>> {
    let value: Value = match &receipt.relationships {
        Some(s) => serde_json::from_str(s)?,
        None => return Ok(None),
    };
    value
        .get("service")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}
pub fn is_service(headers: &HeaderMap) -> bool {
    headers.contains_key(VERIFIED)
}
pub async fn check_frame(app: &App, headers: &HeaderMap, size: usize) -> Result<()> {
    if let Some(raw) = crate::ingress::header(headers, VERIFIED) {
        let context: ServiceContext = serde_json::from_str(&raw)?;
        let b = app.services.authorize(&context).await?;
        if size > b.terms.max_bytes {
            bail!("frame exceeds client contract");
        }
    }
    Ok(())
}
pub async fn admit_mcp(app: &App, headers: &mut HeaderMap, name: &str) -> Result<()> {
    if !app.cfg.services.enabled {
        bail!("contract services are disabled");
    }
    let token =
        crate::ingress::header(headers, "authorization").context("client credential required")?;
    let token = token
        .strip_prefix("Bearer ")
        .context("Bearer credential required")?;
    let digest = hex::encode(Sha256::digest(token.as_bytes()));
    let b = app.services.binding(name, &digest).await?;
    let proof = ServiceContext {
        contract_id: b.contract_id,
        terms_sha256: b.terms_sha256,
        definition_sha256: b.definition_sha256,
        credential_sha256: digest,
    };
    if let Some(previous) = crate::ingress::header(headers, VERIFIED) {
        let previous: ServiceContext = serde_json::from_str(&previous)?;
        if previous.contract_id != proof.contract_id {
            bail!("MCP contract differs from transport contract");
        }
    }
    headers.insert(
        VERIFIED,
        HeaderValue::from_str(&serde_json::to_string(&proof)?)?,
    );
    Ok(())
}

pub async fn authorize_delivery(app: &App, receipt: &Receipt, d: &DeliveryRow) -> Result<()> {
    if let Some(ctx) = context(receipt)? {
        // Returning an already-produced result discloses only to the admitted
        // connection. Every external delivery still requires current authority.
        if d.transport == "return-path" {
            return Ok(());
        }
        let b = app.services.authorize(&ctx).await?;
        if !b.terms.destinations.contains(&d.destination) {
            bail!("destination outside client contract");
        }
    }
    Ok(())
}

async fn advance(app: &App, request: Value) -> Result<Value> {
    let cfg = &app.cfg.services;
    let mut child =
        tokio::process::Command::new(cfg.python.as_ref().context("Continuity python required")?)
            .arg(
                cfg.worker_path
                    .as_ref()
                    .context("Continuity worker_path required")?,
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
    let bytes = serde_json::to_vec(&request)?;
    let mut stdin = child.stdin.take().context("worker stdin")?;
    let write = tokio::spawn(async move {
        stdin.write_all(&bytes).await?;
        stdin.shutdown().await
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut output = Vec::new();
        child
            .stdout
            .take()
            .context("worker stdout")?
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut output)
            .await?;
        if output.len() > 4 * 1024 * 1024 {
            child.kill().await?;
            bail!("graph result exceeds 4 MiB");
        }
        let status = child.wait().await?;
        let value: Value =
            serde_json::from_slice(&output).context("invalid Continuity response")?;
        if !status.success() {
            bail!(
                "Continuity: {}",
                value["error"].as_str().unwrap_or("worker failed")
            );
        }
        Ok(value)
    })
    .await
    .context("Continuity step timeout")?;
    write.await??;
    result
}

pub struct InvokeService;
#[async_trait::async_trait]
impl Capability for InvokeService {
    fn name(&self) -> &str {
        "service.invoke"
    }
    fn description(&self) -> &str {
        "Execute a graph under an accepted service and client contract."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(
            json!({"type":"object","properties":{"contract":{"type":"string"},"input":{"type":"object"},"receipt_id":{"type":"string","description":"Inspect a prior invocation instead of starting another."}},"required":["contract"],"additionalProperties":false}),
        )
    }
    async fn invoke(
        &self,
        ctx: InvocationContext,
        input: CapabilityInput,
    ) -> Result<CapabilityOutput> {
        let _permit=ctx.app.services.workers.acquire().await?;
        let proof = context(&input.receipt)?.context("verified service contract required")?;
        let b = ctx.app.services.authorize(&proof).await?;
        if input.receipt.transport == "mcp" {
            if let Some(rid) = input.params.get("receipt_id").and_then(Value::as_str) {
                let rid = rid.to_string();
                let (receipt, runs, deliveries) = ctx
                    .app
                    .db
                    .call(move |c| {
                        let receipt =
                            crate::receipt::load(c, &rid)?.context("receipt not found")?;
                        let runs = crate::run::for_receipt(c, &rid)?;
                        let deliveries = crate::delivery::recent(c, 1000)?
                            .into_iter()
                            .filter(|d| d.receipt_id.as_deref() == Some(&rid))
                            .collect::<Vec<_>>();
                        Ok((receipt, runs, deliveries))
                    })
                    .await?;
                if context(&receipt)?.as_ref().map(|c| c.contract_id.as_str())
                    != Some(&proof.contract_id)
                {
                    bail!("receipt belongs to another contract");
                }
                return Ok(CapabilityOutput::just(
                    json!({"receipt_id":receipt.id,"status":receipt.status,"runs":runs,"deliveries":deliveries}),
                ));
            }
        }
        let payload = if input.receipt.transport == "mcp" {
            input
                .params
                .get("input")
                .filter(|v| v.is_object())
                .cloned()
                .context("service MCP input object required")?
        } else {
            input
                .receipt
                .json(&ctx.app.bucket)
                .await
                .context("service input must be JSON")?
        };
        let checkpoint = ctx.app.cfg.storage.data_dir.join("continuity.db");
        let mut request = json!({"graph":b.definition.graph,"capabilities":b.definition.capabilities,
            "receipt_id":ctx.receipt_id,"checkpoint":checkpoint,"input":payload});
        for _ in 0..65 {
            ctx.app.services.authorize(&proof).await?;
            let step = advance(&ctx.app, request.clone()).await?;
            if step["status"] == "completed" {
                return Ok(CapabilityOutput {
                    result: json!({"capability":"service.invoke","contract_id":b.contract_id,
                    "contract_sha256":b.terms_sha256,"definition_sha256":b.definition_sha256,
                    "result":step["result"],"outputs":step["outputs"]}),
                    deliveries: serde_json::from_value(step["deliveries"].clone())?,
                });
            }
            let name = step["capability"]
                .as_str()
                .context("graph capability missing")?;
            if !b.definition.capabilities.iter().any(|c| c == name) {
                bail!("graph capability exceeds contract");
            }
            let params = step["params"].clone();
            if name == "delivery.create" {
                let destination = params["destination"]
                    .as_str()
                    .context("destination required")?;
                if !b.terms.destinations.iter().any(|d| d == destination) {
                    bail!("destination outside client contract");
                }
                if !crate::gatekeeper::authorize(&ctx.app.cfg, destination).is_granted() {
                    bail!("destination outside host policy");
                }
            }
            if serde_json::to_vec(&params)?.len() > b.terms.max_bytes {
                bail!("node parameters exceed client contract");
            }
            let cap = ctx.app.caps.get(name).context("capability not installed")?;
            let inner = InvocationContext {
                app: ctx.app.clone(),
                receipt_id: ctx.receipt_id.clone(),
                run_id: ctx.run_id.clone(),
                trace_id: ctx.trace_id.clone(),
                span_id: crate::ids::new_span_id(),
                correlation_id: ctx.correlation_id.clone(),
            };
            let mut output = cap
                .invoke(
                    inner,
                    CapabilityInput {
                        receipt: input.receipt.clone(),
                        params,
                    },
                )
                .await?;
            for delivery in &mut output.deliveries {
                delivery.idempotency_key = format!(
                    "service:{}:{}",
                    ctx.receipt_id,
                    step["node"].as_str().context("node missing")?
                );
            }
            request["resume"] =
                json!({"node":step["node"],"result":output.result,"deliveries":output.deliveries});
        }
        bail!("service graph exceeded 64 steps")
    }
}
