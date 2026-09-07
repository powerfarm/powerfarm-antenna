# ANTENNA_SPEC.md

The invariants and schemas that survived implementation. The mounting spec was
the construction prompt; this is what the code actually guarantees.

Antenna v0.1.0 · LAB 8GB · `antenna.minilab.work`

---

## 1. What a receipt asserts

`antenna:receipt` asserts exactly one thing:

> Antenna received these bytes, at this time, over this transport, from this source.

It does **not** assert that the content is true. A payload reading
`{"claim":"Mars owes me five euros"}` produces a receipt whose only truth is
that the payload arrived. Interpretation adds `runs` and observations; it never
rewrites `raw_body`, `raw_ref` or `raw_digest`.

## 2. The durability boundary

```
receive → bound → digest → preserve → COMMIT → (only then) acknowledge, route, interpret
```

Nothing is acknowledged to the world before the receipt is committed to SQLite
with `synchronous=FULL`. This is enforced structurally: ingress handlers call
`journey::accept` and cannot reach `journey::process` without it.

The forbidden shape — `receive → LLM → maybe store later` — is not reachable:
no ingress path can invoke a capability without a committed receipt id.

## 3. Interaction lanes

Mechanics are classified before meaning. These are independent of content type.

| lane     | meaning                             | where the bytes live      |
|----------|-------------------------------------|---------------------------|
| `event`  | small signal, no answer expected    | SQLite `raw_body`         |
| `blob`   | file / large body                   | bucket; SQLite holds ref  |
| `stream` | websocket message                   | SQLite `raw_body`         |
| `call`   | sender is waiting for an answer     | either; return path req'd |

An MCP `tools/call` is lane `call`, content type JSON, transport `mcp`. Three
independent axes; never conflated.

## 4. Router law

Input: receipt metadata + declared policy + available capabilities.
Output: exactly one of

```
Invoke(capability) · AskTalent(capability) · Defer · Quarantine · Ignore · Fail
```

The router has no domain knowledge. It cannot parse a PDF, understand an
invoice, or recognise a vendor. `routes/default.toml` is configuration, not a
language: it has no expressions, no conditionals, no loops. When a decision
needs logic, the logic moves into a named Capability.

Every decision is written to `route_events` before it is acted on.

## 5. Authority boundary

```
payload → receipt → interpretation → proposed typed action → Gatekeeper → effect
```

- Incoming text is **data**, never authority.
- A capability may **request** a Delivery. It cannot perform one.
- Authority is checked twice: at enqueue, and again immediately before the
  attempt. The decision is stored on the delivery row as JSON.
- `return-path:` and `capability:` destinations are inherently authorized —
  answering an origin Antenna already accepted is not a new power.
- Every other destination must match `gatekeeper.allow_destinations`.
  Empty list ⇒ no external delivery at all.
- There is no shell capability, and no MCP tool that executes arbitrary code.
  The exposed surface is bounded: `inspect_document`, `store_object`,
  `create_delivery`, `list_capabilities`, `query_receipts`, `request_analysis`.

A denied delivery is **recorded with `attempt = 0`** — refused, never tried.

## 6. Duplicate-effect protection

`deliveries.idempotency_key` is `UNIQUE`. Re-enqueueing the same logical effect
returns the existing row.

Keys are derived from the **receipt**, never the run:

```
return-path answer   return:<correlation_id>
proposed delivery    proposed:<receipt_id>:<blake3(destination)[..16]>
```

This is load-bearing. A restart re-routes the receipt and creates a *new* run;
keying on the run would let crash recovery duplicate the outward effect.

Outbound HTTP deliveries also send `Idempotency-Key` so the receiver can
collapse a duplicate that Antenna could not rule out.

## 7. Crash semantics

On boot, in order:

1. `runs` left `running` → closed as `interrupted`; their receipts return to the
   routing worklist.
2. `deliveries` left `in_flight` → the prior attempt's outcome is **unknown**.
   Marked `resumed_uncertain = 1` and retried under the same idempotency key.
3. `receipts` still `accepted` → routed now.

Recovery runs **before** the listener binds. New work is never admitted ahead of
work already accepted.

`resumed_uncertain` is never cleared. That a delivery was once resumed across a
crash is a permanent fact about the record; completing it later does not make
the earlier uncertainty untrue.

A return-path delivery whose channel is gone is settled `orphaned` — terminal,
not retried. Retrying has nowhere to go, and inventing a destination would be a
fabrication.

## 8. State / memory / execution

Kept apart deliberately. There is no table called `messages`.

| | |
|---|---|
| **STATE** | `antenna.toml`, `routes/*.toml`, gatekeeper policy — read at boot, never mutated by traffic |
| **MEMORY** | `receipts`, `objects`, `runs`, `deliveries`, `delivery_attempts`, `route_events` |
| **EXECUTION** | live WebSockets, open HTTP responses, in-flight requests — in RAM, mortal, absent from SQLite by design |

The `ReturnPaths` registry is execution state. Its absence after a restart is
not a bug to paper over; it is the condition the outbox reasons about.

## 9. Storage

**SQLite** — one database, WAL, `synchronous=FULL`, foreign keys on, 10s busy
timeout, one connection behind one lock, all work on the blocking pool. Readable
concurrently by `sqlite3` from another shell, which is the point: the durable
record must be inspectable by a human without going through Antenna.

**Bucket** — content-addressed, `blake3(payload) → objects/ab/cd/<digest>`,
behind `object_store` so the backend can become S3/MinIO/R2 without Receipt
semantics changing. Large uploads stream through `objects/staging/` and are
renamed into place: a 256 MiB blob costs kilobytes of RAM, not megabytes.

Identical bytes collapse to one object. Two arrivals are still two receipts —
two facts.

## 10. Metabolism

```
1. deterministic code   2. known Capability   3. cheap local intelligence
4. stronger remote intelligence                5. human escalation
```

No model is required to boot, and none is required to answer. With
`talent.enabled = false` an unrouted payload **defers** rather than failing:
the receipt is durable and can be re-routed once a talent exists. Unknown input
is allowed to take time.

Whatever a talent returns is an **observation** — a proposal — recorded as such.
It is never authority and never reaches an effect without the Gatekeeper.

## 11. Return paths

```
HTTP call    active-response:<correlation_id>
WebSocket    websocket:<connection_id>
MCP          mcp:<correlation_id>
```

An answer may be produced by a different Actor than the one that received the
signal, so it travels as a correlated envelope carrying its own
`receipt_id` / `correlation_id` / `run_id`, not a bare result.

## 12. MCP profile

Protocol `2026-07-28`, Streamable HTTP, **stateless**. No session map, no
resumable stream, `GET /mcp` → 405. JSON-RPC batching is rejected.

Routing metadata is exploited before the body is parsed: `Mcp-Method` and
`Mcp-Name` headers reach the router directly, so a tool call routes
deterministically with no model and no payload inspection.

Receipts are created for `tools/call` — the meaningful ingress. `initialize`,
`ping`, `tools/list` and notifications are protocol mechanics and get no
receipt, on the same reasoning that WebSocket ping/pong get none: Antenna
records received signals, not transport noise.

## 13. Deviations from the mounting spec

Two, both deliberate, both flagged rather than silently taken.

**MCP is implemented natively, not via `rmcp`.** The spec named the official
crate. Streamable-HTTP-stateless MCP is JSON-RPC over POST — about 200 lines
here — and implementing it directly keeps Receipt/Run/Delivery semantics ours,
per §0's *"if a crate attempts to own Antenna semantics: do not adopt it yet"*.
The Definition of Done requires *"modern stateless MCP works"*, not that `rmcp`
be a dependency. The MCP ingress is one file behind the shared router; swapping
in `rmcp` later changes nothing above it.

**Telemetry is W3C trace-context, not an OTLP exporter.** `tracing` spans plus
`trace_id`/`span_id`/`traceparent` stamped onto every receipt, run and delivery,
and `traceparent` propagated on outbound HTTP. Inbound `traceparent` is honoured
so an external journey keeps its trace. This connects the journey end to end
without adding `opentelemetry-otlp` + `tonic` to an 8 GB box with no collector
running. Adding an exporter later is additive.

`tokio-tungstenite` is a dev-dependency only. Antenna does not yet connect
outward over WebSocket, and §0 forbids abstraction without a present use.

## 14. Definition of done — as verified

```
[x] one Rust binary starts locally
[x] SQLite initializes automatically
[x] bucket initializes automatically
[x] HTTP ingress works                     tests/round_trip.rs
[x] file ingress works                     tests/blob.rs
[x] WebSocket ingress + reply works        tests/websocket.rs
[x] modern stateless MCP works             tests/mcp.rs
[x] every accepted ingress creates a Receipt
[x] every outward effect creates a Delivery
[x] router is shared by all ingress forms
[x] at least one deterministic Capability works
[x] optional ambiguous route may invoke intelligence
[x] no model is required to boot
[x] restart resumes unfinished durable work   tests/crash_recovery.rs
[x] duplicate delivery protections exist      exactly-once under SIGKILL
[x] tracing spans connect the journey         W3C trace-context, see §13
[x] authority checks occur before external effects
[x] raw received material remains inspectable GET /receipts/{id}/raw
```

## 15. Inspection surface

```
GET  /health                 liveness, counts, capability list
GET  /receipts?limit=        recent receipts
GET  /receipts/{id}          one receipt with its runs
GET  /receipts/{id}/raw      the bytes exactly as they arrived
GET  /deliveries?limit=      outbox with authority decisions
```

Logs are not the source of truth. SQLite is.

```bash
sqlite3 ~/lab/antenna/data/antenna.db \
  "SELECT id, transport, interaction, status FROM receipts ORDER BY recorded_at DESC LIMIT 10;"
```
