# Antenna services

The existing Antenna daemon executes JSON graphs under two accepted Registry
contracts: **Antenna → service**, then **service → client application**.
The installed first client is the existing `pf.powerfarm-cli` identity.

## Installed services

| Service | Client contract | Entry |
| --- | --- | --- |
| HTTP | `powerfarm-cli.http` | POST `/` |
| Webhook | `powerfarm-cli.webhook` | POST `/` |
| WebSocket | `powerfarm-cli.websocket` | WebSocket `/` |
| Streaming | `powerfarm-cli.sse` | POST `/`, `Accept: text/event-stream` |

Public endpoint: `https://antenna.minilab.work/`.
Send `Antenna-Contract: <client contract>` and `Authorization: Bearer <client credential>`.
The sample contracts allow 65,536 bytes and no external destinations. Their
graphs preserve the received JSON in content-addressed storage, inspect the
resulting object, and return both node outputs and a receipt.

These are working capture services. CI execution, deployment promotion, local
model invocation, and the Temporal worker from Continuity v2 are not implemented
by these four templates. The original GitHub observer and Sheets integration
continue as existing services.

## CLI

On this Mac and lab-8gb, the existing Powerfarm CLI has the additional `service`
command. Invocation and result inspection are configured on both machines.
Registry management uses the existing signed-in operator session on lab-8gb.

Try the installed contract from this Mac:

```sh
~/.local/bin/pf service invoke powerfarm-cli.http \
  --file /Users/ubl-ops/platform-four/antenna/evidence/services-20260915/input.json
```

On lab-8gb:

```sh
~/.local/bin/pf service list --json
~/.local/bin/pf service inspect powerfarm-cli.http --json
~/.local/bin/pf service invoke powerfarm-cli.http --file /absolute/path/event.json
~/.local/bin/pf service result powerfarm-cli.http --receipt rcp_RECEIPT_ID
~/.local/bin/pf service --help
```

Management uses the operator's existing Registry OAuth session and explicit
`registry.admin` grant. Invocation uses the service credential at
`~/.config/antenna-contracts/antenna-client.token` (override with
`ANTENNA_CLIENT_TOKEN_FILE`). Credentials are never printed by these commands.

To add a service: publish a JSON template with an exact Git source commit, create
its service contract, accept its hash for both parties, create a client contract,
and accept that exact hash for both parties. The CLI help contains each command.
Client limits cannot exceed the parent contract. Changes require new versions and
new acceptance. `service revoke NAME --sha256 HASH` ends derived authority.
The initial contracts expire on 2026-12-14; inspect their exact `valid_until`.

## External LLMs over MCP

Use the same public URL. The July 2026 protocol uses `server/discover` instead of
an initialization session. Every request includes version metadata and matching
`Mcp-Protocol-Version`, `Mcp-Method`, and (for tool calls) `Mcp-Name` headers.
The service client credential authorizes only its accepted client contracts.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "invoke_service",
    "arguments": {
      "contract": "powerfarm-cli.http",
      "input": {"message": "work to preserve"}
    },
    "_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"}
  }
}
```

To inspect prior work, replace `input` with `receipt_id`. A contract cannot inspect
another contract's receipts. The management tools from the original Antenna MCP
surface require the separate operator inspection credential.

## Continuity and recovery

The supplied Continuity compiler builds LangGraph 1.2.11 graphs with SQLite
checkpoints. `runtime/PROVENANCE.md` records the adapted source. Each valve asks
the existing Rust capability to act; the graph then records its result before
moving to the next node. Supported valves: `object.store`, `document.inspect`,
`echo`, `delivery.create`. Graphs contain at most 32 nodes, one edge or conditional
branch per node, explicit END, no cycles and no unreachable nodes. Branches can
select dotted state fields. Up to four graphs execute concurrently.

The graph checkpoint is `data/continuity.db`. Receipts, runs and the transactional
outbox remain in `data/antenna.db`. Back up **both databases plus the objects**.
Recovery reuses the receipt's graph checkpoint and rechecks current authority.
External delivery proposals are committed with graph completion and rechecked
before each attempt. Receivers must deduplicate the supplied Idempotency-Key;
network delivery is at least once. A fresh HTTP POST is a new arrival, so after
an ambiguous request, inspect its receipt before intentionally starting new work.

Registry snapshots are signed with the daemon credential, audience-bound and
valid for 60 seconds. Antenna refreshes every 15 seconds. Expiration or revocation
stops new contract work and external effects; WebSocket frames are reauthorized.
Registry downtime does not stop the original GitHub/Sheets services. Admitted
contract work can wait for authority to return.

## Verification

```sh
uv sync --project runtime --frozen --python 3.13
runtime/.venv/bin/python runtime/test_run_graph.py
ANTENNA_TEST_PYTHON="$PWD/runtime/.venv/bin/python" cargo test --locked
runtime/.venv/bin/python ops/verify-services.py \
  --url https://antenna.minilab.work \
  --token-file ~/.config/antenna-contracts/antenna-client.token \
  --out /tmp/antenna-services-verification.json
```

Production activation and verification receipts are retained under
`evidence/services-20260915/`. The initial root-only release backup is
`/Users/danvoulez/lab/antenna-backup-20260915T080506Z` on lab-8gb.
Rollback uses the recorded prior binary/config and retains all new data.
