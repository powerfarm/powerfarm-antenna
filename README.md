# Antenna

A thin, durable ingress/egress membrane.

> receive faithfully · preserve durably · route cheaply
> delegate intelligently · authorize explicitly · return reliably

One Rust binary. SQLite for the record, a content-addressed bucket for bytes.
No model is required to boot. See [ANTENNA_SPEC.md](ANTENNA_SPEC.md) for the
invariants that survived implementation.

## Run

```bash
cargo run --release
# ANTENNA_CONFIG=antenna.toml  ANTENNA_ROUTES=routes/default.toml
```

## The four journeys

```bash
# A — event/call
curl -X POST localhost:8799/ingress -H 'content-type: application/json' -d '{"hello":"world"}'

# B — blob
curl -X POST localhost:8799/blob -H 'content-type: application/pdf' --data-binary @doc.pdf

# C — websocket
websocat ws://localhost:8799/ws   # then send {"id":1,"hello":"world"}

# D — MCP (stateless, 2026-07-28)
curl -X POST localhost:8799/mcp -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## Inspect

```bash
curl localhost:8799/health
curl localhost:8799/receipts
curl localhost:8799/receipts/<id>/raw     # the bytes exactly as they arrived
curl localhost:8799/deliveries            # outbox, with authority decisions
sqlite3 data/antenna.db "SELECT id,transport,interaction,status FROM receipts LIMIT 10;"
```

## Test

```bash
cargo test                    # 26 tests, incl. exactly-once delivery under SIGKILL
./ops/crash-test.sh           # spec §19 against a live install
```
