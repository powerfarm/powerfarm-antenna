# Production verification — 2026-09-15

The existing Antenna LaunchDaemon on lab-8gb runs the contract adapter at
https://antenna.minilab.work/. These records contain no credentials.

## Executed and observed

- `public-verification.json`: HTTP, webhook, SSE, WebSocket and July MCP each
  executed the accepted graph and returned a receipt. MCP inspected its result.
- `restart-verification.json`: restarted the production daemon, inspected an
  earlier receipt, and confirmed the original five receipts retained the same
  run counts. Four client contracts were current after restart.
- `mac-cli-invocation.json` and `mac-cli-result.json`: the installed CLI on the
  operator's Mac invoked and inspected the public service using its credential.
- `production-activation.json`: executable SHA256, implementation commit,
  activation time, runtime path and rollback backup.

The first request during daemon replacement returned 502 before the listener
was ready. The subsequent complete transport verification passed. The original
Google integration remains enabled; existing configuration was preserved.

## Source and validation

- Antenna implementation: `7de9964d8a376606a5765027c3a8d216a2586b73`.
- Published template source: `9566f5179676ff60f71d9f8e13e37ba2f5ddaa2a`.
- Registry migration source: `86eedb539f3073d1779d4e1837c39072167925d7`.
- CLI source: `f043a23`.
- Rust: 59 tests passed (`rust-tests.log`).
- Existing CLI: 45 tests passed (`cli-tests.log`); new command exercised live.
- Registry: 67 tests, migration parity and brand guard passed
  (`registry-tests.log`). The parity ledger is an August source baseline;
  its "pending" line does not represent live migration status.
- Contract SQL: isolated PostgreSQL authorization, signature, immutability,
  acceptance and revocation checks passed (`registry-sql-tests.log`).

## Practical scope

The four installed graphs store and inspect incoming JSON. They do not build or
deploy repositories, invoke a model, or provide a graphical editor. Graph JSON
is published and instantiated through the CLI, executed by the supplied
Continuity compiler, and supervised through MCP. The original GitHub observer
continues alongside these services.

Production code is published on `feat/antenna-service-contracts` in the Antenna,
Registry and CLI repositories. This record does not claim a merge to main.
