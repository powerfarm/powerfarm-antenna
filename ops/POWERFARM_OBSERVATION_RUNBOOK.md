# PowerFarm GitHub Observation Operations

This is an operational runbook for LAB-8GB Antenna. It is not a PowerFarm
architecture, Target definition, implementation plan, or planning authority.
The Google Sheet remains the living blueprint.

## Service and boundaries

- launchd label: `work.minilab.antenna`
- loopback listener: `127.0.0.1:8799`
- public operator hostname: `antenna.minilab.work`
- public GitHub edge: `ingress.minilab.work/github`
- Worker: `powerfarm-github-ingress`
- GitHub App / installation: `4822751` / `158881753`
- exact tunnel handoff allowed to origin: `/internal/github`
- protected configuration: `/Users/danvoulez/lab/antenna/antenna.toml` (mode `0600`)
- SQLite: `/Users/danvoulez/lab/antenna/data/antenna.db`

The Cloudflare tunnel returns 403 for every path on
`antenna.minilab.work` except `/internal/github`. Antenna independently rejects
that path unless the timestamped HMAC binds delivery ID, event name, and raw
body SHA-256. Inspection additionally requires the application inspection
token and is intended for loopback/operator use.

## Health and status

Public liveness is deliberately unavailable after boundary hardening. Check
loopback liveness:

```sh
curl --fail http://127.0.0.1:8799/health
```

`/status` requires the inspection token and reports counts/timestamps only; it
never returns payload bodies. Do not place the token in shell history or an
argument vector. `ops/verify_local_github.rb` reads it from protected config.

## Build, test, and restart

```sh
cargo test --no-fail-fast
cargo build --release
codesign --force --sign - target/release/antenna
cp target/release/antenna bin/antenna
sudo launchctl kickstart -k system/work.minilab.antenna
```

If launchd reports `OS_REASON_CODESIGNING` after an executable replacement,
reload the unchanged launch definition:

```sh
sudo launchctl bootout system /Library/LaunchDaemons/work.minilab.antenna.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/work.minilab.antenna.plist
```

Always verify the new PID, loopback `/health`, authenticated `/status`, and
the externally observable 403/401 boundaries after restart.

## Local signed canary

```sh
./ops/verify_local_github.rb
```

The canary submits a valid delivery twice, a tampered handoff, a stale
handoff, and an unknown event. It prints no secret. Expected results are 202,
202 with `duplicate: true`, 401, 401, and 202 with classification `UNKNOWN`.

## Scheduled census

The production path is `census::run_once`, authenticated by the configured
GitHub App installation. It paginates `GET /installation/repositories`, proves
the returned count, and commits the projection only after a complete run.

`census-import` is a local bootstrap/recovery tool for importing a demonstrably
complete authenticated organization API response. It is not the scheduled
credential path and records that limitation in `census_runs.error` even when
the imported result is complete.

## Sheet projection

The writer is disabled until its dedicated service-account credential exists
at `data/secrets/google-blueprint-writer.json` and that identity has Editor
access to the canonical spreadsheet.
It resolves the configured tab from spreadsheet metadata first, locates rows
by `Record ID`, restricts writes to configured observed/projection columns,
uses narrow `updateCells.start` requests and field masks, and persists an
in-flight mutation before calling Sheets. An uncertain result is not retried
blindly.

Missing `GH-REPO-<numeric-id>` records are created with `appendCells`; no
physical row number is remembered. The same atomic request writes the stable
GitHub repository ID and only configured projection fields, leaving Target
and constitutional columns unset. Later updates resolve the appended row by
that stable ID.

## Rollback

Rollback material is outside the source tree at:

`/Users/danvoulez/lab/antenna-rollback-20260903T210000Z`

The old Antenna binary can run against the additive database schema, so prefer
rolling back the binary/config while retaining the current database evidence.
Before any database restore, make a fresh consistent SQLite backup. The
pre-migration database exists only for last-resort recovery.

To restore the previous Cloudflare routing state, install the preserved
`config.yml` back to `/etc/cloudflared/config.yml`, validate it with
`cloudflared tunnel ingress validate`, then restart
`system/work.minilab.cloudflared`. Verify the external effect; command success
alone is not a rollback receipt.

Never print or commit the inspection token, GitHub webhook secret, handoff
secret, GitHub App private key, Cloudflare token, or Google credential.
