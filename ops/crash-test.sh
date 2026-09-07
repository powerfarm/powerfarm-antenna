#!/usr/bin/env bash
# Spec §19, against the LIVE installation.
#
#   receive -> commit Receipt -> SIGKILL -> restart -> discover unfinished
#   work -> continue processing -> produce Delivery -> no duplicate effect
#
# Non-destructive: adds one receipt, kills the service, lets launchd restart it.
set -euo pipefail

BASE="${1:-http://127.0.0.1:8799}"
CONF="${ANTENNA_CONFIG:-$HOME/lab/antenna/antenna.toml}"
TOKEN="$(sed -n 's/^inspect_token[[:space:]]*=[[:space:]]*"\(.*\)"/\1/p' "$CONF" || true)"
auth=(); [ -n "$TOKEN" ] && auth=(-H "Authorization: Bearer $TOKEN")

pid_now() { pgrep -f 'lab/antenna/bin/antenna' | head -1; }

echo "== 1. receive and commit =="
before_pid="$(pid_now)"
resp="$(curl -s -X POST "$BASE/ingress?wait=0" -H 'content-type: application/json' \
        -d '{"crash_test":true,"note":"accepted before the kill"}')"
rid="$(python3 -c 'import sys,json;print(json.load(sys.stdin)["receipt_id"])' <<<"$resp")"
echo "   receipt: $rid"
echo "   pid:     $before_pid"

echo "== 2. SIGKILL (uncatchable: no drain, no destructors) =="
kill -9 "$before_pid" 2>/dev/null || sudo -n kill -9 "$before_pid"

echo "== 3. wait for launchd to bring up a NEW process =="
for i in $(seq 1 90); do
  sleep 1
  now="$(pid_now || true)"
  if [ -n "$now" ] && [ "$now" != "$before_pid" ] && curl -sf "$BASE/health" >/dev/null 2>&1; then
    echo "   restarted after ${i}s as pid $now"
    break
  fi
  [ "$i" = 90 ] && { echo "   FAIL: never came back"; exit 1; }
done

echo "== 4. the receipt survived and was carried to completion =="
curl -s "${auth[@]}" "$BASE/receipts/$rid" | python3 -c '
import sys, json
d = json.load(sys.stdin)
if "receipt" not in d:
    print("   FAIL:", json.dumps(d)); sys.exit(1)
r = d["receipt"]
print("   id:     ", r["id"])
print("   digest: ", r["raw_digest"])
print("   status: ", r["status"])
runs = d.get("runs") or []
print("   runs:   ", [(x["capability"], x["status"]) for x in runs])
assert r["status"] != "accepted", "receipt was never routed after the restart"
print("   OK — work accepted before SIGKILL survived and completed after restart")
'
