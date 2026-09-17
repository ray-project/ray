#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CAMPAIGN=""
CLIENT_PYTHON=""
OUT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --campaign) CAMPAIGN="${2:-}"; shift 2 ;;
    --client-python) CLIENT_PYTHON="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    *) echo "Usage: $0 --campaign DIRECTORY --client-python PYTHON --out PNG" >&2; exit 2 ;;
  esac
done
[[ -n "$CAMPAIGN" && -n "$CLIENT_PYTHON" && -n "$OUT" ]] || {
  echo "Usage: $0 --campaign DIRECTORY --client-python PYTHON --out PNG" >&2; exit 2
}
[[ -x "$CLIENT_PYTHON" ]] || { echo "missing client Python: $CLIENT_PYTHON" >&2; exit 2; }
[[ "$CAMPAIGN" = /* ]] || CAMPAIGN="$PWD/$CAMPAIGN"
[[ "$OUT" = /* ]] || OUT="$PWD/$OUT"
[[ -d "$CAMPAIGN/cells" && ! -e "$OUT" ]] || { echo "missing campaign or existing output" >&2; exit 2; }

"$CLIENT_PYTHON" "$SCRIPT_DIR/analyze.py" --campaign "$CAMPAIGN/cells" --out-dir "$CAMPAIGN/analysis"
"$CLIENT_PYTHON" - "$CAMPAIGN/analysis/cells.csv" <<'PY'
import csv
import math
import pathlib
import sys

rows = list(csv.DictReader(pathlib.Path(sys.argv[1]).open()))
expected = {"session-affinity", "pure-kv-cache", "kv-token-aware"}
if {row["variant"] for row in rows} != expected:
    raise SystemExit("incomplete three-variant result")
if len({row["dag_sha256"] for row in rows}) != 1:
    raise SystemExit("router variants used different workloads")
for row in rows:
    for key in ("aiperf_rc", "seed_rc", "routing_validation_rc", "token_validation_rc"):
        if float(row[key]) != 0:
            raise SystemExit(f"{row['variant']}: {key} failed")
    for key in ("response_cache_telemetry_coverage", "rollout_e2e_p99_ms"):
        if not math.isfinite(float(row[key])):
            raise SystemExit(f"{row['variant']}: invalid {key}")
    if row["variant"] != "session-affinity":
        if float(row["kv_tracker_present_rate"]) != 1 or float(row["kv_tokenized_route_rate"]) != 1:
            raise SystemExit(f"{row['variant']}: KVAwareRouter fallback")
print("validated three-variant campaign")
PY
"$CLIENT_PYTHON" "$SCRIPT_DIR/plot_results.py" --campaign "$CAMPAIGN" --out "$OUT"
echo "Wrote $OUT"
