#!/usr/bin/env bash
# REP-64 provenance (expanded): run every arm of the RocksDB delay-surface
# matrix in its own process and print a verdict table.
#
# Each arm is process-isolated because the GCS reads its RAY_TESTING_* knobs via
# std::getenv at startup, and several are cached in function-local statics.
#
# Usage:
#   ./run_rep64_rocksdb.sh                 # all arms, both too_many_returns values
#   ./run_rep64_rocksdb.sh D               # only arms whose id starts with D
#   ./run_rep64_rocksdb.sh E3              # a single arm
set -uo pipefail

cd "$(dirname "$0")/../../.." || exit 1

TEST_FILE="python/ray/tests/test_rep64_rocksdb_delays.py"
FILTER="${1:-}"
LOG_DIR="${REP64_LOG_DIR:-/tmp/rep64_rocksdb}"
mkdir -p "$LOG_DIR"

mapfile -t ARMS < <(
  python -c "
import re,sys
src = open('$TEST_FILE').read()
block = src.split('_ARMS = {',1)[1].split('_ARM_LOAD',1)[0]
for name in re.findall(r'^    \"([A-Z]\d[A-Za-z0-9_]*)\":', block, re.M):
    print(name)
"
)

if [[ -n "$FILTER" ]]; then
  mapfile -t ARMS < <(printf '%s\n' "${ARMS[@]}" | grep "^${FILTER}")
fi

if [[ ${#ARMS[@]} -eq 0 ]]; then
  echo "No arms matched filter '${FILTER}'" >&2
  exit 1
fi

echo "Running ${#ARMS[@]} arm(s) x 2 too_many_returns values; logs -> $LOG_DIR"
echo

RESULTS=()
for arm in "${ARMS[@]}"; do
  for tmr in False True; do
    id="${arm}-${tmr}"
    log="$LOG_DIR/${id}.log"
    printf '  %-38s ' "$id"
    # -p no:cacheprovider keeps parallel-safe; each invocation is its own process.
    timeout 600 python -m pytest -s -q \
      "${TEST_FILE}::test_rocksdb_delay_surface[${arm}-${tmr}]" \
      >"$log" 2>&1
    rc=$?
    line=$(grep -o 'REP64_RESULT .*' "$log" | tail -1)
    if [[ -z "$line" ]]; then
      verdict="NO-RESULT(rc=$rc)"
    else
      verdict=$(sed -n 's/.*verdict=\([A-Z-]*\).*/\1/p' <<<"$line")
      elapsed=$(sed -n 's/.*elapsed=\([^ ]*\).*/\1/p' <<<"$line")
      n=$(sed -n 's/.*n=\([^ ]*\).*/\1/p' <<<"$line")
      verdict="${verdict} elapsed=${elapsed} n=${n}"
    fi
    echo "$verdict"
    RESULTS+=("${id}|${verdict}")
  done
done

echo
echo "==================== REP-64 RocksDB delay-surface verdicts ===================="
printf '%-40s %s\n' "ARM" "VERDICT"
printf '%-40s %s\n' "----------------------------------------" "-------"
for r in "${RESULTS[@]}"; do
  printf '%-40s %s\n' "${r%%|*}" "${r#*|}"
done
echo "=============================================================================="
echo "Logs: $LOG_DIR"
