#!/usr/bin/env bash
# REP-64 node-death lost-wakeup — process-isolated harness runner.
#
# Each arm runs in its own pytest subprocess: a hung arm parks a daemon thread in
# native CoreWorker::Wait(), and reusing the process SIGSEGVs the next ray.init.
# Hung arms are bounded by a wall-clock timeout and reported as PERMANENT-HANG.
#
#   source <your ray dev venv>
#   bash python/ray/tests/run_rep64.sh
set -u
TEST=python/ray/tests/test_rep64_node_death_hang.py
OUT=/tmp/rep64_summary.txt
ARMS="control actor_publish_delayed node_publish_delayed \
      node_persist_delayed node_persist_delayed_f3 node_publish_delayed_f5"
: > "$OUT"
for arm in $ARMS; do
  log=/tmp/rep64_${arm}.log
  echo "=== ARM: ${arm} ===" | tee -a "$OUT"
  timeout 200 python -m pytest -s -q "${TEST}::test_node_death_hang[${arm}]" >"$log" 2>&1
  rc=$?
  verdict=$(grep -aE '^\['"${arm}"'\] verdict' "$log" | tail -1)
  if [ -z "$verdict" ]; then
    if [ "$rc" -eq 124 ]; then
      verdict="[${arm}] verdict=PERMANENT-HANG (killed by 200s wall-clock timeout)"
    else
      verdict="[${arm}] verdict=UNKNOWN (rc=${rc}; see ${log})"
    fi
  fi
  echo "$verdict" | tee -a "$OUT"
  ray stop --force >/dev/null 2>&1 || true
done
echo "=== SUMMARY ===" | tee -a "$OUT"
grep -aE '^\[.*\] verdict' "$OUT"
