#!/usr/bin/env bash
# run-all.sh — orchestrator for the rep-64 POC k8s harness.
#
# Runs every test script in sequence and aggregates results into summary.md.
# Designed so test failures DO NOT abort the run — the whole point of the
# harness is to surface findings, so collecting partial signal is more useful
# than fail-fast.  Infrastructure failures (cluster setup, deploy) DO abort,
# since downstream tests can't run without a ready cluster.
#
# Usage:
#   run-all.sh [--tier=k3d|remote] [--skip-setup] [--teardown]
#
# Tier selects the env file: env/k3d.env or env/remote.env.  Default k3d.
# --skip-setup skips 00-setup-* (use when the cluster already exists).
# --teardown runs 99-teardown.sh at the end (k3d-only; clears local state).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

TIER=k3d
SKIP_SETUP=0
TEARDOWN=0
for arg in "$@"; do
  case "$arg" in
    --tier=*) TIER="${arg#--tier=}" ;;
    --skip-setup) SKIP_SETUP=1 ;;
    --teardown) TEARDOWN=1 ;;
    -h|--help)
      sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

ENV_FILE="$HERE/../env/${TIER}.env"
[[ -f "$ENV_FILE" ]] || { echo "env file not found: $ENV_FILE" >&2; exit 1; }
export HARNESS_ENV_FILE="$ENV_FILE"

# shellcheck disable=SC1091
source "$HERE/../lib.sh"
# Pull RESULTS_DIR (and the rest of the env) into this shell so the aggregate
# call below can resolve it.  Individual test scripts call load_env themselves;
# the orchestrator needs it too because we reference $RESULTS_DIR directly.
load_env

log "=== run-all: tier=$TIER env=$ENV_FILE ==="

# Track per-step outcomes so the summary at the end is honest about what ran.
declare -a STEP_NAMES=()
declare -a STEP_STATUS=()  # ok | fail | skipped

run_step() {
  local name="$1" required="$2"
  shift 2
  log "--- step: $name ---"
  STEP_NAMES+=("$name")
  # Capture the exit code directly — `$?` after `fi` is the if-statement's
  # status, which is 0 when the test fell through, not the failed command's
  # actual return code.
  local rc=0
  "$@" || rc=$?
  if (( rc == 0 )); then
    STEP_STATUS+=("ok")
    return 0
  fi
  STEP_STATUS+=("fail")
  log "--- step $name FAILED (rc=$rc) ---"
  if [[ "$required" == "required" ]]; then
    log "ABORT: $name is a required step; downstream tests cannot run"
    return 1
  fi
  log "continuing — test failures are findings, not run-blockers"
  return 0
}

skip_step() {
  local name="$1" reason="$2"
  log "--- step $name skipped: $reason ---"
  STEP_NAMES+=("$name")
  STEP_STATUS+=("skipped")
}

# 00 — setup (tier-specific; only k3d has a setup script today).
if (( ! SKIP_SETUP )); then
  case "$TIER" in
    k3d)
      if [[ -x "$HERE/00-setup-k3d.sh" ]]; then
        run_step "00-setup-k3d" required "$HERE/00-setup-k3d.sh" \
          || { log "fatal: cluster setup failed"; exit 1; }
      else
        skip_step "00-setup-k3d" "script missing"
      fi
      ;;
    remote)
      if [[ -x "$HERE/00-check-remote.sh" ]]; then
        run_step "00-check-remote" required "$HERE/00-check-remote.sh" \
          || { log "fatal: remote preflight failed"; exit 1; }
      else
        skip_step "00-check-remote" "script missing"
      fi
      ;;
    *)
      log "WARN: unknown tier '$TIER' — assuming cluster already exists"
      ;;
  esac
else
  skip_step "00-setup" "--skip-setup"
fi

# 10 — deploy RayCluster.  If this fails everything else fails too, so
# treat as required.
run_step "10-deploy-cluster" required "$HERE/10-deploy-cluster.sh" \
  || { log "fatal: cluster deploy failed"; exit 1; }

# Tests — failures recorded but not fatal.
run_step "20-actor-survival" optional "$HERE/20-actor-survival.sh" || true
run_step "30-pod-delete"     optional "$HERE/30-pod-delete.sh"     || true
run_step "40-substrate-sweep" optional "$HERE/40-substrate-sweep.sh" || true
run_step "50-fast-storage"   optional "$HERE/50-fast-storage.sh"    || true

# Aggregate.
log "--- aggregating results ---"
if python3 "$HERE/../python/aggregate.py" "$RESULTS_DIR"; then
  STEP_NAMES+=("aggregate")
  STEP_STATUS+=("ok")
else
  STEP_NAMES+=("aggregate")
  STEP_STATUS+=("fail")
  log "WARN: aggregate.py failed — check $RESULTS_DIR for raw JSONs"
fi

# Optional teardown (k3d-only — refusing on remote tier where cluster
# bring-down is the operator's call, not the harness's).
if (( TEARDOWN )); then
  if [[ "$TIER" == "k3d" ]]; then
    run_step "99-teardown" optional "$HERE/99-teardown.sh" || true
  else
    skip_step "99-teardown" "remote tier: refusing to teardown remote cluster"
  fi
fi

# Per-step summary.
log ""
log "=== run-all summary (tier=$TIER) ==="
exit_code=0
for i in "${!STEP_NAMES[@]}"; do
  log "  ${STEP_STATUS[$i]:>7}  ${STEP_NAMES[$i]}"
  [[ "${STEP_STATUS[$i]}" == "fail" ]] && exit_code=1
done
log ""
log "summary.md: $RESULTS_DIR/summary.md"
log "result JSONs: $RESULTS_DIR/*.json"
exit "$exit_code"
