#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# THE one command for the Linux box: every arrow-rs benchmark stage, in the
# order the evidence chain needs them, into ONE timestamped run directory.
#
#   bash release/nightly_tests/dataset/arrow_rs_probe/run_all.sh
#
# Stages (subset via STAGES=mechanism,release — default all):
#   setup      setup.sh: venv + commit-matched wheel + source symlink + crate
#              (skipped when env.sh already imports; FORCE_SETUP=1 after any
#              git pull that touches the crate)
#   fixtures   gen_local_fixtures.py, ALL shapes (incl. the new expansion
#              sweep tensors_lo/tensors_dict/tensors_hi)
#   mechanism  batch_ablation.py — shapes x request-policies x budgets,
#              standalone. Decision variables = the four gates (G1 overshoot,
#              G2 R_rss, G3 R_wall, G4 rows parity). This is the gate any
#              batch-sizing code change must pass first.
#   release    loss_triage.py — the same loss shapes IN RAY, local + S3
#              (per-task USS at 20 Hz, decoder dists), both readers + arena2
#              arm. S3 legs run iff ARROW_RS_S3_BUCKET is set (fixtures are
#              synced up automatically). This is the release-metric gate.
#   soak       soak_probe.py — long-lived session, O(100) tasks/worker,
#              idle-USS floor per round, arms pa/rs/rs_arena2/rs_trim/
#              rs_jemalloc. This is the retention gate (M37/M38/M44).
#   tensors    tensors_nbytes_probe.py — cheap M39 representation check.
#
# Knobs (env):
#   STAGES=...             subset of setup,fixtures,mechanism,release,soak,tensors
#   FIXTURES_ROOT=<dir>    default ~/arrow_rs_repl_fixtures
#   FIXTURE_SCALE=1.0      0.25 for a smoke run
#   ARROW_RS_S3_BUCKET=s3://...   enables the release stage's S3 legs
#   BUDGETS=32             mechanism budget sweep, e.g. 16,32,128
#   ABLATION_SHAPES=...    mechanism shapes (default: batch_ablation.py's 10)
#   TRIAGE_SHAPES=auto,write,tensorscp,tensorsdict
#   SOAK_SHAPES=auto,write  ARMS=pa,rs,rs_arena2,rs_trim,rs_jemalloc
#   REPEAT=3 WARMUP=1 WORKERS=4
#   FORCE_SETUP=1          re-run setup.sh even if the env imports
#
# Results: everything under arrow_rs_probe/suite_runs/<timestamp>/ —
#   ablation.json (+ gate verdict on stdout), loss_triage/summary.json,
#   soak/summary.json, tensors_nbytes.log, and stage logs. Each stage also
#   prints its own R-table; the final index lists every artifact.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
STAGES="${STAGES:-setup,fixtures,mechanism,release,soak,tensors}"
FIXTURES_ROOT="${FIXTURES_ROOT:-$HOME/arrow_rs_repl_fixtures}"
FIXTURE_SCALE="${FIXTURE_SCALE:-1.0}"
BUDGETS="${BUDGETS:-32}"
TRIAGE_SHAPES="${TRIAGE_SHAPES:-auto,write,tensorscp,tensorsdict}"
SOAK_SHAPES="${SOAK_SHAPES:-auto,write}"
ARMS="${ARMS:-pa,rs,rs_arena2,rs_trim,rs_jemalloc}"
REPEAT="${REPEAT:-3}"
WARMUP="${WARMUP:-1}"
WORKERS="${WORKERS:-4}"
RUN_DIR="$SCRIPT_DIR/suite_runs/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RUN_DIR"

say() { printf '\n\033[1;35m### %s\033[0m\n' "$*"; }
has_stage() { case ",$STAGES," in *",$1,"*) return 0;; *) return 1;; esac; }

say "run dir: $RUN_DIR (stages: $STAGES)"

if has_stage setup; then
  say "stage: setup"
  if [ "${FORCE_SETUP:-0}" != "1" ] && [ -f "$SCRIPT_DIR/env.sh" ] && \
     ( source "$SCRIPT_DIR/env.sh" >/dev/null 2>&1 && \
       python -c "import ray, ray_data_arrow_rs" >/dev/null 2>&1 ); then
    say "environment already set up (env.sh + imports OK) - skipping setup.sh"
  else
    bash "$SCRIPT_DIR/setup.sh"
  fi
fi
source "$SCRIPT_DIR/env.sh"
export RAY_ADDRESS=local

if has_stage fixtures; then
  say "stage: fixtures (root=$FIXTURES_ROOT scale=$FIXTURE_SCALE, all shapes)"
  python "$SCRIPT_DIR/gen_local_fixtures.py" --root "$FIXTURES_ROOT" \
    --scale "$FIXTURE_SCALE" 2>&1 | tee "$RUN_DIR/fixtures.log"
fi

if has_stage mechanism; then
  say "stage: mechanism (batch_ablation: budgets=$BUDGETS)"
  python "$SCRIPT_DIR/batch_ablation.py" \
    --fixtures-root "$FIXTURES_ROOT" --scale "$FIXTURE_SCALE" \
    --budgets-mib "$BUDGETS" --out "$RUN_DIR" \
    ${ABLATION_SHAPES:+--shapes "$ABLATION_SHAPES"} \
    2>&1 | tee "$RUN_DIR/mechanism.log"
fi

if has_stage release; then
  say "stage: release-metric (loss_triage: shapes=$TRIAGE_SHAPES s3=${ARROW_RS_S3_BUCKET:-off})"
  python "$SCRIPT_DIR/loss_triage.py" \
    --fixture-root "$FIXTURES_ROOT" --outdir "$RUN_DIR/loss_triage" \
    --shapes "$TRIAGE_SHAPES" --repeat "$REPEAT" --warmup "$WARMUP" \
    2>&1 | tee "$RUN_DIR/release.log"
fi

if has_stage soak; then
  say "stage: soak/retention (shapes=$SOAK_SHAPES arms=$ARMS workers=$WORKERS)"
  python "$SCRIPT_DIR/soak_probe.py" \
    --fixture-root "$FIXTURES_ROOT" --outdir "$RUN_DIR/soak" \
    --shapes "$SOAK_SHAPES" --arms "$ARMS" --workers "$WORKERS" \
    ${ROUNDS:+--rounds "$ROUNDS"} ${PATH_REPEAT:+--path-repeat "$PATH_REPEAT"} \
    2>&1 | tee "$RUN_DIR/soak.log"
fi

if has_stage tensors; then
  say "stage: tensors nbytes probe (M39 representation check)"
  python "$SCRIPT_DIR/tensors_nbytes_probe.py" --fixture-root "$FIXTURES_ROOT" \
    2>&1 | tee "$RUN_DIR/tensors_nbytes.log"
fi

say "DONE — artifact index"
for f in "$RUN_DIR/ablation.json" "$RUN_DIR/loss_triage/summary.json" \
         "$RUN_DIR/soak/summary.json" "$RUN_DIR"/*.log; do
  [ -e "$f" ] && echo "  $f"
done
echo
echo "verdict lines (grep of the stage tables):"
grep -h "gate verdict" "$RUN_DIR/mechanism.log" 2>/dev/null || true
echo "  release + soak R-tables are at the end of release.log / soak.log"
