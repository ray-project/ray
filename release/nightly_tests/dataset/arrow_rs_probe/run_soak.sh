#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ONE command for the Linux box: the soak/churn discriminator for the A/B #4
# retention losses (findings M37 read_large_parquet_autoscaling, M38
# write_parquet) plus the M39 tensors representation check.
#
#   bash release/nightly_tests/dataset/arrow_rs_probe/run_soak.sh
#
# Why this exists: every prior box run (replication_matrix, loss_triage) used
# a FRESH Ray session per cell and ~tens of tasks per worker - and none of the
# release losses reproduced (M35). A/B #4 then proved the losses are real at a
# 20 Hz poll with byte-identical decoder work, so the untested variable is
# WORKER LIFETIME x TASK COUNT. soak_probe.py holds one long-lived Ray session
# per arm (pa / rs / rs+MALLOC_ARENA_MAX=2), pushes rounds of the loss shapes
# through a pinned worker pool until each worker has executed O(100+) tasks,
# and reads the idle-USS floor after every round. See soak_probe.py's
# docstring for the verdict table.
#
# Same setup skeleton as run_loss_triage.sh:
#   1. setup.sh          venv + commit-matched wheel + source symlink + crate
#                        (skipped when env.sh already imports; FORCE_SETUP=1 to
#                        redo - MANDATORY after a git pull that touches the crate)
#   2. fixtures          gen_local_fixtures.py: auto_rg, bin_sweep, tensors_cp
#   3. soak matrix       soak_probe.py (local files only - the retention
#                        question is transport-independent per M35/M38)
#   4. tensors nbytes    tensors_nbytes_probe.py (M39, cheap, standalone)
#
# Knobs (env):
#   FIXTURE_SCALE=0.25   smaller fixtures for a smoke run
#   FIXTURES_ROOT=<dir>  default ~/arrow_rs_repl_fixtures
#   SHAPES=auto          subset: auto,write
#   ARMS=pa,rs           subset: pa,rs,rs_arena2
#   WORKERS=4            pinned worker-pool size (num_cpus)
#   ROUNDS= / PATH_REPEAT=   override soak_probe per-shape defaults
#   SKIP_TENSORS=1       skip step 4
#   FORCE_SETUP=1        re-run setup.sh even if the env already imports
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
FIXTURES_ROOT="${FIXTURES_ROOT:-$HOME/arrow_rs_repl_fixtures}"
FIXTURE_SCALE="${FIXTURE_SCALE:-1.0}"
SHAPES="${SHAPES:-auto,write}"
ARMS="${ARMS:-pa,rs,rs_arena2}"
WORKERS="${WORKERS:-4}"

say() { printf '\n\033[1;35m### %s\033[0m\n' "$*"; }

say "1/4 environment (setup.sh)"
if [ "${FORCE_SETUP:-0}" != "1" ] && [ -f "$SCRIPT_DIR/env.sh" ] && \
   ( source "$SCRIPT_DIR/env.sh" >/dev/null 2>&1 && \
     python -c "import ray, ray_data_arrow_rs" >/dev/null 2>&1 ); then
  say "environment already set up (env.sh + imports OK) - skipping setup.sh (FORCE_SETUP=1 to redo)"
else
  bash "$SCRIPT_DIR/setup.sh"
fi
source "$SCRIPT_DIR/env.sh"
export RAY_ADDRESS=local

say "2/4 fixtures (root=$FIXTURES_ROOT scale=$FIXTURE_SCALE: auto_rg, bin_sweep, tensors_cp)"
python "$SCRIPT_DIR/gen_local_fixtures.py" --root "$FIXTURES_ROOT" \
  --scale "$FIXTURE_SCALE" --shapes auto_rg,bin_sweep,tensors_cp

say "3/4 soak matrix (shapes=$SHAPES arms=$ARMS workers=$WORKERS)"
python "$SCRIPT_DIR/soak_probe.py" \
  --fixture-root "$FIXTURES_ROOT" \
  --shapes "$SHAPES" \
  --arms "$ARMS" \
  --workers "$WORKERS" \
  ${ROUNDS:+--rounds "$ROUNDS"} \
  ${PATH_REPEAT:+--path-repeat "$PATH_REPEAT"}

if [ "${SKIP_TENSORS:-0}" != "1" ]; then
  say "4/4 tensors nbytes probe (M39: representation vs batch sizing)"
  python "$SCRIPT_DIR/tensors_nbytes_probe.py" --fixture-root "$FIXTURES_ROOT"
fi

say "DONE - soak summary printed above; series + logs under $SCRIPT_DIR/soak_runs/"
