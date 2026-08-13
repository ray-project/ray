#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ONE command for a fresh Linux box: environment -> correctness gate ->
# fixtures -> the 2026-08-12 release-A/B replication matrix (TODO 1ab phase 1).
#
#   bash release/nightly_tests/dataset/arrow_rs_probe/run_replication.sh
#
# Same skeleton as run_grand_experiment.sh (which is the setup that already
# works on these boxes), pointed at replication_matrix.py:
#   1. setup.sh            venv + commit-matched Ray wheel + local-source symlink
#                          + Rust toolchain + native crate build + end-to-end check
#                          (skipped automatically when env.sh already imports;
#                          FORCE_SETUP=1 to redo — MANDATORY after a git pull that
#                          touches the crate, or you benchmark a stale .so)
#   2. correctness gate    the two pytest suites; red tests ABORT (SKIP_TESTS=1
#                          to override while debugging)
#   3. fixtures            gen_local_fixtures.py, replication shapes only
#                          (bin_sweep ~4 GiB, tensors_wide ~1.6 GiB, fat_col)
#   4. matrix              replication_matrix.py: tensors / tensorscp / binsweep /
#                          binbound / write / fatcol / oom (see its docstring for the
#                          rationale; binbound is the "is per-task USS bounded by
#                          the bin budget?" check and needs Linux — USS is None on
#                          macOS; oom deliberately gets PyArrow's arm OOM-killed
#                          by Ray's memory monitor, so FAILED pyarrow cells there
#                          are the result, not a broken run)
#
# Knobs (env):
#   SKIP_TESTS=1                skip the pytest gate
#   FIXTURE_SCALE=0.25          smaller fixtures for a quick smoke run
#   FIXTURES_ROOT=<dir>         default ~/arrow_rs_repl_fixtures
#   REPEAT=3                    median-of-N per cell (default 3 — these are the
#                               numbers we act on, so default to medians)
#   ONLY=binsweep,tensors       run a subset of stages
#   FORCE_SETUP=1               re-run setup.sh even if the env already imports
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"  # uv + cargo (setup.sh installs)
FIXTURES_ROOT="${FIXTURES_ROOT:-$HOME/arrow_rs_repl_fixtures}"
FIXTURE_SCALE="${FIXTURE_SCALE:-1.0}"
REPEAT="${REPEAT:-3}"
ONLY="${ONLY:-}"

say() { printf '\n\033[1;35m### %s\033[0m\n' "$*"; }

say "1/4 environment (setup.sh)"
if [ "${FORCE_SETUP:-0}" != "1" ] && [ -f "$SCRIPT_DIR/env.sh" ] && \
   ( source "$SCRIPT_DIR/env.sh" >/dev/null 2>&1 && \
     python -c "import ray, ray_data_arrow_rs" >/dev/null 2>&1 ); then
  say "environment already set up (env.sh + imports OK) — skipping setup.sh (FORCE_SETUP=1 to redo)"
else
  bash "$SCRIPT_DIR/setup.sh"
fi
source "$SCRIPT_DIR/env.sh"

say "installing test deps (pytest, pandas)"
uv pip install --python "$(command -v python)" -q pytest pandas

if [ "${SKIP_TESTS:-0}" != "1" ]; then
  say "2/4 correctness gate: arrow-rs suite"
  python -m pytest "$REPO/python/ray/data/tests/datasource/test_arrow_rs_parquet_reader.py" \
    -q --tb=short -p no:cacheprovider
  say "2/4 correctness gate: parquet V2 suite"
  python -m pytest "$REPO/python/ray/data/tests/datasource/test_read_parquet_v2.py" \
    -q --tb=short -p no:cacheprovider
else
  say "2/4 SKIP_TESTS=1 — correctness gate skipped"
fi

say "3/4 fixtures (root=$FIXTURES_ROOT scale=$FIXTURE_SCALE, replication shapes)"
python "$SCRIPT_DIR/gen_local_fixtures.py" --root "$FIXTURES_ROOT" \
  --scale "$FIXTURE_SCALE" --shapes bin_sweep,tensors_wide,tensors_cp,fat_col

say "4/4 replication matrix (repeat=$REPEAT only=${ONLY:-all})"
python "$SCRIPT_DIR/replication_matrix.py" \
  --fixture-root "$FIXTURES_ROOT" \
  --repeat "$REPEAT" \
  ${ONLY:+--only "$ONLY"}

say "DONE — summary printed above; full logs under $SCRIPT_DIR/replication_runs/"
