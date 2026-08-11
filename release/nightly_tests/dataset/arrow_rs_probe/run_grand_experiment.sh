#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ONE command for a fresh Linux box: environment -> correctness gate ->
# fixtures -> the grand tuning experiment (arrow-rs vs PyArrow on the new
# footer-based planner).
#
#   bash release/nightly_tests/dataset/arrow_rs_probe/run_grand_experiment.sh
#
# What it does, in order:
#   1. setup.sh            venv + commit-matched Ray wheel + local-source symlink
#                          + Rust toolchain + native crate build + end-to-end check
#   2. correctness gate    the two pytest suites. The port to #64985 has only been
#                          verified statically — benchmarking an incorrect reader
#                          is worthless, so red tests ABORT the run (SKIP_TESTS=1
#                          to override while debugging).
#   3. fixtures            gen_local_fixtures.py (5 shapes, ~3.5 GiB, idempotent)
#   4. experiment          grand_experiment.py stages A-D locally; stage E (S3)
#                          engages iff ARROW_RS_S3_BUCKET is exported.
#
# Knobs (env):
#   SKIP_TESTS=1                skip the pytest gate
#   FIXTURE_SCALE=0.25          smaller fixtures for a quick smoke run
#   FIXTURES_ROOT=<dir>         default ~/arrow_rs_grand_fixtures
#   REPEAT=3                    median-of-N per cell (default 1)
#   STAGES=A,B                  subset of A,B,C,D,E (default all)
#   ARROW_RS_S3_BUCKET=s3://... scratch bucket -> enables stage E
#   plus setup.sh's own SKIP_RAY / SKIP_CRATE / SKIP_APT for re-runs.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"  # uv + cargo (setup.sh installs)
FIXTURES_ROOT="${FIXTURES_ROOT:-$HOME/arrow_rs_grand_fixtures}"
FIXTURE_SCALE="${FIXTURE_SCALE:-1.0}"
REPEAT="${REPEAT:-1}"
STAGES="${STAGES:-A,B,C,D,E}"

say() { printf '\n\033[1;35m### %s\033[0m\n' "$*"; }

say "1/4 environment (setup.sh)"
bash "$SCRIPT_DIR/setup.sh"
# env.sh (written by setup.sh) holds venv activation + RAY_ADDRESS=local +
# the memory guard that keeps an OOM from killing the whole node.
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

say "3/4 fixtures (root=$FIXTURES_ROOT scale=$FIXTURE_SCALE)"
python "$SCRIPT_DIR/gen_local_fixtures.py" --root "$FIXTURES_ROOT" --scale "$FIXTURE_SCALE"

say "4/4 grand experiment (stages=$STAGES repeat=$REPEAT s3=${ARROW_RS_S3_BUCKET:-off})"
python "$SCRIPT_DIR/grand_experiment.py" \
  --fixtures-root "$FIXTURES_ROOT" \
  --repeat "$REPEAT" \
  --stages "$STAGES"

say "DONE — summary.md printed above; full logs under $SCRIPT_DIR/grand_runs/"
