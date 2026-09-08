#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ONE command for the Linux box: the 3-part triage of the 2026-08-15 release
# A/B losses (findings M31 read_large_parquet_autoscaling, M32 write_parquet,
# M33 wide_schema tensors).
#
#   bash release/nightly_tests/dataset/arrow_rs_probe/run_loss_triage.sh
#   # with the S3 part (scratch bucket you own — fixtures are synced up):
#   ARROW_RS_S3_BUCKET=s3://arrowrs-bench-xxxx bash .../run_loss_triage.sh
#
# Each loss shape runs standalone (no Ray/no S3), through Ray on local files,
# and through Ray on S3 — both readers each, plus a MALLOC_ARENA_MAX=2 arm on
# the arrow-rs Ray cells — so one summary table says whether a loss is the
# native decoder, Ray integration (worker/allocator), or the crate's S3 path.
# See loss_triage.py's docstring for the shape -> release-test mapping.
#
# Same setup skeleton as run_replication.sh:
#   1. setup.sh          venv + commit-matched wheel + source symlink + crate
#                        (skipped when env.sh already imports; FORCE_SETUP=1 to
#                        redo — MANDATORY after a git pull that touches the crate)
#   2. fixtures          gen_local_fixtures.py: auto_rg, bin_sweep, tensors_cp, tensors_dict
#   3. matrix            loss_triage.py (S3 part auto-enabled when
#                        ARROW_RS_S3_BUCKET is set; AWS creds must be exported)
#
# Knobs (env):
#   FIXTURE_SCALE=0.25   smaller fixtures for a smoke run
#   FIXTURES_ROOT=<dir>  default ~/arrow_rs_repl_fixtures
#   REPEAT=3 WARMUP=1    per-cell medians (as in run_replication.sh)
#   SHAPES=write         subset: auto,write,tensorscp,tensorsdict,agg
#   PARTS=ray_local      subset: standalone,ray_local,ray_s3
#   FORCE_SETUP=1        re-run setup.sh even if the env already imports
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
FIXTURES_ROOT="${FIXTURES_ROOT:-$HOME/arrow_rs_repl_fixtures}"
FIXTURE_SCALE="${FIXTURE_SCALE:-1.0}"
REPEAT="${REPEAT:-3}"
WARMUP="${WARMUP:-1}"
SHAPES="${SHAPES:-auto,write,tensorscp,tensorsdict,agg}"
PARTS="${PARTS:-}"

say() { printf '\n\033[1;35m### %s\033[0m\n' "$*"; }

say "1/3 environment (setup.sh)"
# env.sh (written by setup.sh) exports its own ARROW_RS_S3_BUCKET; preserve a
# value the caller set explicitly so `ARROW_RS_S3_BUCKET=… bash run_loss_triage.sh`
# is honored rather than silently clobbered.
CALLER_S3_BUCKET="${ARROW_RS_S3_BUCKET:-}"
if [ "${FORCE_SETUP:-0}" != "1" ] && [ -f "$SCRIPT_DIR/env.sh" ] && \
   ( source "$SCRIPT_DIR/env.sh" >/dev/null 2>&1 && \
     python -c "import ray, ray_data_arrow_rs" >/dev/null 2>&1 ); then
  say "environment already set up (env.sh + imports OK) — skipping setup.sh (FORCE_SETUP=1 to redo)"
else
  bash "$SCRIPT_DIR/setup.sh"
fi
source "$SCRIPT_DIR/env.sh"
[ -n "$CALLER_S3_BUCKET" ] && export ARROW_RS_S3_BUCKET="$CALLER_S3_BUCKET"
export RAY_ADDRESS=local

say "2/3 fixtures (root=$FIXTURES_ROOT scale=$FIXTURE_SCALE: auto_rg, bin_sweep, tensors_cp, tensors_dict)"
python "$SCRIPT_DIR/gen_local_fixtures.py" --root "$FIXTURES_ROOT" \
  --scale "$FIXTURE_SCALE" --shapes auto_rg,bin_sweep,tensors_cp,tensors_dict

say "3/3 loss triage matrix (repeat=$REPEAT warmup=$WARMUP shapes=$SHAPES s3=${ARROW_RS_S3_BUCKET:-off})"
python "$SCRIPT_DIR/loss_triage.py" \
  --fixture-root "$FIXTURES_ROOT" \
  --repeat "$REPEAT" \
  --warmup "$WARMUP" \
  --shapes "$SHAPES" \
  ${PARTS:+--parts "$PARTS"}

say "DONE — summary printed above; full logs under $SCRIPT_DIR/loss_triage_runs/"
