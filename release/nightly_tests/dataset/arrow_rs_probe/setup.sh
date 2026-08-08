#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# One-shot environment setup for the arrow-rs Linux + S3 read probe.
#
# Brings a fresh box (Linux x86-64, or macOS/arm64 for dev) to the point where you
# can run  gen_s3_fixtures.py  and  run_matrix.py  in this directory, by:
#   1. ensuring a Python 3.12 venv (uv-managed),
#   2. installing a Ray nightly wheel matching this branch's base commit + symlinking
#      THIS repo's python/ray over it (so the arrow-rs reader source is live) — a
#      "latest" wheel drifts from the branch's compiled protobufs and asserts
#      "out of sync" at import. Skip with SKIP_RAY=1.
#   3. installing the Rust toolchain (rustup) + maturin,
#   4. building the native crate `ray_data_arrow_rs` into the venv,
#   5. installing probe deps (psutil, numpy, pyarrow, aiohttp, awscli),
#   6. verifying the arrow-rs read path actually engages end to end.
#
# Idempotent: re-running skips work already done. Everything goes into the venv /
# ~/.cargo — nothing touches the system Python. This is the whole fresh-workspace
# recovery: clone the branch, then run this.
#
# Usage (from anywhere in the checkout):
#   bash release/nightly_tests/dataset/arrow_rs_probe/setup.sh
#
# Knobs (env vars):
#   RAY_VENV=<path>      venv to use/create             (default: <repo>/.venv)
#   RAY_WHEEL_URL=<url>  Ray nightly wheel to install   (default: cp312 linux/mac)
#   SKIP_RAY=1           don't touch Ray (already installed + symlinked)
#   SKIP_APT=1           don't apt-get build deps (build-essential, python3-dev)
#   SKIP_CRATE=1         don't (re)build the Rust crate
# ---------------------------------------------------------------------------
set -euo pipefail

# --- locate the repo (this script lives at <repo>/release/nightly_tests/dataset/arrow_rs_probe) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
CRATE="$REPO/python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs"
RAY_VENV="${RAY_VENV:-$REPO/.venv}"
OS="$(uname -s)"; ARCH="$(uname -m)"

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

say "repo=$REPO  venv=$RAY_VENV  os=$OS/$ARCH"

# --- -1. git remotes: ensure `upstream` exists + is fetched. The wheel pick below
# uses merge-base with upstream/master; on a fresh clone of the fork the remote
# is missing and the script silently falls back to a hardcoded SHA. ---
if ! git -C "$REPO" remote get-url upstream >/dev/null 2>&1; then
  say "adding upstream remote (ray-project/ray)"
  git -C "$REPO" remote add upstream https://github.com/ray-project/ray.git
fi
say "fetching upstream master (for wheel merge-base)"
git -C "$REPO" fetch --quiet upstream master || say "WARN: upstream fetch failed; using fallback SHA"

# --- 0. system build deps (Linux only; the crate links libpython + needs a C toolchain) ---
if [ "$OS" = "Linux" ] && [ "${SKIP_APT:-0}" != "1" ] && command -v apt-get >/dev/null 2>&1; then
  say "apt: build-essential + python3-dev + curl (sudo)"
  sudo apt-get update -qq
  sudo apt-get install -y -qq build-essential python3-dev curl pkg-config
fi

# --- 1. uv + venv ---
if ! command -v uv >/dev/null 2>&1; then
  say "installing uv (official installer)"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
fi
if [ ! -x "$RAY_VENV/bin/python" ]; then
  say "creating venv at $RAY_VENV (python 3.12)"
  uv venv --python 3.12 "$RAY_VENV"
fi
PY="$RAY_VENV/bin/python"
PIP() { uv pip install --python "$PY" "$@"; }
say "python: $($PY --version)"

# --- 2. Ray nightly + local-source symlink ---
# CRITICAL: the wheel's compiled protobuf must match the branch's Python source, or
# `custom_types.py` asserts "out of sync" at import. setup-dev.py symlinks THIS repo's
# python/ray over the wheel, so we install the per-commit nightly built from the
# branch's base commit (merge-base with upstream/master), NOT "latest" — latest drifts.
BASE_SHA="$(git -C "$REPO" merge-base HEAD upstream/master 2>/dev/null \
            || git -C "$REPO" merge-base HEAD origin/master 2>/dev/null \
            || echo 7dc67bed3ba2f3504325b206a70adcc470422860)"
if [ "${SKIP_RAY:-0}" != "1" ]; then
  if [ -z "${RAY_WHEEL_URL:-}" ]; then
    if [ "$OS" = "Linux" ]; then
      PYTAG=cp312; PLAT=manylinux2014_x86_64
    elif [ "$ARCH" = "arm64" ]; then
      PYTAG=cp312; PLAT=macosx_11_0_arm64
    else
      PYTAG=cp312; PLAT=macosx_10_15_x86_64
    fi
    RAY_WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/master/${BASE_SHA}/ray-3.0.0.dev0-${PYTAG}-${PYTAG}-${PLAT}.whl"
  fi
  say "installing Ray nightly (base commit ${BASE_SHA:0:12}): $RAY_WHEEL_URL"
  # Wipe any prior ray install FIRST. A re-run over a setup-dev'd tree has symlinked
  # subpackages (ray/workflow -> local source); pip/uv --force-reinstall dies trying
  # to rmdir a symlink ("Not a directory"). Removing the dir just unlinks those
  # symlinks (never touches the repo source they point to), so a clean install lands
  # the commit-matched wheel — and swaps a same-version "latest" wheel too.
  SITE="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
  rm -rf "$SITE/ray" "$SITE"/ray-*.dist-info "$SITE"/ray_*.dist-info 2>/dev/null || true
  PIP "ray[data] @ $RAY_WHEEL_URL"
  # Symlink THIS repo's python/ray over the installed wheel so the local
  # arrow-rs reader source is what actually runs (mirrors the mac dev setup).
  say "symlinking local python/ray via setup-dev.py"
  "$PY" "$REPO/python/ray/setup-dev.py" -y
else
  say "SKIP_RAY=1 — assuming Ray is installed and python/ray is symlinked"
fi

# --- 3. Rust toolchain + maturin ---
if ! command -v cargo >/dev/null 2>&1; then
  say "installing Rust via rustup (official installer)"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
export PATH="$HOME/.cargo/bin:$PATH"
say "rustc: $(rustc --version)"
PIP maturin

# --- 4. build the native crate into the venv ---
if [ "${SKIP_CRATE:-0}" != "1" ]; then
  say "building ray_data_arrow_rs (maturin develop --release) — compiles arrow/parquet, ~2-5 min"
  # maturin refuses if BOTH VIRTUAL_ENV and CONDA_PREFIX are set (common when a
  # base conda env is active); unset CONDA_PREFIX for this build only.
  ( cd "$CRATE" && unset CONDA_PREFIX && VIRTUAL_ENV="$RAY_VENV" "$RAY_VENV/bin/maturin" develop --release )
else
  say "SKIP_CRATE=1 — assuming ray_data_arrow_rs is already built"
fi

# --- 5. probe Python deps ---
# aiohttp: NOT pulled in by the ray[data] extra, but the runtime-env agent imports
# it; without it the agent crashes and the raylet fate-shares (`ray.init()` hangs
# indefinitely) — cost a day on the 2026-07-27 workspace run.
# awscli: gen_s3_fixtures.py uploads fixtures via `aws s3 sync`.
say "installing probe deps (psutil, numpy, pyarrow, aiohttp, awscli)"
PIP psutil numpy pyarrow aiohttp awscli
# aiohttp failure mode is silent (runtime-env agent crashes -> ray.init hangs
# forever), so verify the import loudly here instead of debugging a hang later.
"$PY" -c "import aiohttp, psutil, numpy, pyarrow" \
  || { say "FATAL: probe dep import failed (aiohttp/psutil/numpy/pyarrow)"; exit 1; }

# --- 6. verify the arrow-rs path actually engages ---
say "verifying arrow-rs read path end to end"
RAY_ADDRESS=local RAY_DATA_USE_DATASOURCE_V2=1 RAY_DATA_USE_ARROW_RS_PARQUET_READER=1 \
  RAY_task_events_report_interval_ms=0 \
  "$PY" - <<'PYEOF'
import os, tempfile
os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)   # Anyscale platform hook not in this venv
os.environ.pop("RAY_RUNTIME_ENV_PLUGINS", None)  # platform cgroup plugin crashes the agent
import numpy as np, pyarrow as pa, pyarrow.parquet as pq
import ray_data_arrow_rs  # noqa: F401  -> import must succeed (crate built)
import ray

d = tempfile.mkdtemp()
p = os.path.join(d, "t.parquet")
pq.write_table(pa.table({"a": np.arange(1000), "b": np.arange(1000) * 1.5}),
               p, write_page_index=True)
ray.init(address="local", include_dashboard=False,
         ignore_reinit_error=True, log_to_driver=False)
ds = ray.data.read_parquet(p)
assert ds.count() == 1000, ds.count()
assert ds.sum("a") == sum(range(1000))
print("OK  ray", ray.__version__, " arrow-rs read path verified (count + sum match)")
ray.shutdown()
PYEOF

# --- 7. write env.sh: the complete probe environment in one sourceable file, so a
# node restart never means retyping exports. Also hook it into ~/.bashrc
# (idempotent) so fresh shells come up ready. ---
say "writing $SCRIPT_DIR/env.sh + ~/.bashrc hook"
cat > "$SCRIPT_DIR/env.sh" <<ENVEOF
# arrow_rs probe environment — source this (auto-generated by setup.sh; re-run
# setup.sh to regenerate). Safe to source repeatedly.
source "$RAY_VENV/bin/activate"
[ -f "\$HOME/.cargo/env" ] && source "\$HOME/.cargo/env"

export RAY_ADDRESS=local
export RAY_task_events_report_interval_ms=0
unset RAY_RUNTIME_ENV_HOOK RAY_RUNTIME_ENV_PLUGINS

# MEMORY GUARD: make Ray's monitor kill an over-allocating task BEFORE the OS
# OOM-killer takes down the raylet (which terminates the whole workspace). The
# 2026-08-03 run OOM-killed the node on the wide arrow_rs S3 read without this.
export RAY_memory_monitor_refresh_ms=250
export RAY_memory_usage_threshold=0.8

# The benchmark bucket lives in us-west-2 (always — it was created there and
# buckets never move). Cross-region reads skew timings AND cost per-GB.
export AWS_DEFAULT_REGION=us-west-2
export P=s3://arrowrs-bench-21f6c795/wide_schema/primitives
export IMG=s3://arrowrs-bench-21f6c795/imagenet/parquet
ENVEOF
if ! grep -q "arrow_rs_probe/env.sh" "$HOME/.bashrc" 2>/dev/null; then
  echo "[ -f \"$SCRIPT_DIR/env.sh\" ] && source \"$SCRIPT_DIR/env.sh\"" >> "$HOME/.bashrc"
  say "added env.sh source line to ~/.bashrc"
fi

say "DONE."
cat <<EOF

Environment is written to $SCRIPT_DIR/env.sh (new shells source it via ~/.bashrc).
For THIS shell:

  source $SCRIPT_DIR/env.sh

Fixtures live in the bucket and survive node restarts; regenerate only if the
bucket was wiped:

  python gen_s3_fixtures.py --bucket s3://arrowrs-bench-21f6c795

Then:

  python run_matrix.py --wide-path \$P --imagenet-path \$IMG
EOF
