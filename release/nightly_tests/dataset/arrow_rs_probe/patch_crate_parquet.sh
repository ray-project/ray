#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# A/B tool for TODO item 1o (fat_col wall loss): rebuild ray_data_arrow_rs
# against a locally patched parquet 59.1.0, or revert to the stock crates.io
# build. The patch (patches/parquet-59.1.0-dict-reserve.diff) pre-sizes the
# values buffer in OffsetBuffer::extend_from_dictionary from the dictionary's
# average value length — the omission that makes fat dictionary-encoded binary
# columns pay ~one extra full copy (findings T20; prior art apache/arrow-rs
# #5250, which used an exact per-key sum and regressed small strings — this is
# the O(1) variant that doesn't).
#
#   bash patch_crate_parquet.sh           # vendor + patch + rebuild (PATCHED arm)
#   REVERT=1 bash patch_crate_parquet.sh  # restore stock parquet + rebuild
#
# The A/B for the fatcol stage is then:
#   ONLY=fatcol bash run_replication.sh                      # stock arm
#   bash patch_crate_parquet.sh
#   ONLY=fatcol bash run_replication.sh                      # patched arm
#   REVERT=1 bash patch_crate_parquet.sh                     # leave box stock
#
# Mechanics: copies the pristine parquet-59.1.0 source out of the cargo
# registry cache into <crate>/vendor/, applies the diff, and points
# [patch.crates-io] at it via <crate>/.cargo/config.toml (never Cargo.toml, so
# the tree stays clean). Cargo.lock is backed up before the first patch and
# restored on REVERT — do not commit a lock file that lost its parquet
# checksum line. vendor/ and .cargo/ are build-local; never commit them.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
CRATE="$REPO/python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs"
DIFF="$SCRIPT_DIR/patches/parquet-59.1.0-dict-reserve.diff"
VENDOR="$CRATE/vendor/parquet-59.1.0-dict-reserve"
CARGO_CFG="$CRATE/.cargo/config.toml"
LOCK_BAK="$CRATE/Cargo.lock.stock"

say() { printf '\n\033[1;35m### %s\033[0m\n' "$*"; }

# Environment: prefer the probe env.sh (Linux boxes), else whatever venv can
# already see maturin, else the repo-local .venv (macOS dev).
if [ -f "$SCRIPT_DIR/env.sh" ]; then
  source "$SCRIPT_DIR/env.sh"
elif ! command -v maturin >/dev/null 2>&1 && [ -f "$REPO/.venv/bin/activate" ]; then
  source "$REPO/.venv/bin/activate"
fi
export PATH="$HOME/.cargo/bin:$PATH"
command -v cargo >/dev/null || { echo "cargo not on PATH — run setup.sh first"; exit 1; }
command -v maturin >/dev/null || { echo "maturin not on PATH — 'uv pip install maturin' into the venv"; exit 1; }

rebuild() {
  say "rebuilding ray_data_arrow_rs (maturin develop --release)"
  ( cd "$CRATE" && unset CONDA_PREFIX && VIRTUAL_ENV="${VIRTUAL_ENV}" maturin develop --release )
  python - <<'PYEOF'
import ray_data_arrow_rs as rs
print("crate imports OK:", rs.__name__)
PYEOF
}

if [ "${REVERT:-0}" = "1" ]; then
  say "REVERT: removing the parquet patch"
  rm -f "$CARGO_CFG"
  rmdir "$CRATE/.cargo" 2>/dev/null || true
  rm -rf "$CRATE/vendor"
  if [ -f "$LOCK_BAK" ]; then
    mv "$LOCK_BAK" "$CRATE/Cargo.lock"
  fi
  rebuild
  say "stock crate restored"
  exit 0
fi

[ -f "$DIFF" ] || { echo "missing $DIFF"; exit 1; }

say "vendoring pristine parquet 59.1.0 from the cargo registry cache"
( cd "$CRATE" && cargo fetch )
SRC="$(find "$HOME"/.cargo/registry/src -maxdepth 2 -type d -name parquet-59.1.0 2>/dev/null | head -1)"
[ -n "$SRC" ] || { echo "parquet-59.1.0 not in the registry cache even after cargo fetch"; exit 1; }
rm -rf "$VENDOR"
mkdir -p "$(dirname "$VENDOR")"
cp -R "$SRC" "$VENDOR"

say "applying $DIFF"
# Diff paths are relative to the arrow-rs repo root (a/parquet/src/...);
# the vendored dir is the parquet crate root, so strip two components.
patch -d "$VENDOR" -p2 < "$DIFF"

say "pointing [patch.crates-io] at the vendored copy (via .cargo/config.toml)"
[ -f "$LOCK_BAK" ] || cp "$CRATE/Cargo.lock" "$LOCK_BAK"
mkdir -p "$CRATE/.cargo"
cat > "$CARGO_CFG" <<'CFGEOF'
# Written by patch_crate_parquet.sh (TODO 1o A/B) — NEVER COMMIT.
# REVERT=1 bash patch_crate_parquet.sh removes this and restores Cargo.lock.
[patch.crates-io]
parquet = { path = "vendor/parquet-59.1.0-dict-reserve" }
CFGEOF

rebuild

say "verifying the patched source is what got built"
if ( cd "$CRATE" && cargo metadata --format-version 1 --offline 2>/dev/null || cd "$CRATE" && cargo metadata --format-version 1 ) | grep -q "parquet-59.1.0-dict-reserve"; then
  echo "OK: cargo resolves parquet to vendor/parquet-59.1.0-dict-reserve"
else
  echo "WARNING: vendored parquet not visible in cargo metadata — check [[patch.unused]] in Cargo.lock"
  exit 1
fi
say "patched crate installed — run the fatcol stage now"
