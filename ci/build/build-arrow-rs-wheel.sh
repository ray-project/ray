#!/bin/bash
# build-arrow-rs-wheel.sh -- Build an abi3 wheel for the `ray_data_arrow_rs`
# native Parquet decoder.
#
# WHY THIS EXISTS
# ---------------
# Ray cannot build Rust in-tree today:
#   * python/ has no pyproject.toml -- the build is legacy setup.py/setuptools,
#     so maturin cannot be Ray's build backend.
#   * setup.py compiles nothing; native artifacts come from Bazel and are
#     shutil.copy'd into build_lib by pip_run(). Rust would be the first thing
#     setuptools actually builds.
#   * rules_rust is not an option: Ray is on Bazel 7.5.0 in WORKSPACE mode
#     (.bazelrc: `common --noenable_bzlmod`, no MODULE.bazel), and rules_rust
#     removed WORKSPACE support in 0.71.0. The piece we would actually need,
#     rules_rust_pyo3, has never had a WORKSPACE path in any release.
#
# So the crate is built ONCE here, into a normal wheel, and everything
# downstream (CI images, release BYOD images) just `pip install`s that wheel.
# crates.io is touched by this one script rather than by every image build --
# which keeps the network surface to a single known hole while the CodeArtifact
# /pip-mirror work catches up.
#
# The wheel this produces is deliberately THE SAME ARTIFACT a future
# `ray-data-arrow-rs` PyPI package would ship, just from a different index. That
# makes this a dress rehearsal for the long-term split rather than throwaway
# scaffolding.
#
# ABI3: the crate builds against pyo3's `abi3-py39` feature, so ONE binary per
# platform works on every CPython >= 3.9. The matrix is 1 wheel per platform,
# not 1 per (platform x python version).
#
# Usage:
#   ci/build/build-arrow-rs-wheel.sh [OUTPUT_DIR]
#
# Environment:
#   CRATE_DIR       -- crate source dir (default: rust/ray_data_arrow_rs)
#   RUST_TOOLCHAIN  -- rust toolchain to install/use (default: stable)
#   MATURIN_VERSION -- maturin version to pin (default: 1.14.1)
#   CARGO_HOME      -- honoured if preset, so callers can mount a cargo cache

set -exuo pipefail

OUTPUT_DIR="${1:-${PWD}/.whl-arrow-rs}"
CRATE_DIR="${CRATE_DIR:-rust/ray_data_arrow_rs}"
RUST_TOOLCHAIN="${RUST_TOOLCHAIN:-stable}"
MATURIN_VERSION="${MATURIN_VERSION:-1.14.1}"

if [[ ! -f "${CRATE_DIR}/Cargo.toml" ]]; then
  echo "error: no Cargo.toml under ${CRATE_DIR}" >&2
  echo "       (expected the crate at the repo-root rust/ directory; set CRATE_DIR to override)" >&2
  exit 1
fi

# --- Rust toolchain -------------------------------------------------------
# None of Ray's build images ship rustc: ci/build/build-manylinux-forge.sh
# installs bazelisk, node and the JDK only, and there is not a single
# cargo/rustup/rustc reference anywhere under ci/ or docker/.
if ! command -v cargo >/dev/null 2>&1; then
  export CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
  export RUSTUP_HOME="${RUSTUP_HOME:-${HOME}/.rustup}"
  curl -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
  sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain "${RUST_TOOLCHAIN}" --no-modify-path
  rm -f /tmp/rustup-init.sh
fi
export PATH="${CARGO_HOME:-${HOME}/.cargo}/bin:${PATH}"
cargo --version
rustc --version

# Fail fast rather than silently resolving a different dependency set than the
# one that was reviewed. The committed Cargo.lock is the reproducibility story.
if [[ ! -f "${CRATE_DIR}/Cargo.lock" ]]; then
  echo "error: ${CRATE_DIR}/Cargo.lock is missing; refusing to build unpinned" >&2
  exit 1
fi

# --- Build ----------------------------------------------------------------
mkdir -p "${OUTPUT_DIR}"
pip install --no-cache-dir "maturin==${MATURIN_VERSION}"

# --locked: build exactly the reviewed Cargo.lock, never re-resolve.
maturin build \
  --release \
  --locked \
  --manifest-path "${CRATE_DIR}/Cargo.toml" \
  --out "${OUTPUT_DIR}"

# --- Verify ---------------------------------------------------------------
# An abi3 build must be tagged abi3, not cp3XX -- if the feature silently
# stopped applying we would start needing one wheel per Python version and
# nothing would tell us until an install failed on another interpreter.
shopt -s nullglob
wheels=("${OUTPUT_DIR}"/*.whl)
if [[ ${#wheels[@]} -ne 1 ]]; then
  echo "error: expected exactly 1 wheel in ${OUTPUT_DIR}, got ${#wheels[@]}" >&2
  exit 1
fi
wheel="${wheels[0]}"
if [[ "$(basename "${wheel}")" != *"abi3"* ]]; then
  echo "error: ${wheel} is not an abi3 wheel -- did the pyo3 abi3-py39 feature get dropped?" >&2
  exit 1
fi

echo "built ${wheel}"
