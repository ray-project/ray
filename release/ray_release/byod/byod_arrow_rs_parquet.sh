#!/bin/bash
# Install the `ray_data_arrow_rs` native Parquet decoder into a release-test
# BYOD image, so tests that read Parquet exercise the Rust reader instead of
# PyArrow. Runs as a Docker RUN layer at IMAGE BUILD time (has internet).
#
# DIFFERENCES FROM THE VERSION IN PR #65117
# -----------------------------------------
# That version did:
#     curl -sL https://github.com/<author>/ray/archive/refs/heads/<branch>.tar.gz
#     pip3 install /tmp/ray-<branch>/<crate subdir>
# i.e. every release-test image independently installed a Rust toolchain and
# compiled the crate from a mutable branch head on a personal fork. That means:
#   * the artifact under test is whatever that branch pointed at when the image
#     happened to build -- not reproducible, and not reviewable;
#   * a fork going away, or being force-pushed, silently changes or breaks the
#     release suite;
#   * every image build pays the full arrow/parquet compile and hits crates.io.
#
# This version installs a prebuilt, pinned wheel -- the same artifact
# ci/build/build-arrow-rs-wheel.sh produces for CI, and the same one a future
# `ray-data-arrow-rs` PyPI release would ship. Set ARROW_RS_WHEEL_URL to a
# pinned object; the fallback path builds from the in-repo crate so this still
# works before any wheel has been published.
set -exo pipefail

# Pin to an immutable artifact (commit-SHA-keyed, not a branch). Override in the
# test definition to bump.
ARROW_RS_WHEEL_URL="${ARROW_RS_WHEEL_URL:-}"

if [[ -n "${ARROW_RS_WHEEL_URL}" ]]; then
  pip3 install --no-cache-dir --no-deps "${ARROW_RS_WHEEL_URL}"
else
  # No published wheel yet: build from the crate in this checkout. Same script
  # CI uses, so the toolchain/pin/verification logic lives in exactly one place.
  echo "ARROW_RS_WHEEL_URL unset -- building from the in-repo crate" >&2
  CRATE_DIR="${CRATE_DIR:-rust/ray_data_arrow_rs}" \
    bash ci/build/build-arrow-rs-wheel.sh /tmp/whl-arrow-rs
  pip3 install --no-cache-dir --no-deps /tmp/whl-arrow-rs/*.whl
fi

# Fail the image build loudly if the crate is not importable or is a partial
# build, so it surfaces here rather than as a scanner error at test run time.
python3 -c "import ray_data_arrow_rs as m; assert hasattr(m, 'read_row_groups'), dir(m)"
