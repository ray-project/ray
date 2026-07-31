#!/bin/bash
# Build + install the ray_data_arrow_rs native Parquet decoder into the
# release-test BYOD image, so tests that read Parquet exercise the Rust reader
# instead of PyArrow. Runs as a Docker RUN layer at IMAGE BUILD time (has
# internet). The crate is NOT in the Ray wheel or on PyPI, so without this the
# reader would raise on `import ray_data_arrow_rs`.
#
# We build from source (rather than shipping a prebuilt wheel): install a
# minimal Rust toolchain, fetch the crate source from the branch, and let
# `pip install <dir>` drive maturin via the pyproject build backend. Only rustc
# needs to pre-exist; pip installs maturin itself in the isolated build env, and
# the committed Cargo.lock makes the dependency set reproducible.
#
# Build cost (a few minutes to compile arrow/parquet/object_store in release)
# is paid once per image build, is cached, and is NOT part of any test's
# measured runtime — it happens before the cluster boots.
set -exo pipefail

BRANCH="arrow-rs-parquet-reader-pr"
CRATE_SUBDIR="python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs"

# Minimal stable Rust toolchain (~1 min for rustup itself).
curl -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain stable
export PATH="$HOME/.cargo/bin:$PATH"

# Fetch the crate source from the branch head and build+install it. pip reads
# the maturin build-backend from pyproject.toml and compiles the extension.
curl -sL "https://github.com/AarryaSaraf/ray/archive/refs/heads/${BRANCH}.tar.gz" \
  | tar xz -C /tmp
pip3 install --no-cache-dir "/tmp/ray-${BRANCH}/${CRATE_SUBDIR}"

# Fail the image build loudly if the crate isn't importable / is a partial
# build, so it surfaces here instead of as a scanner error at test run time.
python3 -c "import ray_data_arrow_rs as m; assert hasattr(m, 'read_row_groups')"
