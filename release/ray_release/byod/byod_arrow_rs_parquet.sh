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

# The crate source MUST match the Ray code under test: the reader passes
# keyword args that only exist in matching crate versions, so a drifted .so
# fails late (or worse, silently). Prefer the build's own commit
# (BUILDKITE_COMMIT, if the release pipeline exports it into this layer) so
# branch pushes during a build can't desync the pair; fall back to the branch
# head otherwise. The echo makes the chosen ref auditable in the image-build
# log — check it on the first run after any branch switch.
BRANCH="arrow-rs-on-64985"
REF="${BUILDKITE_COMMIT:-refs/heads/${BRANCH}}"
CRATE_SUBDIR="python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs"

# Minimal stable Rust toolchain (~1 min for rustup itself).
curl -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain stable
export PATH="$HOME/.cargo/bin:$PATH"

# Fetch the crate source at the pinned ref and build+install it. pip reads
# the maturin build-backend from pyproject.toml and compiles the extension.
# -f fails the pipe on HTTP errors (404 = bad ref) instead of feeding tar
# an error page; --strip-components drops the ref-dependent top-level dir.
echo "arrow-rs byod: fetching crate source at ${REF}"
mkdir -p /tmp/ray-src
curl -sfL "https://github.com/AarryaSaraf/ray/archive/${REF}.tar.gz" \
  | tar xz -C /tmp/ray-src --strip-components=1
pip3 install --no-cache-dir "/tmp/ray-src/${CRATE_SUBDIR}"

# Fail the image build loudly if the crate isn't importable / is a partial
# build, so it surfaces here instead of as a scanner error at test run time.
python3 -c "import ray_data_arrow_rs as m; assert hasattr(m, 'read_row_groups')"
