# syntax=docker/dockerfile:1.3-labs
#
# Layers the `ray_data_arrow_rs` native Parquet decoder on top of the existing
# data CI image. Kept as a SEPARATE image, deliberately, so that:
#
#   1. rustc/cargo and the crates.io fetch stay out of datalbuild/data17build.
#      Only the arrow-rs test job pays the cost; every other data test job is
#      untouched.
#   2. crates.io enters the CI network surface in exactly one image build, whose
#      cache key is the crate source (see `srcs` in the wanda yaml), so it
#      rebuilds only when the crate actually changes. That is one known hole for
#      the CodeArtifact/pip-mirror work to close, not N.
#   3. The wheel built here is the same artifact a future `ray-data-arrow-rs`
#      PyPI package would publish -- so this image is a dress rehearsal for the
#      long-term repo split, not throwaway scaffolding.
#
# Without this, python/ray/data/tests/datasource/test_arrow_rs_parquet_reader.py
# opens with `pytest.importorskip("ray_data_arrow_rs")` and its entire test
# module SILENTLY SKIPS in premerge -- green CI over zero coverage.

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/datalbuild-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON=3.10

SHELL ["/bin/bash", "-ice"]

COPY ci/build/build-arrow-rs-wheel.sh /home/ray/ci/build/build-arrow-rs-wheel.sh
COPY rust/ray_data_arrow_rs /home/ray/rust/ray_data_arrow_rs

RUN <<EOF
#!/bin/bash

set -ex

cd /home/ray

# Build the abi3 wheel. Several minutes cold: arrow/parquet/object_store at
# opt-level=3 + lto=true + codegen-units=1. Bazel's remote cache does not cover
# cargo, which is precisely why this is a cached Docker layer rather than a step
# inside the Ray wheel build.
CRATE_DIR=rust/ray_data_arrow_rs \
  bash ci/build/build-arrow-rs-wheel.sh /home/ray/.whl-arrow-rs

# Install into the image env. --no-deps: the crate has no Python dependencies,
# and must not be allowed to perturb the depset the base image pinned.
uv pip install --system --no-deps /home/ray/.whl-arrow-rs/*.whl

# Fail the IMAGE build loudly if the extension is not importable, so a partial
# build surfaces here instead of as a silent mass `importorskip` at test time --
# which would look identical to a green run.
python -c "import ray_data_arrow_rs as m; assert hasattr(m, 'read_row_groups'), dir(m)"

# Discard the toolchain and build tree: rustup + cargo registry + target/ is
# ~2GB, and nothing at test time needs a compiler.
rm -r -f /home/ray/.cargo /home/ray/.rustup /home/ray/rust /home/ray/.whl-arrow-rs

EOF
