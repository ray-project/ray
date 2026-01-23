# syntax=docker/dockerfile:1.3-labs
#
# Ray C++ Core Artifacts Builder
# ==============================
# Builds ray_cpp_pkg.zip containing C++ headers, libraries, and examples.
#
ARG ARCH_SUFFIX=
ARG HOSTTYPE=x86_64
ARG MANYLINUX_VERSION
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

ARG PYTHON_VERSION=3.10
ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY
ARG HOSTTYPE

WORKDIR /home/forge/ray

COPY . .

# Mounting cache dir for faster local rebuilds (architecture-specific to avoid toolchain conflicts)
RUN --mount=type=cache,target=/home/forge/.cache,uid=2000,gid=100,id=bazel-cache-${HOSTTYPE}-${PYTHON_VERSION} \
    <<'EOF'
#!/bin/bash
set -euo pipefail

PY_CODE="${PYTHON_VERSION//./}"
PY_BIN="cp${PY_CODE}-cp${PY_CODE}"
export RAY_BUILD_ENV="manylinux_py${PY_BIN}"

sudo ln -sf "/opt/python/${PY_BIN}/bin/python3" /usr/local/bin/python3
sudo ln -sf /usr/local/bin/python3 /usr/local/bin/python

if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  echo "build --remote_upload_local_results=false" >> "$HOME/.bazelrc"
fi

echo "build --repository_cache=/home/forge/.cache/bazel-repo" >> "$HOME/.bazelrc"

bazelisk build --config=ci //cpp:ray_cpp_pkg_zip

cp bazel-bin/cpp/ray_cpp_pkg.zip /home/forge/ray_cpp_pkg.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_cpp_pkg.zip /
