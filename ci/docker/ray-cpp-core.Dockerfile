# syntax=docker/dockerfile:1.3-labs
#
# Ray C++ Core Artifacts Builder
# ==============================
# Builds ray_cpp_pkg.zip containing C++ headers, libraries, and examples.
#
ARG ARCH_SUFFIX=
ARG HOSTTYPE=x86_64
ARG MANYLINUX_VERSION
FROM cr.ray.io/rayproject/bazel-glibc217-${HOSTTYPE} AS bazel

FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

ARG PYTHON_VERSION=3.10
ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY
ARG HOSTTYPE
ARG CACHE_DIR=/opt/cache

ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}
ENV BUILDKITE_CACHE_READONLY=${BUILDKITE_CACHE_READONLY}
ENV CACHE_DIR=${CACHE_DIR}
ENV DOWNLOAD_CACHE=${CACHE_DIR}/downloads
ENV BAZEL_CACHE=${CACHE_DIR}/bazel

WORKDIR /home/forge/ray

# Bazel's own Linux releases need glibc >= 2.25 since Bazel 8, and this image is
# glibc 2.17. Run the copy built for it (ci/docker/bazel-glibc217.Dockerfile)
# instead. The build calls bazelisk, and /usr/local/bin/bazel links to it.
COPY --from=bazel /bazel /usr/local/bin/bazel-glibc217
RUN sudo mv /usr/local/bin/bazelisk /usr/local/bin/bazelisk.real \
  && printf '#!/bin/sh\nexec /usr/local/bin/bazel-glibc217 "$@"\n' \
    | sudo tee /usr/local/bin/bazelisk >/dev/null \
  && sudo chmod 0755 /usr/local/bin/bazelisk

COPY . .

RUN --mount=type=cache,target=${DOWNLOAD_CACHE},uid=2000,gid=100,id=ray-downloads-${HOSTTYPE} \
    --mount=type=cache,target=${BAZEL_CACHE},uid=2000,gid=100,id=ray-bazel-${HOSTTYPE} \
    <<'EOF'
#!/bin/bash
set -euo pipefail

export BAZELISK_HOME=$DOWNLOAD_CACHE/bazelisk
REPOSITORY_CACHE=$DOWNLOAD_CACHE/repo

PY_CODE="${PYTHON_VERSION//./}"
PY_BIN="cp${PY_CODE}-cp${PY_CODE}"
export RAY_BUILD_ENV="manylinux_py${PY_BIN}"

sudo ln -sf "/opt/python/${PY_BIN}/bin/python3" /usr/local/bin/python3
sudo ln -sf /usr/local/bin/python3 /usr/local/bin/python

BAZEL_CACHE_ARGS=""
if [[ -z "${BUILDKITE_BAZEL_CACHE_URL:-}" ]]; then
  # Disable remote cache for local builds (no credentials)
  BAZEL_CACHE_ARGS="--remote_cache="
else
  # Override whatever the base image baked into ~/.bazelrc
  BAZEL_CACHE_ARGS="--remote_cache=${BUILDKITE_BAZEL_CACHE_URL}"
  if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
    BAZEL_CACHE_ARGS="$BAZEL_CACHE_ARGS --remote_upload_local_results=false"
  fi
fi

bazelisk --output_base=$BAZEL_CACHE build --config=ci \
    --repository_cache=$REPOSITORY_CACHE \
    $BAZEL_CACHE_ARGS \
    //cpp:ray_cpp_pkg_zip

cp bazel-bin/cpp/ray_cpp_pkg.zip /home/forge/ray_cpp_pkg.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_cpp_pkg.zip /
