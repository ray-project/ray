# syntax=docker/dockerfile:1.3-labs
ARG ARCH_SUFFIX=
FROM cr.ray.io/rayproject/manylinux$ARCH_SUFFIX AS builder

ARG PYTHON_VERSION=3.9
ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY

WORKDIR /home/forge/ray

COPY . .

RUN <<EOF
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

bazelisk build --config=ci //:ray_pkg_zip //:ray_py_proto_zip

cp bazel-bin/ray_pkg.zip /home/forge/ray_pkg.zip
cp bazel-bin/ray_py_proto.zip /home/forge/ray_py_proto.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_pkg.zip /home/forge/ray_py_proto.zip /
