# syntax=docker/dockerfile:1.3-labs
#
# Bazel built from source inside the manylinux2014 image, for the images that run
# Bazel there. Bazel's own Linux releases need glibc >= 2.25 since Bazel 8;
# manylinux2014 is CentOS 7 with glibc 2.17, so the official binary cannot start.
# Building here links everything against glibc 2.17. The version comes from
# .bazelversion, so bumping that file rebuilds this image.

ARG MANYLINUX_VERSION
ARG HOSTTYPE
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS build

ARG HOSTTYPE

COPY .bazelversion /tmp/.bazelversion

RUN <<'EOF'
#!/bin/bash

set -euo pipefail

version="$(tr -d '[:space:]' < /tmp/.bazelversion)"
case "${HOSTTYPE}" in
  x86_64) jdk_arch=x64 ;;
  aarch64) jdk_arch=aarch64 ;;
  *) echo "unsupported HOSTTYPE ${HOSTTYPE}" >&2; exit 1 ;;
esac

work="${HOME}/bazel-src"
mkdir -p "${work}/jdk21" "${work}/src"

# Bootstrapping Bazel 8 needs JDK 21; the image only has JDK 8. Temurin builds
# target glibc 2.17.
curl -fsSL "https://api.adoptium.net/v3/binary/latest/21/ga/linux/${jdk_arch}/jdk/hotspot/normal/eclipse" \
  | tar -xz -C "${work}/jdk21" --strip-components=1

curl -fsSL -o /tmp/dist.zip \
  "https://github.com/bazelbuild/bazel/releases/download/${version}/bazel-${version}-dist.zip"
unzip -q /tmp/dist.zip -d "${work}/src"
rm /tmp/dist.zip

# devtoolset's enable script reads unset variables.
set +u
source /opt/rh/devtoolset-10/enable
set -u
export JAVA_HOME="${work}/jdk21"
export PATH="${JAVA_HOME}/bin:${PATH}"

cd "${work}/src"

# Stage 1: compile.sh produces bazel_nojdk, which needs a JDK 21 at runtime.
env EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk" bash ./compile.sh

# Stage 2: use it to build the normal, self-contained //src:bazel with an
# embedded JDK, so the result runs on the image as-is.
./output/bazel build -c opt --tool_java_runtime_version=local_jdk //src:bazel
cp bazel-bin/src/bazel /tmp/bazel
./output/bazel shutdown

# Smoke test: start a server from the final binary, which exercises its
# embedded JDK on glibc 2.17, not just the client.
mkdir -p /tmp/smoke
cd /tmp/smoke
touch MODULE.bazel
/tmp/bazel --output_user_root=/tmp/smoke-root info release | grep -Fx "release ${version}"
/tmp/bazel --output_user_root=/tmp/smoke-root shutdown
EOF

FROM scratch
COPY --from=build /tmp/bazel /bazel
