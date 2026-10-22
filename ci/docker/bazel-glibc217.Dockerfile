# syntax=docker/dockerfile:1.3-labs
#
# Bazel built from source inside the manylinux2014 image, for the images that run
# Bazel there. Bazel's own Linux releases need glibc >= 2.25 since Bazel 8;
# manylinux2014 is CentOS 7 with glibc 2.17, so the official binary cannot start.
# Building here links it against glibc 2.17; it runs on the bundled Temurin
# JDK 21, which also supports glibc 2.17. The version comes from
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

# compile.sh produces bazel_nojdk, which runs on a separate JDK 21 instead of
# an embedded one. Building the self-contained //src:bazel from it does not
# work here: that build runs rules_java's prebuilt ijar, which itself needs
# GLIBCXX_3.4.21. So ship bazel_nojdk together with the JDK it was built with.
env EMBED_LABEL="${version}" EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk" \
  bash ./compile.sh

mkdir -p /tmp/out
cp output/bazel /tmp/out/bazel
cp -a "${work}/jdk21" /tmp/out/jdk21

# Smoke test: start a server, which runs the shipped JDK on glibc 2.17, not
# just the client.
mkdir -p /tmp/smoke
cd /tmp/smoke
touch MODULE.bazel
bazel_cmd=(/tmp/out/bazel --nohome_rc --server_javabase=/tmp/out/jdk21 --output_user_root=/tmp/smoke-root)
"${bazel_cmd[@]}" info release | grep -Fx "release ${version}"
"${bazel_cmd[@]}" shutdown
EOF

FROM scratch
COPY --from=build /tmp/out/bazel /bazel
COPY --from=build /tmp/out/jdk21 /jdk21
