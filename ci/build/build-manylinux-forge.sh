#!/bin/bash
# Build an environment for building ray wheels for the manylinux2014 platform.

set -exuo pipefail

BAZELISK_VERSION="v1.26.0"

platform="unknown"

case "${OSTYPE}" in
  msys)
    echo "Platform is Windows."
    platform="windows"
    # No installer for Windows
    ;;
  darwin*)
    echo "Platform is Mac OS X."
    platform="darwin"
    ;;
  linux*)
    echo "Platform is Linux (or WSL)."
    platform="linux"
    ;;
  *)
    echo "Unrecognized platform."
    exit 1
esac

echo "Architecture(HOSTTYPE) is ${HOSTTYPE}"

if [[ ! -e /usr/bin/nproc ]]; then
  echo -e '#!/bin/bash\necho 10' > "/usr/bin/nproc"
  chmod +x /usr/bin/nproc
fi

# Install ray cpp dependencies.
sudo yum -y install unzip zip sudo openssl xz
if [[ "${HOSTTYPE-}" == "x86_64" ]]; then
  sudo yum -y install libasan-4.8.5-44.el7.x86_64 libubsan-7.3.1-5.10.el7.x86_64 \
    devtoolset-8-libasan-devel.x86_64
fi

# Install ray java dependencies.
if [[ "${RAY_INSTALL_JAVA}" == "1" ]]; then
  sudo yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel maven
  java -version
  JAVA_BIN="$(readlink -f "$(command -v java)")"
  echo "java_bin path ${JAVA_BIN}"
  export JAVA_HOME="${JAVA_BIN%jre/bin/java}"
fi

# Install ray dashboard dependencies.
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
source "$HOME"/.nvm/nvm.sh

NODE_VERSION="14"
nvm install "$NODE_VERSION"
nvm use "$NODE_VERSION"

# Install bazel
mkdir -p "$HOME"/bin
if [[ "${HOSTTYPE}" == "aarch64" || "${HOSTTYPE}" = "arm64" ]]; then
  # architecture is "aarch64", but the bazel tag is "arm64"
  url="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-${platform}-arm64"
elif [ "${HOSTTYPE}" = "x86_64" ]; then
  url="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-${platform}-amd64"
else
  echo "Could not found matching bazelisk URL for platform ${platform} and architecture ${HOSTTYPE}"
  exit 1
fi
curl -sSfL -o "$HOME"/bin/bazelisk ${url}
chmod +x "$HOME"/bin/bazelisk
sudo ln -sf "$HOME"/bin/bazelisk /usr/local/bin/bazel

# Use python3.9 as default python3
sudo ln -sf /usr/local/bin/python3.9 /usr/local/bin/python3

{
  echo "build --config=ci"
  echo "build --announce_rc"
  if [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}"
  fi
} > "$HOME"/.bazelrc
