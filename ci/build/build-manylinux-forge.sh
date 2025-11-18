#!/bin/bash
# Build an environment for building ray wheels for the manylinux2014 platform.

set -exuo pipefail

if [[ ! -e /usr/bin/nproc ]]; then
  echo -e '#!/bin/bash\necho 10' > "/usr/bin/nproc"
  chmod +x /usr/bin/nproc
fi

# Install ray cpp dependencies.
yum -y install unzip zip sudo openssl xz
if [[ "${HOSTTYPE-}" == "x86_64" ]]; then
  yum -y install libasan-4.8.5-44.el7.x86_64 libubsan-7.3.1-5.10.el7.x86_64 \
    devtoolset-8-libasan-devel.x86_64
fi

# Install ray java dependencies.
if [[ "${RAY_INSTALL_JAVA}" == "1" ]]; then
  yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel maven
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
npm install -g @bazel/bazelisk
ln -sf "$(which bazelisk)" /usr/local/bin/bazel

{
  echo "build --config=ci"
  echo "build --announce_rc"
  if [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}"
  fi
} > ~/.bazelrc
