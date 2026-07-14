#!/bin/bash
# Build an environment for building ray wheels for the manylinux2014 platform.

set -exuo pipefail

BAZELISK_VERSION="v1.26.0"

ARCH="$(uname -m)"

case "$ARCH" in
  x86_64|amd64)
    ARCH="x86_64"
    ;;
  aarch64|arm64)
    ARCH="aarch64"
    ;;
  *)
    echo "Unsupported arch: $ARCH" >&2
    exit 1
    ;;
esac

echo "Architecture is ${ARCH}"

if [[ ! -e /usr/bin/nproc ]]; then
  echo -e '#!/bin/bash\necho 10' > "/usr/bin/nproc"
  chmod +x /usr/bin/nproc
fi

# Install ray cpp dependencies.
sudo yum -y install unzip zip sudo openssl xz
if [[ "${ARCH}" == "x86_64" ]]; then
  sudo yum -y install libasan-4.8.5-44.el7.x86_64 libubsan-7.3.1-5.10.el7.x86_64 \
    devtoolset-8-libasan-devel.x86_64
fi

# Expose a C/C++ compiler on the strict-action PATH for rules_foreign_cc.
# RocksDB (REP-64) is built via rules_foreign_cc's cmake()/ninja bootstrap,
# which invokes a bare `c++`/`cc` resolved through PATH. Ray's .bazelrc sets
# --incompatible_strict_action_env, which pins the build action PATH to
# /bin:/usr/bin:/usr/local/bin and hides the devtoolset compiler under
# /opt/rh. The x86_64 base ships a system compiler at /usr/bin, but the
# aarch64 base does not, so symlink the toolchain compiler into
# /usr/local/bin (which is on the strict action PATH) there.
if [[ "${ARCH}" == "aarch64" ]]; then
  CXX_BIN="$(command -v c++ || command -v g++ || true)"
  CC_BIN="$(command -v cc || command -v gcc || true)"
  if [[ -z "${CXX_BIN}" || -z "${CC_BIN}" ]]; then
    echo "Unable to locate a C/C++ compiler for rules_foreign_cc" >&2
    exit 1
  fi
  # Idempotent symlink: skip if src and dest already resolve to the same path.
  # Re-running otherwise risks `command -v` returning the /usr/local/bin symlink
  # itself, turning `ln -sf X X` into a self-referential loop that breaks the
  # compiler with "Too many levels of symbolic links".
  safe_ln() {
    local src="$1"
    local dest="$2"
    if [[ "$(readlink -f "${src}")" != "$(readlink -f "${dest}" 2>/dev/null)" ]]; then
      sudo ln -sf "${src}" "${dest}"
    fi
  }
  safe_ln "${CXX_BIN}" /usr/local/bin/c++
  safe_ln "${CXX_BIN}" /usr/local/bin/g++
  safe_ln "${CC_BIN}" /usr/local/bin/cc
  safe_ln "${CC_BIN}" /usr/local/bin/gcc
fi

# Install ray java dependencies.
if [[ "${RAYCI_DISABLE_JAVA:-false}" != "true" && "${RAY_INSTALL_JAVA:-1}" == "1" ]]; then
  sudo yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel maven
  java -version
  JAVA_BIN="$(readlink -f "$(command -v java)")"
  echo "java_bin path ${JAVA_BIN}"
  export JAVA_HOME="${JAVA_BIN%jre/bin/java}"
fi

# Install nodejs
NODE_VERSION_FULL="14.21.3"

if [[ "${ARCH}" == "x86_64" ]]; then
  NODE_URL="https://nodejs.org/dist/v${NODE_VERSION_FULL}/node-v${NODE_VERSION_FULL}-linux-x64.tar.xz"
  NODE_SHA256="05c08a107c50572ab39ce9e8663a2a2d696b5d262d5bd6f98d84b997ce932d9a"
else # aarch64
  NODE_URL="https://nodejs.org/dist/v${NODE_VERSION_FULL}/node-v${NODE_VERSION_FULL}-linux-arm64.tar.xz"
  NODE_SHA256="f06642bfcf0b8cc50231624629bec58b183954641b638e38ed6f94cd39e8a6ef"
fi

NODE_DIR="/usr/local/node"
curl -fsSL "${NODE_URL}" -o /tmp/node.tar.xz
echo "$NODE_SHA256  /tmp/node.tar.xz" | sha256sum -c -
sudo mkdir -p "$NODE_DIR"
sudo tar -xf /tmp/node.tar.xz -C "$NODE_DIR" --strip-components=1
rm /tmp/node.tar.xz

# Install bazel
mkdir -p "$HOME"/bin
if [[ "${ARCH}" == "x86_64" ]]; then
  BAZELISK_URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-amd64"
else # aarch64
  BAZELISK_URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-arm64"
fi

curl -sSfL -o /tmp/bazelisk "${BAZELISK_URL}"
chmod +x /tmp/bazelisk
sudo mv /tmp/bazelisk /usr/local/bin/bazelisk
sudo ln -sf /usr/local/bin/bazelisk /usr/local/bin/bazel

# Use python3.10 as default python3
sudo ln -sf /usr/local/bin/python3.10 /usr/local/bin/python3

{
  echo "build --config=ci"
  echo "build --announce_rc"
  if [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}"
  fi
} > "$HOME"/.bazelrc
