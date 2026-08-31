#!/usr/bin/env bash

# Installs the gVisor runtime (runsc) needed by the Ray sandbox tests.
# Written primarily for CI. Kept on "latest" for now
# Pinning a specific gVisor version is a separate follow-up tracked in issue #65352.

set -euxo pipefail

# Install directory. Defaults to /usr/local/bin (on PATH); overridable via $1.
INSTALL_DIR="${1:-/usr/local/bin}"

install_runsc() {
    case "${OSTYPE}" in linux*)
        if command -v runsc > /dev/null 2>&1; then
            echo "runsc already installed at $(command -v runsc), skipping."
            return 0
        fi

        local arch="x86_64"
        if [ "${HOSTTYPE}" = "aarch64" ]; then
            arch="aarch64"
        fi

        local url="https://storage.googleapis.com/gvisor/releases/release/latest/${arch}/runsc"

        # Quieter output under Buildkite, matching install-llvm-binaries.sh.
        local wget_options=""
        if [ -n "${BUILDKITE-}" ]; then
            wget_options="-nv"
        fi

        echo "Downloading runsc for ${arch} from ${url}"
        wget ${wget_options} -c "${url}" -O runsc

        chmod 0755 runsc
        sudo mkdir -p "${INSTALL_DIR}"
        sudo mv runsc "${INSTALL_DIR}/runsc"

        echo "Installed runsc to ${INSTALL_DIR}/runsc"
        runsc --version
        ;;
    *)
        echo "runsc is only supported on Linux; skipping on ${OSTYPE}." 1>&2
        false
        ;;
    esac
}

install_runsc "$@"
