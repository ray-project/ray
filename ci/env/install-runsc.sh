#!/usr/bin/env bash

# Installs the gVisor runtime (runsc) needed by the Ray sandbox tests.
# Written primarily for CI. Kept on "latest" for now
# Pinning a specific gVisor version is a separate follow-up tracked in issue #65352.

set -euxo pipefail

# Install directory. Defaults to /usr/local/bin (on PATH); overridable via $1.
INSTALL_DIR="${1:-/usr/local/bin}"

install_runsc() {
    case "${OSTYPE}" in
    linux*)
        if command -v runsc > /dev/null 2>&1; then
            echo "runsc already installed at $(command -v runsc), skipping."
            return 0
        fi

        # Detect the CPU architecture via uname; normalize both ARM names.
        local arch
        case "$(uname -m)" in
            aarch64 | arm64)
                arch="aarch64"
                ;;
            x86_64)
                arch="x86_64"
                ;;
            *)
                echo "Unsupported architecture: $(uname -m)" 1>&2
                return 1
                ;;
        esac

        local url="https://storage.googleapis.com/gvisor/releases/release/latest/${arch}/gvisor.tar.bz2"

        # Quieter output under Buildkite, matching install-llvm-binaries.sh.
        local wget_options=""
        if [ -n "${BUILDKITE-}" ]; then
            wget_options="-nv"
        fi

        # Download the tarball to a temp file so the large archive never lands
        # in INSTALL_DIR (e.g. /usr/local/bin). Extract only runsc into INSTALL_DIR.
        local tmp_tarball
        tmp_tarball="$(mktemp)"

        echo "Downloading runsc for ${arch} from ${url}"
        wget ${wget_options} -c "${url}" -O "${tmp_tarball}"

        # Only use sudo if the target dir isn't writable.
        local sudo_cmd=""
        if [ -d "${INSTALL_DIR}" ] && [ ! -w "${INSTALL_DIR}" ]; then
            sudo_cmd="sudo"
        elif ! mkdir -p "${INSTALL_DIR}" 2>/dev/null; then
            sudo_cmd="sudo"
            ${sudo_cmd} mkdir -p "${INSTALL_DIR}"
        fi

        # Extract just the runsc binary directly into INSTALL_DIR.
        ${sudo_cmd} tar -xjf "${tmp_tarball}" -C "${INSTALL_DIR}" runsc
        ${sudo_cmd} chmod 0755 "${INSTALL_DIR}/runsc"

        rm -f "${tmp_tarball}"

        echo "Installed runsc to ${INSTALL_DIR}/runsc"
        "${INSTALL_DIR}/runsc" --version
        ;;
    *)
        echo "runsc is only supported on Linux; skipping on ${OSTYPE}." 1>&2
        false
        ;;
    esac
}

install_runsc "$@"
