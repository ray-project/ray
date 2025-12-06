#!/bin/bash

printInfo() {
    local blue=$'\033[0;34m'
    local reset=$'\033[0m'
    printf '%sINFO:%s %s\n' "$blue" "$reset" "$@"
}

printWarn() {
    local yellow=$'\033[0;33m'
    local reset=$'\033[0m'
    printf '%sWARN:%s %s\n' "$yellow" "$reset" "$@"
}

printError() {
    local red=$'\033[0;31m'
    local reset=$'\033[0m'
    printf '%sERROR:%s %s\n' "$red" "$reset" "$@"
}

printHeader() {
    if [[ -n "${BUILDKITE:-}" ]]; then
        # See: https://buildkite.com/docs/pipelines/configure/managing-log-output#grouping-log-output-collapsed-groups
        printf -- '--- %s\n' "$@"
    else
        local yellow=$'\033[1;33m'
        local bold=$'\033[1m'
        local reset=$'\033[0m'
        local text="$*"
        
        printf '\n%s%s==>%s %s\n' "$yellow" "$bold" "$reset" "$text"
    fi
}

# Normalize architecture to supported architectures.
# Currently supported architectures: x86_64
normalize_arch() {
    local arch="${1:-$(uname -m)}"
    case "${arch}" in
        x86_64|amd64)
            echo "x86_64"
            ;;
        *)
            printError "Unsupported architecture: $arch"
            return 1
            ;;
    esac
}

install_wanda() {
    local wanda_bin_path="${1:?Wanda binary path is required}"
    
    # Check for version file
    REPO_ROOT_DIR=$(git rev-parse --show-toplevel)
    RAYCI_VERSION_FILE="${REPO_ROOT_DIR}/.rayciversion"
    if [[ ! -f "${RAYCI_VERSION_FILE}" ]]; then
        printError ".rayciversion file not found"
        return 1
    fi
    
    local version
    version="$(cat "${RAYCI_VERSION_FILE}")"
    printInfo "Installing wanda binary v${version}"
    
    # Determine platform-specific URL
    local base_url="https://github.com/ray-project/rayci/releases/download/v${version}"
    local platform_suffix
    
    if [[ "$OSTYPE" =~ ^linux ]]; then
        if [[ "$HOSTTYPE" == "arm64" || "$HOSTTYPE" == "aarch64" ]]; then
            platform_suffix="linux-arm64"
        else
            platform_suffix="linux-amd64"
        fi
    elif [[ "$OSTYPE" == "msys" ]]; then
        platform_suffix="windows-amd64"
    else
        printError "Unsupported platform: ${OSTYPE} ${HOSTTYPE}"
        return 1
    fi
    
    local wanda_url="${base_url}/wanda-${platform_suffix}"
    
    printInfo "Downloading from: ${wanda_url}"
    curl -fsSL "${wanda_url}" -o "${wanda_bin_path}"
    
    if [[ ! -f "${wanda_bin_path}" ]]; then
        printError "Failed to download wanda binary"
        return 1
    fi
    
    chmod +x "${wanda_bin_path}"
    
    printInfo "Wanda binary installed to: ${wanda_bin_path}"
}
