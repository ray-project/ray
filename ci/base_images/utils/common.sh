#!/bin/bash


[[ -n "${_CI_BASE_IMAGES_UTILS_COMMON_SH_SOURCED:-}" ]] && return 0
_CI_BASE_IMAGES_UTILS_COMMON_SH_SOURCED=1

printInfo() {
    local blue=$'\033[0;34m'
    local reset=$'\033[0m'
    printf '%sINFO:%s %s\n' "$blue" "$reset" "$@"
}

printWarn() {
    local yellow=$'\033[0;33m'
    local reset=$'\033[0m'
    printf '%sWARN:%s %s\n' "$yellow" "$reset" "$@" >&2
}

printError() {
    local red=$'\033[0;31m'
    local reset=$'\033[0m'
    printf '%sERROR:%s %s\n' "$red" "$reset" "$@" >&2
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
