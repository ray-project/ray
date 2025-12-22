#!/bin/bash
#
# Shared utilities for local Ray build scripts.
# Source this file in other scripts: source "$(dirname "$0")/local-build-utils.sh"
#

# Colors for output
RED='\033[31;1m'
BLUE='\033[34;1m'
GREEN='\033[32;1m'
YELLOW='\033[33;1m'
RESET='\033[0m'

# Print a blue header
header() {
    echo -e "\n${BLUE}===> $1${RESET}"
}

# Print a green success message
success() {
    echo -e "${GREEN}$1${RESET}"
}

# Print a yellow warning
warn() {
    echo -e "${YELLOW}Warning: $1${RESET}" >&2
}

# Print a red error message
error() {
    echo -e "${RED}Error: $1${RESET}" >&2
}

# Check if Docker is running
check_docker() {
    if ! docker info &>/dev/null; then
        error "Docker is not running or not accessible."
        echo "  Please start Docker and try again." >&2
        return 1
    fi
    return 0
}

# Check if wanda binary exists
# Usage: check_wanda "$WANDA_BIN"
check_wanda() {
    local wanda_bin="${1:-$WANDA_BIN}"
    if [[ ! -x "$wanda_bin" ]]; then
        error "Wanda binary not found at: $wanda_bin"
        echo "  Set WANDA_BIN environment variable to the correct path:" >&2
        echo "    export WANDA_BIN=/path/to/wanda" >&2
        echo "  Or install wanda from the rayci repository." >&2
        return 1
    fi
    return 0
}

# Check all prerequisites for wanda-based builds
# Usage: check_wanda_prerequisites "$WANDA_BIN"
check_wanda_prerequisites() {
    local wanda_bin="${1:-$WANDA_BIN}"
    local has_error=0

    check_docker || has_error=1
    check_wanda "$wanda_bin" || has_error=1

    if [[ $has_error -eq 1 ]]; then
        exit 1
    fi
}

# Check only Docker prerequisite (for scripts that don't need wanda)
check_docker_prerequisites() {
    if ! check_docker; then
        exit 1
    fi
}

# Default environment variables for local builds
setup_build_env() {
    export PYTHON_VERSION="${PYTHON_VERSION:-3.10}"
    export ARCH_SUFFIX="${ARCH_SUFFIX:-}"
    export HOSTTYPE="${HOSTTYPE:-x86_64}"
    export MANYLINUX_VERSION="${MANYLINUX_VERSION:-251216.3835fc5}"
    export WANDA_BIN="${WANDA_BIN:-/home/ubuntu/rayci/bin/wanda}"
}
