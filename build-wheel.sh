#!/bin/bash
# build-wheel.sh - Build Ray manylinux wheels locally using raymake
#
# This script uses 'raymake', Ray's container image builder, to orchestrate
# Docker builds with caching. Raymake is run via 'uvx' to ensure the correct
# version is used (the wheel is cached after first download).
set -euo pipefail

RAYMAKE_SPEC="ci/docker/ray-wheel.wanda.yaml"
RAYMAKE_VERSION="0.28.0"
RAYMAKE_BASE_URL="https://github.com/ray-project/rayci/releases/download/v${RAYMAKE_VERSION}"
SUPPORTED_PY=("3.9" "3.10" "3.11" "3.12" "3.13")

# Colors
if [[ -t 1 ]]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BLUE='\033[0;34m'; NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
fi

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()       { log_error "$@"; exit 1; }

show_help() {
    cat <<EOF
Usage: $0 <PYTHON_VERSION> [OUTPUT_DIR]

Arguments:
  PYTHON_VERSION   Supported: ${SUPPORTED_PY[*]}
  OUTPUT_DIR       Default: .whl/
EOF
}

setup_env() {
    HOST_ARCH="$(uname -m)"
    HOST_OS="$(uname -s)"

    case "$HOST_OS" in
        Darwin)
            case "$HOST_ARCH" in
                arm64)
                    HOSTTYPE="aarch64"
                    ARCH_SUFFIX="-aarch64"
                    RAYMAKE_WHEEL="${RAYMAKE_BASE_URL}/raymake-${RAYMAKE_VERSION}-py3-none-macosx_12_0_arm64.whl"
                    ;;
                *) die "macOS $HOST_ARCH is not supported. Use macOS ARM (arm64) or Linux." ;;
            esac
            ;;
        Linux)
            case "$HOST_ARCH" in
                x86_64|amd64)
                    HOSTTYPE="x86_64"
                    ARCH_SUFFIX=""
                    RAYMAKE_WHEEL="${RAYMAKE_BASE_URL}/raymake-${RAYMAKE_VERSION}-py3-none-manylinux_2_17_x86_64.whl"
                    ;;
                aarch64|arm64)
                    HOSTTYPE="aarch64"
                    ARCH_SUFFIX="-aarch64"
                    RAYMAKE_WHEEL="${RAYMAKE_BASE_URL}/raymake-${RAYMAKE_VERSION}-py3-none-manylinux_2_17_aarch64.whl"
                    ;;
                *) die "Unsupported Linux architecture: $HOST_ARCH" ;;
            esac
            ;;
        *) die "Unsupported OS: $HOST_OS" ;;
    esac

    # Source MANYLINUX_VERSION from rayci.env
    MANYLINUX_VERSION=$(grep '^MANYLINUX_VERSION=' rayci.env | cut -d'=' -f2 | tr -d '"' | tr -d "'")
    if [[ -z "$MANYLINUX_VERSION" ]]; then
        die "MANYLINUX_VERSION is not set in rayci.env"
    fi
}

validate_args() {
    [[ $# -eq 0 || "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { show_help; exit 0; }

    PYTHON_VERSION="$1"
    OUTPUT_DIR="${2:-.whl}"

    # Check Python version support
    local valid=false
    for v in "${SUPPORTED_PY[@]}"; do [[ "$v" == "$PYTHON_VERSION" ]] && valid=true; done
    [[ "$valid" == "true" ]] || die "Unsupported Python version: $PYTHON_VERSION. Use: ${SUPPORTED_PY[*]}"

    # Check dependencies
    if ! command -v uv >/dev/null 2>&1; then
        log_error "'uv' not found. See:"
        echo "  https://docs.astral.sh/uv/getting-started/installation/"
        exit 1
    fi

    command -v docker >/dev/null 2>&1 || die "Docker is required but not installed."
    docker info >/dev/null 2>&1 || die "Docker is not running. Please start Docker and try again."

    [[ -f "$RAYMAKE_SPEC" ]] || die "Spec not found at $RAYMAKE_SPEC. Run from Ray root."

}

extract_wheel() {
    local image="$1"
    mkdir -p "$OUTPUT_DIR"

    log_info "Extracting wheel from $image..." >&2

    # Create container with dummy command (image is FROM scratch, has no shell)
    local container_id
    container_id=$(docker create "$image" /_noop_)

    local whl_files
    whl_files=$(docker export "$container_id" | tar -tf - 2>/dev/null | grep '\.whl$' || true)

    if [[ -z "$whl_files" ]]; then
        docker rm "$container_id" >/dev/null
        die "No wheel files found in image root."
    fi

    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)

    for whl in $whl_files; do
        local dest="$OUTPUT_DIR/$whl"
        # Archive existing wheel with timestamp suffix if it would conflict
        if [[ -f "$dest" ]]; then
            mv "$dest" "${dest}.${timestamp}"
            log_info "Archived existing: ${whl}.${timestamp}" >&2
        fi
        docker cp "$container_id:/$whl" "$dest"
    done

    docker rm "$container_id" >/dev/null

    # Return extracted wheel paths (newline-separated)
    for whl in $whl_files; do
        echo "$OUTPUT_DIR/$whl"
    done
}

main() {
    setup_env
    validate_args "$@"

    local commit
    commit=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

    log_info "Build Configuration:"
    echo "  Python:  $PYTHON_VERSION"
    echo "  Arch:    $HOSTTYPE"
    echo "  Commit:  $commit"
    echo "  Output:  $OUTPUT_DIR"
    echo "  Raymake: $RAYMAKE_WHEEL (via uvx)"
    echo "------------------------------------------------"

    export PYTHON_VERSION MANYLINUX_VERSION HOSTTYPE ARCH_SUFFIX
    export BUILDKITE_COMMIT="$commit" IS_LOCAL_BUILD="true"

    uvx --from "$RAYMAKE_WHEEL" raymake "$RAYMAKE_SPEC"

    local image="cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}${ARCH_SUFFIX}"
    local wheel_paths
    wheel_paths=$(extract_wheel "$image")

    log_ok "Success! Built wheel(s):"
    echo "$wheel_paths" | while read -r whl; do
        echo "  $whl"
    done
    echo ""
    echo "Install with:"
    echo "$wheel_paths" | while read -r whl; do
        echo "  pip install $whl"
    done
}

main "$@"
