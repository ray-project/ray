#!/bin/bash
# build-wheel.sh - Build Ray manylinux wheels locally using wanda
set -euo pipefail

WANDA_SPEC="ci/docker/ray-wheel.wanda.yaml"
SUPPORTED_PY=("3.9" "3.10" "3.11" "3.12" "3.13")
LOCAL_BIN="${LOCAL_BIN:-$HOME/.local/bin}"

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
    # Detect System
    OS_TYPE="$(uname -s | tr '[:upper:]' '[:lower:]')"
    HOST_ARCH="$(uname -m)"
    
    case "$HOST_ARCH" in
        x86_64|amd64)
            WANDA_BINARY_ARCH="amd64"
            HOSTTYPE="amd64"
            ARCH_SUFFIX=""
            ;;
        arm64|aarch64)
            WANDA_BINARY_ARCH="arm64"
            HOSTTYPE="aarch64"
            ARCH_SUFFIX="-aarch64"
            ;;
        *) die "Unsupported architecture: $HOST_ARCH"
            ;;
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

    # Check Binaries
    if ! command -v wanda >/dev/null 2>&1; then
        log_error "'wanda' not found."
        WANDA_URL="https://github.com/ray-project/rayci/releases/latest/download/wanda-${OS_TYPE}-${WANDA_BINARY_ARCH}"
        
        echo -e "\n${YELLOW}Run this to install wanda locally:${NC}"
        echo "------------------------------------------------"
        echo "mkdir -p $LOCAL_BIN"
        echo "curl -fL $WANDA_URL -o $LOCAL_BIN/wanda"
        echo "chmod +x $LOCAL_BIN/wanda"
        echo "------------------------------------------------"
        echo -e "Then ensure $LOCAL_BIN is in your PATH.\n"
        exit 1
    fi
    
    command -v docker >/dev/null 2>&1 || die "Docker is required but not installed."

    [[ -f "$WANDA_SPEC" ]] || die "Spec not found at $WANDA_SPEC. Run from Ray root."
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
    echo "  Python: $PYTHON_VERSION"
    echo "  Arch:   $HOSTTYPE"
    echo "  Commit: $commit"
    echo "  Output: $OUTPUT_DIR"
    echo "------------------------------------------------"

    export PYTHON_VERSION MANYLINUX_VERSION HOSTTYPE ARCH_SUFFIX
    export BUILDKITE_COMMIT="$commit" IS_LOCAL_BUILD="true"

    wanda "$WANDA_SPEC"

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
