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

# Get the repository root directory
get_repo_root() {
    git rev-parse --show-toplevel 2>/dev/null || {
        error "Not in a git repository. Please run from within the Ray repo."
        exit 1
    }
}

# Normalize architecture name (arm64 -> aarch64)
normalize_arch() {
    local arch="${1:-$(uname -m)}"
    if [[ "$arch" == "arm64" ]]; then
        echo "aarch64"
    else
        echo "$arch"
    fi
}

# Default environment variables for local builds
setup_build_env() {
    export PYTHON_VERSION="${PYTHON_VERSION:-3.10}"
    export ARCH_SUFFIX="${ARCH_SUFFIX:-}"
    export HOSTTYPE="$(normalize_arch "${HOSTTYPE:-$(uname -m)}")"
    export MANYLINUX_VERSION="${MANYLINUX_VERSION:-251216.3835fc5}"
    export WANDA_BIN="${WANDA_BIN:-$(command -v wanda || echo /home/ubuntu/rayci/bin/wanda)}"
}

# Emit Bazel flags for local scheduling and C++ compile concurrency.
#
# Environment knobs
#   BAZEL_DETECT_MEM_HEADROOM_PCT     (default: 90)  percent of HOST_RAM for Bazel memory
#   BAZEL_DETECT_CPU_HEADROOM_PCT     (default: 80)  percent of HOST_CPUS for Bazel CPU
#   BAZEL_DETECT_MB_PER_CPP           (default: 1024) estimated MB per concurrent C++ compile
#   BAZEL_DETECT_CPP_RESERVE_MB       (default: 3072) MB reserved for non-compile work (OS/link/etc.)
#
# Decision: rely on HOST_RAM / HOST_CPUS
# We intentionally do not read cgroup limits (/sys/fs/cgroup/*) by default.
# In our environments, Bazel's HOST_* values reflect the effective resource boundary
# (e.g., Docker Desktop/BuildKit VM or a dedicated CI runner), and HOST_* expressions
# are well-supported via --local_resources.
#
# When to revisit this decision
# If builds run under strict cgroup limits (Kubernetes, docker --memory, etc.) where
# actions can allocate far less than HOST_RAM, reintroduce cgroup/proc detection and
# switch memory to a numeric MB value derived from the actual limit.
bazel_container_resource_flags() {
    local mem_headroom="${BAZEL_DETECT_MEM_HEADROOM:-0.90}"
    local cpu_headroom="${BAZEL_DETECT_CPU_HEADROOM:-0.90}"

    # --- compute integer cpu + ram for legacy bazel flags ---
    local cpu_total
    cpu_total="$(nproc 2>/dev/null || echo 1)"

    # Prefer MemAvailable; fallback to MemTotal
    local avail_mb
    avail_mb="$(awk '
        $1=="MemAvailable:" {print int($2/1024); found=1; exit}
        $1=="MemTotal:" {total=int($2/1024)}
        END {if (!found) print total}
    ' /proc/meminfo)"

    # Apply headroom multipliers -> integers
    local bazel_cpus bazel_ram_mb
    bazel_cpus="$(awk -v c="$cpu_total" -v h="$cpu_headroom" 'BEGIN{v=int(c*h); if(v<1)v=1; print v}')"
    bazel_ram_mb="$(awk -v m="$avail_mb" -v h="$mem_headroom" 'BEGIN{v=int(m*h); if(v<256)v=256; print v}')"

    # --- derive cpp compile cap from available memory budget ---
    local mb_per_cpp="${BAZEL_DETECT_MB_PER_CPP:-3072}"
    local reserve_mb="${BAZEL_DETECT_CPP_RESERVE_MB:-1024}"

    local budget_mb=$(( bazel_ram_mb - reserve_mb ))
    (( budget_mb < 0 )) && budget_mb=0

    local cpp_compile_max=1
    if (( mb_per_cpp > 0 )); then
        cpp_compile_max=$(( budget_mb / mb_per_cpp ))
        (( cpp_compile_max < 1 )) && cpp_compile_max=1
    fi

    local extra_flags=""
    # Limit C++ compile concurrency to prevent OOMs
    extra_flags+=" --local_extra_resources=cppmem=${cpp_compile_max}"
    extra_flags+=" --modify_execution_info=CppCompile=+resources:cppmem:1"

    echo "--local_cpu_resources=${bazel_cpus} --local_ram_resources=${bazel_ram_mb}${extra_flags}"
}
