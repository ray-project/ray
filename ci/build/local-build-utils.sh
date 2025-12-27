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

# Emit Bazel flags tuned to the container's cgroup CPU + memory limits.
#
# Output example:
#   --jobs=4 --local_cpu_resources=4 --local_ram_resources=6144
#
# Env overrides:
#   BAZEL_DETECT_HEADROOM_PCT   (default: 80)  # percent of mem limit to give Bazel
#   BAZEL_DETECT_MIN_RAM_MB     (default: 2048)
#   BAZEL_DETECT_MAX_JOBS       (default: 0)   # 0 means no cap
#   BAZEL_DETECT_FORCE_JOBS     (default: 0)   # if >0, use exactly this jobs/cpus
#   BAZEL_DETECT_FORCE_RAM_MB   (default: 0)   # if >0, use exactly this RAM
#
# Notes:
# - Works with cgroup v2 and v1.
# - Falls back to host totals if unbounded.
# - Intended for running *inside* the build container, but will also behave sensibly on host.
bazel_container_resource_flags() {
    local headroom_pct="${BAZEL_DETECT_HEADROOM_PCT:-80}"
    local min_ram_mb="${BAZEL_DETECT_MIN_RAM_MB:-2048}"
    local max_jobs="${BAZEL_DETECT_MAX_JOBS:-0}"
    local force_jobs="${BAZEL_DETECT_FORCE_JOBS:-0}"
    local force_ram_mb="${BAZEL_DETECT_FORCE_RAM_MB:-0}"

    # -----------------------
    # Memory detection (MB)
    # -----------------------
    local mem_limit_mb mem_max v bazel_mem_mb
    if [[ -r /sys/fs/cgroup/memory.max ]]; then
        # cgroup v2
        mem_max="$(cat /sys/fs/cgroup/memory.max)"
        if [[ "$mem_max" != "max" ]]; then
            mem_limit_mb=$(( mem_max / 1024 / 1024 ))
        else
            mem_limit_mb=""
        fi
    fi

    if [[ -z "${mem_limit_mb:-}" && -r /sys/fs/cgroup/memory/memory.limit_in_bytes ]]; then
        # cgroup v1
        v="$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)"
        # Treat very large values as "unlimited"
        if (( v > 0 && v < (1<<60) )); then
            mem_limit_mb=$(( v / 1024 / 1024 ))
        fi
    fi

    if [[ -z "${mem_limit_mb:-}" ]]; then
        # fallback to host total: kB -> MB
        mem_limit_mb="$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)"
    fi

    if (( force_ram_mb > 0 )); then
        bazel_mem_mb="$force_ram_mb"
    else
        bazel_mem_mb=$(( mem_limit_mb * headroom_pct / 100 ))
        if (( bazel_mem_mb < min_ram_mb )); then
            bazel_mem_mb="$min_ram_mb"
        fi
    fi

    # -----------------------
    # CPU detection (count)
    # -----------------------
    local cpu_limit quota period bazel_jobs bazel_cpus
    local cpu_full_threshold="${BAZEL_DETECT_CPU_FULL_THRESHOLD:-4}"
    local cpu_headroom_pct="${BAZEL_DETECT_CPU_HEADROOM_PCT:-65}"

    # C++-oriented RAM budget
    local mb_per_job="${BAZEL_DETECT_MB_PER_JOB:-2048}"
    local ram_reserve_mb="${BAZEL_DETECT_RAM_RESERVE_MB:-3072}"  # keep some for links/OS

    cpu_limit=""

    if [[ -r /sys/fs/cgroup/cpu.max ]]; then
        # cgroup v2: "max" or "<quota> <period>"
        read -r quota period < /sys/fs/cgroup/cpu.max
        if [[ "$quota" == "max" ]]; then
            cpu_limit="$(nproc)"
        else
            cpu_limit=$(( (quota + period - 1) / period )) # ceil
        fi
    elif [[ -r /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -r /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
        # cgroup v1
        quota="$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)"
        period="$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)"
        if (( quota > 0 && period > 0 )); then
            cpu_limit=$(( (quota + period - 1) / period )) # ceil
        fi
    fi

    if [[ -z "${cpu_limit:-}" ]]; then
        cpu_limit="$(nproc)"
    fi
    if (( cpu_limit < 1 )); then cpu_limit=1; fi

    if (( force_jobs > 0 )); then
        bazel_jobs="$force_jobs"
        bazel_cpus="$force_jobs"
    else
        bazel_cpus="$cpu_limit"

        # CPU-based "wiggle" jobs
        local jobs_by_cpu
        if (( cpu_limit <= cpu_full_threshold )); then
            jobs_by_cpu="$cpu_limit"
        else
            jobs_by_cpu=$(( cpu_limit * cpu_headroom_pct / 100 ))
            if (( jobs_by_cpu < cpu_full_threshold )); then jobs_by_cpu="$cpu_full_threshold"; fi
            if (( jobs_by_cpu > cpu_limit )); then jobs_by_cpu="$cpu_limit"; fi
        fi

        # RAM-based jobs (with reserve)
        local usable_mb="$bazel_mem_mb"
        if (( usable_mb > ram_reserve_mb )); then
            usable_mb=$(( usable_mb - ram_reserve_mb ))
        else
            usable_mb=0
        fi

        local jobs_by_ram=1
        if (( mb_per_job > 0 )); then
            jobs_by_ram=$(( usable_mb / mb_per_job ))
            if (( jobs_by_ram < 1 )); then jobs_by_ram=1; fi
        fi

        # Final jobs = tighter bound
        bazel_jobs="$jobs_by_cpu"
        if (( jobs_by_ram < bazel_jobs )); then
            bazel_jobs="$jobs_by_ram"
        fi

        # Sane bounds
        if (( bazel_jobs < 1 )); then bazel_jobs=1; fi
        if (( bazel_jobs > cpu_limit )); then bazel_jobs="$cpu_limit"; fi
    fi

    if (( max_jobs > 0 && bazel_jobs > max_jobs )); then
        bazel_jobs="$max_jobs"
    fi
    if (( max_jobs > 0 && bazel_cpus > max_jobs )); then
        bazel_cpus="$max_jobs"
    fi

    echo "--jobs=${bazel_jobs} --local_cpu_resources=${bazel_cpus} --local_ram_resources=${bazel_mem_mb}"
}
