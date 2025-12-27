#!/usr/bin/env bash
# GLIBC version checker for wheel files
#
# This script provides functions to verify that shared objects (.so files)
# inside Python wheels don't require a GLIBC version higher than allowed.
#
# Usage:
#   source ci/build/glibc-check.sh
#   check_wheel_glibc "path/to/wheel.whl" "2.17"
#
# Or run directly:
#   ./ci/build/glibc-check.sh path/to/wheel.whl [max_glibc_version]

set -euo pipefail

# Check if a command exists
require_cmd() {
    command -v "$1" >/dev/null || { echo "ERROR: $1 not found"; return 1; }
}

# Extract the maximum GLIBC version referenced in a shared object file
# Returns empty string if no GLIBC references found
max_glibc_in_file() {
    local so_path="$1"
    objdump -p "$so_path" 2>/dev/null \
        | grep -oE 'GLIBC_[0-9]+\.[0-9]+' \
        | sed 's/^GLIBC_//' \
        | sort -Vu \
        | tail -n 1
}

# Check all .so files in a wheel for GLIBC version compliance
# Args:
#   $1 - path to wheel file
#   $2 - maximum allowed GLIBC version (default: 2.17)
# Returns:
#   0 if all .so files comply
#   1 if any .so file requires GLIBC > allowed
check_wheel_glibc() {
    local whl="$1"
    local allowed="${2:-2.17}"

    if [[ ! -f "$whl" ]]; then
        echo "ERROR: Wheel file not found: $whl"
        return 1
    fi

    local tmp
    tmp="$(mktemp -d)"

    # Ensure cleanup happens on exit
    _glibc_check_cleanup() {
        rm -rf "$tmp"
    }

    unzip -q "$whl" -d "$tmp"

    local max_seen=""
    local so_count=0
    local offending=()

    # Find all shared objects in the wheel
    while IFS= read -r -d '' so; do
        so_count=$((so_count + 1))
        local m
        m="$(max_glibc_in_file "$so" || true)"
        if [[ -n "$m" ]]; then
            if [[ -z "$max_seen" ]] || [[ "$(printf '%s\n' "$max_seen" "$m" | sort -V | tail -n 1)" == "$m" ]]; then
                max_seen="$m"
            fi
            # If m > allowed => fail
            if [[ "$(printf '%s\n' "$allowed" "$m" | sort -V | tail -n 1)" != "$allowed" ]]; then
                offending+=("$so (GLIBC $m)")
            fi
        fi
    done < <(find "$tmp" -type f \( -name '*.so' -o -name '*.so.*' \) -print0)

    if (( so_count == 0 )); then
        echo "WARNING: No .so files found inside wheel: $whl"
        _glibc_check_cleanup
        return 0
    fi

    if (( ${#offending[@]} > 0 )); then
        echo "ERROR: Wheel $whl contains shared objects requiring GLIBC > $allowed"
        printf '  %s\n' "${offending[@]}"
        _glibc_check_cleanup
        return 1
    fi

    if [[ -z "$max_seen" ]]; then
        echo "WARNING: No GLIBC version references found in any .so inside $whl (unexpected?)"
    else
        echo "GLIBC check passed for $whl: max referenced GLIBC is $max_seen (allowed <= $allowed)"
    fi

    _glibc_check_cleanup
}

# Check all wheels in a directory
# Args:
#   $1 - directory containing wheels
#   $2 - maximum allowed GLIBC version (default: 2.17)
check_wheels_shared_libs_glibc() {
    local dir="$1"
    local allowed="${2:-2.17}"

    require_cmd objdump || return 1
    require_cmd unzip || return 1

    shopt -s nullglob
    local wheels=("$dir"/*.whl)

    if (( ${#wheels[@]} == 0 )); then
        echo "ERROR: No wheels found in $dir"
        return 1
    fi

    local failed=0
    for whl in "${wheels[@]}"; do
        if ! check_wheel_glibc "$whl" "$allowed"; then
            failed=1
        fi
    done

    return $failed
}

# Run directly if not sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ $# -lt 1 ]]; then
        echo "Usage: $0 <wheel_file_or_directory> [max_glibc_version]"
        echo ""
        echo "Examples:"
        echo "  $0 dist/ray-2.0.0-cp39-cp39-linux_x86_64.whl"
        echo "  $0 dist/ray-2.0.0-cp39-cp39-linux_x86_64.whl 2.28"
        echo "  $0 .whl/"
        exit 1
    fi

    require_cmd objdump
    require_cmd unzip

    target="$1"
    allowed="${2:-2.17}"

    if [[ -d "$target" ]]; then
        check_wheels_shared_libs_glibc "$target" "$allowed"
    else
        check_wheel_glibc "$target" "$allowed"
    fi
fi
