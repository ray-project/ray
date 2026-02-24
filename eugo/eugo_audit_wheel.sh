#!/usr/bin/env bash
# =============================================================================
# eugo_audit_wheel.sh — Compare Eugo Ray wheel against upstream PyPI wheel
#
# Checks performed:
#   1. File tree diff (what's in each wheel)
#   2. Exported symbols (nm on _raylet.so, gcs_server, raylet)
#   3. Linked libraries (otool -L / ldd)
#   4. Binary sizes
#
# Usage:
#   ./eugo_audit_wheel.sh [path/to/eugo_wheel.whl]
#
# If no wheel path is given, it will look for the most recent .whl in the
# current directory matching 'ray-*.whl'.
#
# Prerequisites:
#   - pip (for downloading upstream wheel)
#   - nm, otool (macOS) or readelf, ldd (Linux)
#   - unzip
#   - diff, sort, awk, column (standard Unix)
# =============================================================================
set -euo pipefail

# --- Configuration ---
UPSTREAM_VERSION="3.0.0.dev0"  # Ray nightly version to compare against
AUDIT_DIR="${TMPDIR:-/tmp}/eugo_wheel_audit_$$"
REPORT_FILE="eugo_wheel_audit_report.txt"

# Colors (disabled if not terminal)
if [[ -t 1 ]]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; CYAN=''; BOLD=''; NC=''
fi

# --- Helper Functions ---
info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
header(){ echo -e "\n${BOLD}━━━ $* ━━━${NC}"; }

detect_platform() {
    case "$(uname -s)" in
        Darwin) PLATFORM="macos" ;;
        Linux)  PLATFORM="linux" ;;
        *)      err "Unsupported platform: $(uname -s)"; exit 1 ;;
    esac
    ARCH="$(uname -m)"
    info "Platform: ${PLATFORM} ${ARCH}"
}

# Map platform to pip platform tag for downloading
pip_platform_tag() {
    case "${PLATFORM}-${ARCH}" in
        macos-arm64)    echo "macosx_11_0_arm64" ;;
        macos-x86_64)   echo "macosx_10_15_x86_64" ;;
        linux-x86_64)   echo "manylinux2014_x86_64" ;;
        linux-aarch64)  echo "manylinux2014_aarch64" ;;
        *)              err "Unknown platform-arch: ${PLATFORM}-${ARCH}"; exit 1 ;;
    esac
}

# Returns the Python version tag (e.g., cp312) for the running interpreter
python_cp_tag() {
    python3 -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')"
}

cleanup() {
    if [[ -d "${AUDIT_DIR}" ]]; then
        info "Cleaning up ${AUDIT_DIR}"
        rm -rf "${AUDIT_DIR}"
    fi
}
trap cleanup EXIT

# --- Phase 0: Setup ---
detect_platform

EUGO_WHEEL="${1:-}"
if [[ -z "${EUGO_WHEEL}" ]]; then
    # Auto-detect most recent ray wheel in current directory
    EUGO_WHEEL="$(ls -t ray-*.whl 2>/dev/null | head -1 || true)"
    if [[ -z "${EUGO_WHEEL}" ]]; then
        err "No Eugo wheel specified and none found in $(pwd)"
        echo "Usage: $0 [path/to/eugo_wheel.whl]"
        echo ""
        echo "Build one first with: ./eugo_pip3_wheel.sh"
        exit 1
    fi
fi

if [[ ! -f "${EUGO_WHEEL}" ]]; then
    err "Wheel file not found: ${EUGO_WHEEL}"
    exit 1
fi

info "Eugo wheel: ${EUGO_WHEEL}"

mkdir -p "${AUDIT_DIR}"/{eugo,upstream}

# --- Phase 1: Download Upstream Wheel ---
header "Phase 1: Downloading upstream Ray wheel from PyPI"

PYTAG="$(python_cp_tag)"
PLATTAG="$(pip_platform_tag)"

info "Looking for ray==${UPSTREAM_VERSION} (${PYTAG}, ${PLATTAG})"

# Try to download the upstream nightly wheel
UPSTREAM_WHEEL=""
if pip download "ray==${UPSTREAM_VERSION}" \
    --no-deps \
    --python-version "${PYTAG#cp}" \
    --platform "${PLATTAG}" \
    --only-binary=:all: \
    -d "${AUDIT_DIR}/download" \
    2>"${AUDIT_DIR}/pip_download.log"; then
    UPSTREAM_WHEEL="$(ls "${AUDIT_DIR}/download"/ray-*.whl 2>/dev/null | head -1)"
fi

if [[ -z "${UPSTREAM_WHEEL}" || ! -f "${UPSTREAM_WHEEL}" ]]; then
    warn "Could not download upstream wheel for ${PYTAG}/${PLATTAG}."
    warn "Trying without platform constraint (any available)..."

    pip download "ray==${UPSTREAM_VERSION}" \
        --no-deps \
        --only-binary=:all: \
        -d "${AUDIT_DIR}/download" \
        2>>"${AUDIT_DIR}/pip_download.log" || true

    UPSTREAM_WHEEL="$(ls "${AUDIT_DIR}/download"/ray-*.whl 2>/dev/null | head -1 || true)"
fi

if [[ -z "${UPSTREAM_WHEEL}" || ! -f "${UPSTREAM_WHEEL}" ]]; then
    err "Could not download upstream wheel. Check ${AUDIT_DIR}/pip_download.log"
    err "You can also manually place an upstream wheel at ${AUDIT_DIR}/download/ray-*.whl and re-run."
    exit 1
fi

info "Upstream wheel: $(basename "${UPSTREAM_WHEEL}")"

# --- Phase 2: Unpack Both Wheels ---
header "Phase 2: Unpacking wheels"

unzip -q "${EUGO_WHEEL}" -d "${AUDIT_DIR}/eugo"
unzip -q "${UPSTREAM_WHEEL}" -d "${AUDIT_DIR}/upstream"

ok "Eugo wheel unpacked to ${AUDIT_DIR}/eugo"
ok "Upstream wheel unpacked to ${AUDIT_DIR}/upstream"

# --- Phase 3: File Tree Comparison ---
header "Phase 3: File Tree Comparison"

# Generate sorted file lists (relative to wheel root, exclude .dist-info)
(cd "${AUDIT_DIR}/eugo" && find . -type f | grep -v '\.dist-info' | sort) > "${AUDIT_DIR}/eugo_files.txt"
(cd "${AUDIT_DIR}/upstream" && find . -type f | grep -v '\.dist-info' | sort) > "${AUDIT_DIR}/upstream_files.txt"

EUGO_COUNT=$(wc -l < "${AUDIT_DIR}/eugo_files.txt" | tr -d ' ')
UPSTREAM_COUNT=$(wc -l < "${AUDIT_DIR}/upstream_files.txt" | tr -d ' ')

info "Eugo file count: ${EUGO_COUNT}"
info "Upstream file count: ${UPSTREAM_COUNT}"

# Diff
diff "${AUDIT_DIR}/upstream_files.txt" "${AUDIT_DIR}/eugo_files.txt" > "${AUDIT_DIR}/file_tree_diff.txt" 2>&1 || true

ONLY_UPSTREAM=$(grep '^< ' "${AUDIT_DIR}/file_tree_diff.txt" | wc -l | tr -d ' ')
ONLY_EUGO=$(grep '^> ' "${AUDIT_DIR}/file_tree_diff.txt" | wc -l | tr -d ' ')

if [[ "${ONLY_UPSTREAM}" -eq 0 && "${ONLY_EUGO}" -eq 0 ]]; then
    ok "File trees match exactly!"
else
    warn "File tree differences found: ${ONLY_UPSTREAM} only in upstream, ${ONLY_EUGO} only in Eugo"

    if [[ "${ONLY_UPSTREAM}" -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Files ONLY in upstream wheel (missing from Eugo):${NC}"
        grep '^< ' "${AUDIT_DIR}/file_tree_diff.txt" | sed 's/^< /  /' | head -50
        if [[ "${ONLY_UPSTREAM}" -gt 50 ]]; then echo "  ... and $((ONLY_UPSTREAM - 50)) more"; fi
    fi

    if [[ "${ONLY_EUGO}" -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Files ONLY in Eugo wheel (extra):${NC}"
        grep '^> ' "${AUDIT_DIR}/file_tree_diff.txt" | sed 's/^> /  /' | head -50
        if [[ "${ONLY_EUGO}" -gt 50 ]]; then echo "  ... and $((ONLY_EUGO - 50)) more"; fi
    fi
fi

# --- Known intentional differences ---
echo ""
info "Checking known intentional exclusions from Eugo wheel..."

KNOWN_UPSTREAM_ONLY=(
    "ray/core/libjemalloc.so"
    "ray/thirdparty_files/"
    "ray/_private/runtime_env/agent/thirdparty_files/"
)

for pattern in "${KNOWN_UPSTREAM_ONLY[@]}"; do
    matches=$(grep "^< .*${pattern}" "${AUDIT_DIR}/file_tree_diff.txt" | wc -l | tr -d ' ')
    if [[ "${matches}" -gt 0 ]]; then
        ok "Expected exclusion: ${pattern} (${matches} files) — handled via pip deps"
    fi
done

# --- Phase 4: Binary Inventory ---
header "Phase 4: Binary Inventory & Sizes"

# Binaries we expect in both wheels
EXPECTED_BINARIES=(
    "ray/_raylet.cpython-*.so:_raylet extension"
    "ray/core/src/ray/gcs/gcs_server:GCS server"
    "ray/core/src/ray/raylet/raylet:Raylet"
)

# Also check for any we might use .so suffix
if [[ "${PLATFORM}" == "macos" ]]; then
    RAYLET_EXT_GLOB="ray/_raylet.cpython-*.so"
else
    RAYLET_EXT_GLOB="ray/_raylet.cpython-*.so"
fi

printf "\n%-60s %12s %12s %12s\n" "Binary" "Upstream" "Eugo" "Delta"
printf "%-60s %12s %12s %12s\n" "$(printf '%0.s─' {1..60})" "$(printf '%0.s─' {1..12})" "$(printf '%0.s─' {1..12})" "$(printf '%0.s─' {1..12})"

find_binary() {
    local dir="$1" glob="$2"
    # Use find to locate the binary, handle globs
    find "${dir}" -path "*/${glob}" -o -name "$(basename "${glob}")" 2>/dev/null | head -1
}

compare_binary_size() {
    local name="$1" upstream_path="$2" eugo_path="$3"

    local up_size="-" eu_size="-" delta="-"

    if [[ -n "${upstream_path}" && -f "${upstream_path}" ]]; then
        up_size=$(stat -f%z "${upstream_path}" 2>/dev/null || stat --format=%s "${upstream_path}" 2>/dev/null || echo "?")
    fi
    if [[ -n "${eugo_path}" && -f "${eugo_path}" ]]; then
        eu_size=$(stat -f%z "${eugo_path}" 2>/dev/null || stat --format=%s "${eugo_path}" 2>/dev/null || echo "?")
    fi

    if [[ "${up_size}" != "-" && "${up_size}" != "?" && "${eu_size}" != "-" && "${eu_size}" != "?" ]]; then
        delta=$(( eu_size - up_size ))
        local pct
        if [[ "${up_size}" -ne 0 ]]; then
            pct=$(awk "BEGIN { printf \"%.1f\", (${delta}/${up_size})*100 }")
            delta="${delta} (${pct}%)"
        fi
    fi

    # Format sizes in MB if > 1MB
    format_size() {
        local s="$1"
        if [[ "${s}" == "-" || "${s}" == "?" ]]; then echo "${s}"; return; fi
        if [[ "${s}" -gt 1048576 ]]; then
            awk "BEGIN { printf \"%.1f MB\", ${s}/1048576 }"
        elif [[ "${s}" -gt 1024 ]]; then
            awk "BEGIN { printf \"%.1f KB\", ${s}/1024 }"
        else
            echo "${s} B"
        fi
    }

    printf "%-60s %12s %12s %12s\n" "${name}" "$(format_size "${up_size}")" "$(format_size "${eu_size}")" "${delta}"
}

# Find and compare key binaries
declare -A EUGO_BINS UPSTREAM_BINS

# _raylet extension
UPSTREAM_BINS[_raylet]="$(find "${AUDIT_DIR}/upstream" -name '_raylet*.so' 2>/dev/null | head -1)"
EUGO_BINS[_raylet]="$(find "${AUDIT_DIR}/eugo" -name '_raylet*.so' 2>/dev/null | head -1)"

# gcs_server
UPSTREAM_BINS[gcs_server]="$(find "${AUDIT_DIR}/upstream" -name 'gcs_server' -type f 2>/dev/null | head -1)"
EUGO_BINS[gcs_server]="$(find "${AUDIT_DIR}/eugo" -name 'gcs_server' -type f 2>/dev/null | head -1)"

# raylet
UPSTREAM_BINS[raylet]="$(find "${AUDIT_DIR}/upstream" -name 'raylet' -type f 2>/dev/null | head -1)"
EUGO_BINS[raylet]="$(find "${AUDIT_DIR}/eugo" -name 'raylet' -type f 2>/dev/null | head -1)"

for bin in _raylet gcs_server raylet; do
    up="${UPSTREAM_BINS[${bin}]:-}"
    eu="${EUGO_BINS[${bin}]:-}"

    if [[ -z "${up}" ]]; then
        warn "${bin}: NOT FOUND in upstream wheel"
    fi
    if [[ -z "${eu}" ]]; then
        err "${bin}: NOT FOUND in Eugo wheel"
    fi

    compare_binary_size "${bin}" "${up}" "${eu}"
done

# Also show total wheel sizes
echo ""
compare_binary_size "TOTAL WHEEL" "${UPSTREAM_WHEEL}" "${EUGO_WHEEL}"

# --- Phase 5: Linked Libraries ---
header "Phase 5: Linked Libraries"

linked_libs() {
    local binary="$1"
    if [[ ! -f "${binary}" ]]; then echo "(not found)"; return; fi

    if [[ "${PLATFORM}" == "macos" ]]; then
        otool -L "${binary}" 2>/dev/null | tail -n +2 | awk '{print $1}' | sort
    else
        # Linux: ldd or readelf
        if command -v ldd &>/dev/null; then
            ldd "${binary}" 2>/dev/null | awk '{print $1}' | sort
        else
            readelf -d "${binary}" 2>/dev/null | grep NEEDED | awk -F'[][]' '{print $2}' | sort
        fi
    fi
}

for bin in _raylet gcs_server raylet; do
    up="${UPSTREAM_BINS[${bin}]:-}"
    eu="${EUGO_BINS[${bin}]:-}"

    echo ""
    echo -e "${BOLD}${bin}:${NC}"

    if [[ -n "${up}" && -f "${up}" ]]; then
        linked_libs "${up}" > "${AUDIT_DIR}/upstream_${bin}_libs.txt"
    else
        echo "(upstream not found)" > "${AUDIT_DIR}/upstream_${bin}_libs.txt"
    fi

    if [[ -n "${eu}" && -f "${eu}" ]]; then
        linked_libs "${eu}" > "${AUDIT_DIR}/eugo_${bin}_libs.txt"
    else
        echo "(eugo not found)" > "${AUDIT_DIR}/eugo_${bin}_libs.txt"
    fi

    # Show diff
    if diff -u "${AUDIT_DIR}/upstream_${bin}_libs.txt" "${AUDIT_DIR}/eugo_${bin}_libs.txt" > "${AUDIT_DIR}/${bin}_libs_diff.txt" 2>&1; then
        ok "${bin}: linked libraries match"
    else
        warn "${bin}: linked library differences:"
        cat "${AUDIT_DIR}/${bin}_libs_diff.txt" | head -40
    fi
done

# --- Phase 6: Exported Symbols ---
header "Phase 6: Exported Symbols (key binaries)"

exported_symbols() {
    local binary="$1"
    if [[ ! -f "${binary}" ]]; then echo "(not found)"; return; fi

    if [[ "${PLATFORM}" == "macos" ]]; then
        nm -gU "${binary}" 2>/dev/null | awk '{print $NF}' | sort
    else
        nm -D --defined-only "${binary}" 2>/dev/null | awk '{print $NF}' | sort
    fi
}

for bin in _raylet gcs_server raylet; do
    up="${UPSTREAM_BINS[${bin}]:-}"
    eu="${EUGO_BINS[${bin}]:-}"

    echo ""
    echo -e "${BOLD}${bin}:${NC}"

    if [[ -n "${up}" && -f "${up}" ]]; then
        exported_symbols "${up}" > "${AUDIT_DIR}/upstream_${bin}_syms.txt"
    else
        echo "(upstream not found)" > "${AUDIT_DIR}/upstream_${bin}_syms.txt"
    fi

    if [[ -n "${eu}" && -f "${eu}" ]]; then
        exported_symbols "${eu}" > "${AUDIT_DIR}/eugo_${bin}_syms.txt"
    else
        echo "(eugo not found)" > "${AUDIT_DIR}/eugo_${bin}_syms.txt"
    fi

    UP_SYM_COUNT=$(wc -l < "${AUDIT_DIR}/upstream_${bin}_syms.txt" | tr -d ' ')
    EU_SYM_COUNT=$(wc -l < "${AUDIT_DIR}/eugo_${bin}_syms.txt" | tr -d ' ')

    info "Symbol counts — upstream: ${UP_SYM_COUNT}, eugo: ${EU_SYM_COUNT}"

    # For _raylet, do a detailed diff since it's the Python-facing interface
    if [[ "${bin}" == "_raylet" ]]; then
        # Find symbols only in upstream (potentially missing from our build)
        comm -23 "${AUDIT_DIR}/upstream_${bin}_syms.txt" "${AUDIT_DIR}/eugo_${bin}_syms.txt" > "${AUDIT_DIR}/${bin}_missing_syms.txt"
        comm -13 "${AUDIT_DIR}/upstream_${bin}_syms.txt" "${AUDIT_DIR}/eugo_${bin}_syms.txt" > "${AUDIT_DIR}/${bin}_extra_syms.txt"

        MISSING_COUNT=$(wc -l < "${AUDIT_DIR}/${bin}_missing_syms.txt" | tr -d ' ')
        EXTRA_COUNT=$(wc -l < "${AUDIT_DIR}/${bin}_extra_syms.txt" | tr -d ' ')

        if [[ "${MISSING_COUNT}" -eq 0 && "${EXTRA_COUNT}" -eq 0 ]]; then
            ok "${bin}: exported symbols match exactly"
        else
            if [[ "${MISSING_COUNT}" -gt 0 ]]; then
                warn "${bin}: ${MISSING_COUNT} symbols in upstream but MISSING from Eugo:"
                head -20 "${AUDIT_DIR}/${bin}_missing_syms.txt" | sed 's/^/  /'
                if [[ "${MISSING_COUNT}" -gt 20 ]]; then echo "  ... and $((MISSING_COUNT - 20)) more"; fi
            fi
            if [[ "${EXTRA_COUNT}" -gt 0 ]]; then
                info "${bin}: ${EXTRA_COUNT} symbols in Eugo but not in upstream (likely OK):"
                head -10 "${AUDIT_DIR}/${bin}_extra_syms.txt" | sed 's/^/  /'
                if [[ "${EXTRA_COUNT}" -gt 10 ]]; then echo "  ... and $((EXTRA_COUNT - 10)) more"; fi
            fi
        fi
    else
        # For executables, just report count difference
        if [[ "${UP_SYM_COUNT}" == "${EU_SYM_COUNT}" ]]; then
            ok "${bin}: same number of exported symbols"
        else
            warn "${bin}: symbol count differs (upstream=${UP_SYM_COUNT}, eugo=${EU_SYM_COUNT})"
        fi
    fi
done

# --- Phase 7: Python Module Inventory ---
header "Phase 7: Python Module Inventory"

# Check that key Python packages are present in both
echo ""
echo "Checking key Python modules..."

KEY_MODULES=(
    "ray/__init__.py"
    "ray/_raylet.pyx"
    "ray/actor.py"
    "ray/autoscaler/_private/commands.py"
    "ray/dashboard/dashboard.py"
    "ray/serve/__init__.py"
    "ray/tune/__init__.py"
    "ray/data/__init__.py"
    "ray/train/__init__.py"
    "ray/workflow/__init__.py"
)

printf "%-60s %10s %10s\n" "Module" "Upstream" "Eugo"
printf "%-60s %10s %10s\n" "$(printf '%0.s─' {1..60})" "$(printf '%0.s─' {1..10})" "$(printf '%0.s─' {1..10})"

for mod in "${KEY_MODULES[@]}"; do
    up_present="❌"
    eu_present="❌"
    [[ -f "${AUDIT_DIR}/upstream/${mod}" ]] && up_present="✅"
    [[ -f "${AUDIT_DIR}/eugo/${mod}" ]] && eu_present="✅"
    printf "%-60s %10s %10s\n" "${mod}" "${up_present}" "${eu_present}"
done

# --- Phase 8: Proto/Generated File Comparison ---
header "Phase 8: Generated Files (proto, Cython)"

echo ""
echo "Proto-generated Python files:"

(cd "${AUDIT_DIR}/upstream" && find . -name '*_pb2.py' -o -name '*_pb2_grpc.py' | sort) > "${AUDIT_DIR}/upstream_proto_py.txt"
(cd "${AUDIT_DIR}/eugo" && find . -name '*_pb2.py' -o -name '*_pb2_grpc.py' | sort) > "${AUDIT_DIR}/eugo_proto_py.txt"

UP_PROTO=$(wc -l < "${AUDIT_DIR}/upstream_proto_py.txt" | tr -d ' ')
EU_PROTO=$(wc -l < "${AUDIT_DIR}/eugo_proto_py.txt" | tr -d ' ')

info "Upstream proto Python files: ${UP_PROTO}"
info "Eugo proto Python files: ${EU_PROTO}"

if diff "${AUDIT_DIR}/upstream_proto_py.txt" "${AUDIT_DIR}/eugo_proto_py.txt" > "${AUDIT_DIR}/proto_py_diff.txt" 2>&1; then
    ok "Proto Python file sets match"
else
    warn "Proto Python file differences:"
    MISSING_PROTO=$(grep '^< ' "${AUDIT_DIR}/proto_py_diff.txt" | wc -l | tr -d ' ')
    EXTRA_PROTO=$(grep '^> ' "${AUDIT_DIR}/proto_py_diff.txt" | wc -l | tr -d ' ')
    if [[ "${MISSING_PROTO}" -gt 0 ]]; then
        echo -e "${YELLOW}  Missing from Eugo:${NC}"
        grep '^< ' "${AUDIT_DIR}/proto_py_diff.txt" | sed 's/^< /    /'
    fi
    if [[ "${EXTRA_PROTO}" -gt 0 ]]; then
        echo -e "${YELLOW}  Extra in Eugo:${NC}"
        grep '^> ' "${AUDIT_DIR}/proto_py_diff.txt" | sed 's/^> /    /'
    fi
fi

# --- Summary ---
header "AUDIT SUMMARY"

echo ""
echo -e "${BOLD}Artifacts compared:${NC}"
echo "  Eugo wheel:     $(basename "${EUGO_WHEEL}")"
echo "  Upstream wheel:  $(basename "${UPSTREAM_WHEEL}")"
echo ""
echo -e "${BOLD}Results:${NC}"
echo "  File tree:       ${ONLY_UPSTREAM:-0} missing, ${ONLY_EUGO:-0} extra"

# Count issues
TOTAL_ISSUES=0
for bin in _raylet gcs_server raylet; do
    eu="${EUGO_BINS[${bin}]:-}"
    if [[ -z "${eu}" || ! -f "${eu:-/dev/null}" ]]; then
        echo -e "  ${bin}:           ${RED}NOT FOUND${NC}"
        TOTAL_ISSUES=$((TOTAL_ISSUES + 1))
    fi
done

echo ""
if [[ "${TOTAL_ISSUES}" -eq 0 && "${ONLY_UPSTREAM}" -le 50 ]]; then
    ok "Audit complete. Check detailed output above for warnings."
else
    warn "Audit complete with ${TOTAL_ISSUES} critical issue(s). Review output above."
fi

echo ""
echo "Detailed diff files saved to: ${AUDIT_DIR}/"
echo "  file_tree_diff.txt     — full file tree diff"
echo "  *_libs_diff.txt        — linked library diffs per binary"
echo "  *_missing_syms.txt     — symbols missing from Eugo _raylet"
echo "  *_extra_syms.txt       — symbols extra in Eugo _raylet"
echo "  proto_py_diff.txt      — proto Python file diff"
echo ""
echo "Re-run with AUDIT_DIR=${AUDIT_DIR} to inspect intermediate files."

# Optionally save report
if [[ -n "${REPORT_FILE:-}" ]]; then
    info "Note: To save full output, re-run with: $0 ${EUGO_WHEEL} 2>&1 | tee ${REPORT_FILE}"
fi
