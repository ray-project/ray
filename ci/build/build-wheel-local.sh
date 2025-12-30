#!/bin/bash
# ============================================================================
# WHEEL BUILD HIERARCHY
# ============================================================================
#
# This script builds distributable Ray wheel files (.whl) using pre-built
# artifacts from wanda images. The C++ compilation happens once in ray-core,
# then gets packaged into wheels.
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                        PRE-BUILT ARTIFACTS                              │
#   │                                                                         │
#   │   manylinux2014 ──┬── ray-core ──────► ray_pkg.zip, ray_py_proto.zip    │
#   │                   ├── ray-dashboard ─► dashboard.tar.gz                 │
#   │                   └── ray-java ──────► ray-java.jar                     │
#   └───────────────────────────┬─────────────────────────────────────────────┘
#                               │
#            ┌──────────────────┴──────────────────┐
#            ▼                                     ▼
#   ┌────────────────────────┐        ┌────────────────────────┐
#   │   RAY WHEEL            │        │   RAY-CPP WHEEL        │
#   │   ray-wheel.wanda.yaml │        │   ray-cpp-wheel.wanda  │
#   │                        │        │                        │
#   │   Combines artifacts   │        │   ray-cpp-core builds  │
#   │   + pip wheel          │        │   C++ headers + libs   │
#   │         │              │        │         │              │
#   │         ▼              │        │         ▼              │
#   │   ray-*.whl            │        │   ray_cpp-*.whl        │
#   └────────────────────────┘        └────────────────────────┘
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/local-build-utils.sh"

usage() {
  cat <<EOF
Usage:
  $0 [PYTHON_VERSION] [TARGET]

TARGET:
  ray (default)  - Build ray wheel
  cpp            - Build ray-cpp wheel
  all            - Build both

Examples:
  $0                      # Build ray wheel for Python 3.10
  $0 3.11                 # Build ray wheel for Python 3.11
  $0 3.10 cpp             # Build ray-cpp wheel
  $0 3.10 all             # Build both wheels

Flags:
  --py <3.x>
  --target <ray|cpp|all>
  --print-config
  --skip-deps
  --skip-extract
  --out-dir <path>
  --only-deps
  --only-build
  -h|--help
EOF
  exit 1
}

validate_target() {
  case "$1" in ray|cpp|all) return 0 ;; *) error "Invalid target '$1'"; exit 1 ;; esac
}

if [[ "${1:-}" == "help" || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then usage; fi

PRINT_CONFIG=0
SKIP_DEPS=0
SKIP_EXTRACT=0
ONLY_DEPS=0
ONLY_BUILD=0

PYTHON_VERSION="3.10"
TARGET="ray"
OUT_DIR=""

positionals=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --py)
      [[ $# -ge 2 ]] || { error "--py requires a value"; usage; }
      PYTHON_VERSION="$(normalize_python_version "$2")"
      shift 2
      ;;
    --target)
      [[ $# -ge 2 ]] || { error "--target requires a value"; usage; }
      TARGET="$2"
      shift 2
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || { error "--out-dir requires a value"; usage; }
      OUT_DIR="$2"
      shift 2
      ;;
    --print-config) PRINT_CONFIG=1; shift ;;
    --skip-deps) SKIP_DEPS=1; shift ;;
    --skip-extract) SKIP_EXTRACT=1; shift ;;
    --only-deps) ONLY_DEPS=1; shift ;;
    --only-build) ONLY_BUILD=1; SKIP_EXTRACT=1; shift ;;
    -h|--help) usage ;;
    *) positionals+=("$1"); shift ;;
  esac
done

parse_strict_py_value_required_for_value positionals TARGET "3.10" "ray" "" usage
validate_target "${TARGET}"

if [[ "${ONLY_DEPS}" -eq 1 && "${ONLY_BUILD}" -eq 1 ]]; then
  error "--only-deps and --only-build are mutually exclusive."
  exit 1
fi

setup_build_env
check_wanda_prerequisites "$WANDA_BIN"

REPO_ROOT="$(get_repo_root)"
cd "$REPO_ROOT"

if [[ -z "${OUT_DIR}" ]]; then
  OUT_DIR="${REPO_ROOT}/.whl"
fi

if [[ -z "${BUILDKITE_COMMIT:-}" ]]; then
  BUILDKITE_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo "unknown")"
fi
export BUILDKITE_COMMIT
export PYTHON_VERSION

print_config_block \
  "Repo root:${REPO_ROOT}" \
  "Python:${PYTHON_VERSION}" \
  "Target:${TARGET}" \
  "Wanda:${WANDA_BIN:-<unset>}" \
  "Commit:${BUILDKITE_COMMIT}" \
  "Out dir:${OUT_DIR}" \
  "Skip deps:${SKIP_DEPS}" \
  "Skip extract:${SKIP_EXTRACT}" \
  "Only deps:${ONLY_DEPS}" \
  "Only build:${ONLY_BUILD}"

if [[ "${PRINT_CONFIG}" -eq 1 ]]; then
  exit 0
fi

build_wheel_deps() {
  if [[ "${SKIP_DEPS}" -eq 1 ]]; then
    header "Skipping wheel deps (ray-core/dashboard/java) due to --skip-deps"
    return 0
  fi

  header "Building ray-core..."
  "$WANDA_BIN" ci/docker/ray-core.wanda.yaml

  header "Building ray-dashboard..."
  "$WANDA_BIN" ci/docker/ray-dashboard.wanda.yaml

  header "Building ray-java..."
  "$WANDA_BIN" ci/docker/ray-java.wanda.yaml

  header "Tagging images for local wanda access..."
  docker tag "cr.ray.io/rayproject/ray-core-py${PYTHON_VERSION}:latest" "ray-core-py${PYTHON_VERSION}:latest"
  docker tag "cr.ray.io/rayproject/ray-java-build:latest" "ray-java-build:latest"
  docker tag "cr.ray.io/rayproject/ray-dashboard:latest" "ray-dashboard:latest"
}

build_ray_wheel() {
  header "Building ray-wheel..."
  "$WANDA_BIN" ci/docker/ray-wheel.wanda.yaml
  docker tag "cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest" "ray-wheel-py${PYTHON_VERSION}:latest"
  header "Ray wheel build complete!"
}

build_cpp_wheel() {
  header "Building ray-cpp-core..."
  "$WANDA_BIN" ci/docker/ray-cpp-core.wanda.yaml
  docker tag "cr.ray.io/rayproject/ray-cpp-core-py${PYTHON_VERSION}:latest" "ray-cpp-core-py${PYTHON_VERSION}:latest"

  header "Building ray-cpp-wheel..."
  "$WANDA_BIN" ci/docker/ray-cpp-wheel.wanda.yaml
  header "Ray C++ wheel build complete!"
}

# Extract *.whl from an image into an output dir.
# Uses docker export + tar wildcards; flattens wheels into out_dir.
extract_wheels_from_image() {
  local image_name="$1"
  local wheel_type="$2"
  local out_dir="$3"

  header "Extracting ${wheel_type} wheel(s) to ${out_dir}..."
  mkdir -p "${out_dir}"

  local container_id
  container_id="$(docker create "${image_name}" true)"

  docker export "${container_id}" | tar -x -C "${out_dir}" --wildcards --no-anchored '*.whl' 2>/dev/null || true
  docker rm "${container_id}" > /dev/null

  # Flatten to top-level out_dir
  find "${out_dir}" -type f -name '*.whl' -print0 | while IFS= read -r -d '' f; do
    if [[ "$(dirname "$f")" != "${out_dir}" ]]; then
      mv -n "$f" "${out_dir}/"
    fi
  done

  # Clean up non-wheel files/dirs
  find "${out_dir}" -type f ! -name '*.whl' -delete 2>/dev/null || true
  find "${out_dir}" -type d -empty -delete 2>/dev/null || true
}

build_wheel_deps

if [[ "${ONLY_DEPS}" -eq 1 ]]; then
  header "Deps build complete!"
  exit 0
fi

case "$TARGET" in
  ray) build_ray_wheel ;;
  cpp) build_cpp_wheel ;;
  all)
    build_ray_wheel
    build_cpp_wheel
    ;;
esac

if [[ "${SKIP_EXTRACT}" -eq 0 ]]; then
  if [[ "$TARGET" == "ray" || "$TARGET" == "all" ]]; then
    extract_wheels_from_image "cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest" "ray" "${OUT_DIR}"
  fi
  if [[ "$TARGET" == "cpp" || "$TARGET" == "all" ]]; then
    extract_wheels_from_image "cr.ray.io/rayproject/ray-cpp-wheel-py${PYTHON_VERSION}:latest" "ray-cpp" "${OUT_DIR}"
  fi

  header "Wheels extracted to ${OUT_DIR}:"
  ls -la "${OUT_DIR}"/*.whl 2>/dev/null || echo "  (no wheel files found)"
else
  header "Skipping wheel extraction due to --skip-extract/--only-build"
fi
