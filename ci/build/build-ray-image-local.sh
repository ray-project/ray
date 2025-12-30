#!/bin/bash
# ============================================================================
# IMAGE BUILD HIERARCHY
# ============================================================================
#
# This script builds publishable Ray docker images (ray:nightly-py3.10-cpu, etc).
# Unlike test containers, these require a wheel build to create distributable images.
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                     PRE-BUILT ARTIFACTS                                 │
#   │                                                                         │
#   │   manylinux2014                                                         │
#   │        │                                                                │
#   │        ├──────────────────┬──────────────────┐                          │
#   │        ▼                  ▼                  ▼                          │
#   │   ray-core           ray-dashboard       ray-java                       │
#   │   (C++ via bazel)    (npm build)         (Java JARs)                    │
#   │        │                  │                  │                          │
#   │        │  ray_pkg.zip     │  dashboard.tar   │  ray-java.jar            │
#   │        └──────────────────┴──────────────────┘                          │
#   │                           │                                             │
#   │                           ▼                                             │
#   │                      ray-wheel                                          │
#   │                   (pip wheel build)                                     │
#   │                           │                                             │
#   │                    ray-{ver}.whl                                        │
#   └───────────────────────────┼─────────────────────────────────────────────┘
#                               │
#                               ▼
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                      BASE IMAGES                                        │
#   │                                                                         │
#   │   CPU Path:                        CUDA Path:                           │
#   │                                                                         │
#   │   ubuntu:22.04                     nvidia/cuda:{ver}-ubuntu22.04        │
#   │        │                                  │                             │
#   │        ▼                                  ▼                             │
#   │   ray-py{ver}-cpu-base             ray-py{ver}-cu{cuda}-base            │
#   │   (docker/base-deps/cpu)           (docker/base-deps/cuda)              │
#   │        │                                  │                             │
#   └────────┼──────────────────────────────────┼─────────────────────────────┘
#            │                                  │
#            ▼                                  ▼
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                    FINAL RAY IMAGES                                     │
#   │                                                                         │
#   │   ray:nightly-py{ver}-cpu          ray:nightly-py{ver}-cu{cuda}         │
#   │   (ci/docker/ray-image-cpu)        (ci/docker/ray-image-cuda)           │
#   │                                                                         │
#   │   Contents:                                                             │
#   │   • Ray wheel installed                                                 │
#   │   • Default Ray dependencies                                            │
#   │   • Ready for: docker run rayproject/ray:nightly-py3.10-cpu             │
#   └─────────────────────────────────────────────────────────────────────────┘
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/local-build-utils.sh"

DEFAULT_CUDA_PLATFORM="cu12.1.1-cudnn8"

usage() {
  cat <<EOF
Usage:
  $0 [PYTHON_VERSION] [PLATFORM]

PLATFORM:
  cpu (default)
  cu<CUDA_VERSION>  (e.g. cu12.1.1-cudnn8, cu11.8.0-cudnn8)

Examples:
  $0                          # Build CPU image for Python 3.10
  $0 3.11                     # Build CPU image for Python 3.11
  $0 3.10 cpu                 # Build CPU image
  $0 3.10 ${DEFAULT_CUDA_PLATFORM}   # Build CUDA image

Flags:
  --py <3.x>                 Set Python version
  --platform <cpu|cu...>     Set platform
  --print-config             Print resolved config and exit
  --skip-wheel               Skip building the Ray wheel (assume it exists)
  --skip-deps                Skip building ray-core/dashboard/java
  --only-wheel               Only build the Ray wheel
  --only-base                Only build the base image
  --only-image               Only build the final image (assumes wheel + base exist)
  --default-cuda             Print default CUDA platform and exit
  -h|--help                  Show help
EOF
  exit 1
}

normalize_platform() {
  local p="$1"
  if [[ "$p" == "cpu" ]]; then echo "cpu"; return 0; fi
  if [[ "$p" =~ ^cu.+$ ]]; then echo "$p"; return 0; fi
  if [[ "$p" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?-cudnn[0-9]+$ ]]; then echo "cu${p}"; return 0; fi
  echo "$p"
}

validate_platform() {
  local p="$1"
  if [[ "$p" == "cpu" || "$p" =~ ^cu.+$ ]]; then return 0; fi
  error "Invalid platform '$p'. Expected 'cpu' or 'cu<cuda_version>' (e.g. ${DEFAULT_CUDA_PLATFORM})."
  exit 1
}

if [[ "${1:-}" == "--default-cuda" ]]; then echo "${DEFAULT_CUDA_PLATFORM}"; exit 0; fi
if [[ "${1:-}" == "help" || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then usage; fi

PRINT_CONFIG=0
SKIP_WHEEL=0
SKIP_DEPS=0
ONLY_WHEEL=0
ONLY_BASE=0
ONLY_IMAGE=0

PYTHON_VERSION="3.10"
PLATFORM="cpu"
CUDA_VERSION=""

positionals=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --py)
      [[ $# -ge 2 ]] || { error "--py requires a value"; usage; }
      PYTHON_VERSION="$(normalize_python_version "$2")"
      shift 2
      ;;
    --platform)
      [[ $# -ge 2 ]] || { error "--platform requires a value"; usage; }
      PLATFORM="$(normalize_platform "$2")"
      shift 2
      ;;
    --print-config) PRINT_CONFIG=1; shift ;;
    --skip-wheel) SKIP_WHEEL=1; shift ;;
    --skip-deps) SKIP_DEPS=1; shift ;;
    --only-wheel) ONLY_WHEEL=1; shift ;;
    --only-base) ONLY_BASE=1; shift ;;
    --only-image) ONLY_IMAGE=1; shift ;;
    --default-cuda) echo "${DEFAULT_CUDA_PLATFORM}"; exit 0 ;;
    -h|--help) usage ;;
    *) positionals+=("$1"); shift ;;
  esac
done

parse_strict_py_value_required_for_value positionals PLATFORM "3.10" "cpu" normalize_platform usage
validate_platform "${PLATFORM}"
[[ "${PLATFORM}" =~ ^cu.+$ ]] && CUDA_VERSION="${PLATFORM#cu}"

only_count=$((ONLY_WHEEL + ONLY_BASE + ONLY_IMAGE))
if [[ "$only_count" -gt 1 ]]; then
  error "--only-wheel, --only-base, and --only-image are mutually exclusive."
  exit 1
fi

setup_build_env
export PYTHON_VERSION
check_wanda_prerequisites "$WANDA_BIN"

print_config_block \
  "Python:${PYTHON_VERSION}" \
  "Platform:${PLATFORM}" \
  "CUDA version:${CUDA_VERSION:-}" \
  "Wanda:${WANDA_BIN:-<unset>}" \
  "Skip wheel:${SKIP_WHEEL}" \
  "Skip deps:${SKIP_DEPS}" \
  "Only wheel:${ONLY_WHEEL}" \
  "Only base:${ONLY_BASE}" \
  "Only image:${ONLY_IMAGE}"

[[ "${PRINT_CONFIG}" -eq 1 ]] && exit 0

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
  if [[ "${SKIP_WHEEL}" -eq 1 ]]; then
    header "Skipping ray-wheel build due to --skip-wheel"
    return 0
  fi

  build_wheel_deps

  header "Building ray-wheel..."
  "$WANDA_BIN" ci/docker/ray-wheel.wanda.yaml
  docker tag "cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest" "ray-wheel-py${PYTHON_VERSION}:latest"
}

build_cpu_base() {
  export REQUIREMENTS_FILE="ray_base_deps_py${PYTHON_VERSION}.lock"
  header "Building CPU base image..."
  "$WANDA_BIN" docker/base-deps/cpu.wanda.yaml
  local BASE_IMAGE="ray-py${PYTHON_VERSION}-cpu-base"
  docker tag "cr.ray.io/rayproject/${BASE_IMAGE}:latest" "${BASE_IMAGE}:latest"
}

build_cuda_base() {
  local cuda_version="$1"
  export CUDA_VERSION="${cuda_version}"
  export REQUIREMENTS_FILE="ray_base_deps_py${PYTHON_VERSION}.lock"
  header "Building CUDA ${cuda_version} base image..."
  "$WANDA_BIN" docker/base-deps/cuda.wanda.yaml
  local BASE_IMAGE="ray-py${PYTHON_VERSION}-cu${cuda_version}-base"
  docker tag "cr.ray.io/rayproject/${BASE_IMAGE}:latest" "${BASE_IMAGE}:latest"
}

build_image_cpu() {
  if [[ "${ONLY_IMAGE}" -ne 1 ]]; then
    build_ray_wheel
    build_cpu_base
  fi

  header "Building ray CPU image..."
  "$WANDA_BIN" ci/docker/ray-image-cpu.wanda.yaml

  local REMOTE="cr.ray.io/rayproject/ray:nightly-py${PYTHON_VERSION}-cpu"
  local LOCAL="ray:nightly-py${PYTHON_VERSION}-cpu"
  docker tag "${REMOTE}" "${LOCAL}"

  header "Ray CPU image build complete!"
  echo "  Remote: ${REMOTE}"
  echo "  Local:  ${LOCAL}"
}

build_image_cuda() {
  local cuda_version="$1"

  if [[ "${ONLY_IMAGE}" -ne 1 ]]; then
    build_ray_wheel
    build_cuda_base "$cuda_version"
  fi

  export CUDA_VERSION="${cuda_version}"
  header "Building ray CUDA ${cuda_version} image..."
  "$WANDA_BIN" ci/docker/ray-image-cuda.wanda.yaml

  local REMOTE="cr.ray.io/rayproject/ray:nightly-py${PYTHON_VERSION}-cu${cuda_version}"
  local LOCAL="ray:nightly-py${PYTHON_VERSION}-cu${cuda_version}"
  docker tag "${REMOTE}" "${LOCAL}"

  header "Ray CUDA image build complete!"
  echo "  Remote: ${REMOTE}"
  echo "  Local:  ${LOCAL}"
}

if [[ "${ONLY_WHEEL}" -eq 1 ]]; then
  build_ray_wheel
  header "Wheel build complete!"
  exit 0
fi

if [[ "${ONLY_BASE}" -eq 1 ]]; then
  if [[ "${PLATFORM}" == "cpu" ]]; then
    build_cpu_base
    header "CPU base image build complete!"
  else
    build_cuda_base "${CUDA_VERSION}"
    header "CUDA base image build complete!"
  fi
  exit 0
fi

if [[ "${PLATFORM}" == "cpu" ]]; then
  build_image_cpu
else
  build_image_cuda "${CUDA_VERSION}"
fi
