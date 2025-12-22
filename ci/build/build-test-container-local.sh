#!/bin/bash
#
# Build Ray test containers locally using wanda.
#
# This script builds the test container images needed to run CI tests locally.
# It uses the wanda-based flow which avoids rebuilding from source by using
# pre-built C++ artifacts from ray-core and ray-dashboard images.
#
# ============================================================================
# HOW THE WANDA-BASED FLOW WORKS (vs traditional wheel build)
# ============================================================================
#
# Traditional flow (BuilderContainer.run()):
#   1. Runs build-manylinux-ray.sh (builds dashboard via npm)
#   2. Runs build-manylinux-wheel.sh (runs `pip wheel` - compiles C++ from scratch)
#   3. Produces a .whl file
#   4. tests.env.Dockerfile rebuilds Ray again via `pip install -e python/`
#   Result: C++ compiled TWICE, ~30-40 minutes
#
# Wanda-based flow (this script / --use-wanda-prebuilt flag):
#   1. ray-core image: bazel builds C++ once, packages into ray_pkg.zip
#   2. ray-dashboard image: npm builds dashboard, packages into dashboard.tar.gz
#   3. tests.env.Dockerfile extracts these artifacts (no compilation):
#        unzip /opt/ray-core/ray_pkg.zip -d python/
#        tar -xzf /opt/ray-dashboard/dashboard.tar.gz
#        SKIP_BAZEL_BUILD=1 pip install -e python/
#   Result: C++ compiled ONCE, reused across all test containers
#
# ============================================================================
# KEY FILES
# ============================================================================
#
# Wanda definitions:
#   ci/docker/ray-core.wanda.yaml      - Builds C++ via bazel, outputs ray_pkg.zip
#   ci/docker/ray-dashboard.wanda.yaml - Builds dashboard via npm, outputs dashboard.tar.gz
#   ci/docker/core.build.wanda.yaml    - Team-specific test dependencies
#
# Installation logic:
#   ci/ray_ci/tests.env.Dockerfile     - Extracts pre-built artifacts when available:
#     if [[ -e /opt/ray-core/ray_pkg.zip && "$BUILD_TYPE" == "optimized" ]]; then
#         unzip /opt/ray-core/ray_pkg.zip -d python/
#         SKIP_BAZEL_BUILD=1 pip install -e python/
#     fi
#
# CI integration:
#   ci/ray_ci/tester.py                - Use --use-wanda-prebuilt flag to skip
#                                        BuilderContainer.run() and use pre-built images
#   ci/ray_ci/linux_container.py       - install_ray() passes RAY_CORE_IMAGE and
#                                        RAY_DASHBOARD_IMAGE to tests.env.Dockerfile
#
# ============================================================================
# USAGE
# ============================================================================
#
# From repo root:
#   ci/build/build-test-container-local.sh                    # Build corebuild-py3.10
#   ci/build/build-test-container-local.sh core 3.10          # Build corebuild-py3.10
#   ci/build/build-test-container-local.sh data 3.10          # Build databuild-py3.10
#   ci/build/build-test-container-local.sh --base-only 3.10   # Build only base images
#
# After building, run tests with:
#   ci/build/run-tests-local.sh //python/ray/tests/test_basic.py core
#
# In CI pipelines, use the --use-wanda-prebuilt flag with test_in_docker:
#   bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... core \
#       --build-type=wheel --use-wanda-prebuilt
#
set -euo pipefail

# Source shared utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/local-build-utils.sh"

usage() {
    echo "Usage: $0 [OPTIONS] [TEAM] [PYTHON_VERSION]"
    echo ""
    echo "Teams:"
    echo "  core (default)   - Core Ray tests"
    echo "  data             - Ray Data tests"
    echo "  serve            - Ray Serve tests"
    echo "  ml               - Ray ML tests"
    echo "  rllib            - RLlib tests"
    echo ""
    echo "Options:"
    echo "  --base-only        - Only build base images (oss-ci-base_test, oss-ci-base_build)"
    echo "  --skip-base        - Skip building base images (assume they exist)"
    echo "  --skip-ray-core    - Skip building ray-core/ray-dashboard (assume they exist)"
    echo "  --skip-ray-install - Skip final Ray installation step (just build base images)"
    echo "  --gpu              - Build GPU variant"
    echo ""
    echo "Examples:"
    echo "  $0                        # Build corebuild-py3.10 + ray-core + ray-dashboard"
    echo "  $0 core 3.12              # Build corebuild-py3.12"
    echo "  $0 --skip-ray-core core   # Build only test container (reuse ray-core)"
    exit 1
}

# Defaults
TEAM="core"
PYTHON_VERSION="3.10"
BASE_ONLY=false
SKIP_BASE=false
SKIP_RAY_CORE=false
SKIP_RAY_INSTALL=false
GPU=false

# Setup environment (sets WANDA_BIN if not set)
setup_build_env

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --base-only)
            BASE_ONLY=true
            shift
            ;;
        --skip-base)
            SKIP_BASE=true
            shift
            ;;
        --skip-ray-core)
            SKIP_RAY_CORE=true
            shift
            ;;
        --skip-ray-install)
            SKIP_RAY_INSTALL=true
            shift
            ;;
        --gpu)
            GPU=true
            shift
            ;;
        -h|--help|help)
            usage
            ;;
        core|data|serve|ml|rllib|doc|llm)
            TEAM="$1"
            shift
            ;;
        3.9|3.10|3.11|3.12|3.13)
            PYTHON_VERSION="$1"
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            usage
            ;;
    esac
done

# Map team to wanda file
get_team_wanda_file() {
    local team="$1"
    case "$team" in
        core)  echo "ci/docker/core.build.wanda.yaml" ;;
        data)  echo "ci/docker/data.build.wanda.yaml" ;;
        serve) echo "ci/docker/serve.build.wanda.yaml" ;;
        ml)    echo "ci/docker/ml.build.wanda.yaml" ;;
        rllib) echo "ci/docker/rllib.build.wanda.yaml" ;;
        doc)   echo "ci/docker/doc.build.wanda.yaml" ;;
        llm)   echo "ci/docker/llm.build.wanda.yaml" ;;
        *)     echo ""; return 1 ;;
    esac
}

# Get build image name
get_build_image_name() {
    local team="$1"
    local python="$2"
    local gpu="$3"

    if [[ "$gpu" == "true" ]]; then
        echo "${team}gpubuild-py${python}"
    else
        echo "${team}build-py${python}"
    fi
}

# Determine if team needs ML base image
# Returns: "ml" if team needs oss-ci-base_ml, "build" otherwise
get_team_base_type() {
    local team="$1"
    case "$team" in
        # These teams require oss-ci-base_ml (has ML/DL dependencies)
        data|ml|rllib)  echo "ml" ;;
        # These teams use oss-ci-base_build directly
        core|serve|doc|llm|*)  echo "build" ;;
    esac
}

# Export environment variables for wanda
export PYTHON="$PYTHON_VERSION"
export PYTHON_VERSION="$PYTHON_VERSION"
export BUILDKITE_BAZEL_CACHE_URL="${BUILDKITE_BAZEL_CACHE_URL:-}"
export HOSTTYPE="${HOSTTYPE:-x86_64}"
export ARCH_SUFFIX=""
export MANYLINUX_VERSION="${MANYLINUX_VERSION:-251216.3835fc5}"

# Check prerequisites
check_wanda_prerequisites "$WANDA_BIN"

echo "Building test environment for team: $TEAM, Python: $PYTHON_VERSION"
if [[ "$GPU" == "true" ]]; then
    echo "    GPU build: enabled"
fi

#
# Step 1: Build base test images
#
if [[ "$SKIP_BASE" != "true" ]]; then
    header "Building oss-ci-base_test-py${PYTHON_VERSION}..."
    $WANDA_BIN ci/docker/base.test.wanda.yaml

    # Tag for local use
    docker tag "cr.ray.io/rayproject/oss-ci-base_test-py${PYTHON_VERSION}:latest" \
               "oss-ci-base_test-py${PYTHON_VERSION}:latest"

    header "Building oss-ci-base_build-py${PYTHON_VERSION}..."
    $WANDA_BIN ci/docker/base.build.wanda.yaml

    docker tag "cr.ray.io/rayproject/oss-ci-base_build-py${PYTHON_VERSION}:latest" \
               "oss-ci-base_build-py${PYTHON_VERSION}:latest"

    if [[ "$GPU" == "true" ]]; then
        header "Building oss-ci-base_gpu-py${PYTHON_VERSION}..."
        $WANDA_BIN ci/docker/base.gpu.wanda.yaml

        docker tag "cr.ray.io/rayproject/oss-ci-base_gpu-py${PYTHON_VERSION}:latest" \
                   "oss-ci-base_gpu-py${PYTHON_VERSION}:latest"
    fi
fi

if [[ "$BASE_ONLY" == "true" ]]; then
    header "Base images built successfully!"
    echo "    oss-ci-base_test-py${PYTHON_VERSION}"
    echo "    oss-ci-base_build-py${PYTHON_VERSION}"
    exit 0
fi

#
# Step 1b: Build ML base image if needed
#
# Teams: data, ml, rllib require oss-ci-base_ml which has PyTorch, TensorFlow, etc.
#
TEAM_BASE_TYPE=$(get_team_base_type "$TEAM")
if [[ "$TEAM_BASE_TYPE" == "ml" && "$SKIP_BASE" != "true" ]]; then
    header "Building oss-ci-base_ml-py${PYTHON_VERSION} (required for $TEAM team)..."
    $WANDA_BIN ci/docker/base.ml.wanda.yaml

    docker tag "cr.ray.io/rayproject/oss-ci-base_ml-py${PYTHON_VERSION}:latest" \
               "oss-ci-base_ml-py${PYTHON_VERSION}:latest"
fi

#
# Step 2: Build ray-core and ray-dashboard (pre-built C++ artifacts)
#
# These images contain the compiled artifacts that will be extracted in Step 4:
#   - ray-core: ray_pkg.zip (C++ binaries from bazel), ray_py_proto.zip (protobuf)
#   - ray-dashboard: dashboard.tar.gz (npm build output)
#
# In CI, these are built once and cached in ECR with tag:
#   {RAYCI_WORK_REPO}:{RAYCI_BUILD_ID}-ray-core-py{version}
#
if [[ "$SKIP_RAY_CORE" != "true" ]]; then
    header "Building ray-core-py${PYTHON_VERSION}..."
    $WANDA_BIN ci/docker/ray-core.wanda.yaml

    docker tag "cr.ray.io/rayproject/ray-core-py${PYTHON_VERSION}:latest" \
               "ray-core-py${PYTHON_VERSION}:latest"

    header "Building ray-dashboard..."
    $WANDA_BIN ci/docker/ray-dashboard.wanda.yaml

    docker tag "cr.ray.io/rayproject/ray-dashboard:latest" \
               "ray-dashboard:latest"
fi

#
# Step 3: Build team-specific test container
#
TEAM_WANDA_FILE=$(get_team_wanda_file "$TEAM")
if [[ -z "$TEAM_WANDA_FILE" ]]; then
    echo "Error: Unknown team: $TEAM"
    exit 1
fi

BUILD_IMAGE_NAME=$(get_build_image_name "$TEAM" "$PYTHON_VERSION" "$GPU")

# Set IMAGE_FROM based on team requirements and GPU flag
TEAM_BASE_TYPE=$(get_team_base_type "$TEAM")
if [[ "$GPU" == "true" ]]; then
    if [[ "$TEAM_BASE_TYPE" == "ml" ]]; then
        export IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_ml_gpu-py${PYTHON_VERSION}"
    else
        export IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_gpu-py${PYTHON_VERSION}"
    fi
    export RAYCI_IS_GPU_BUILD="true"
else
    if [[ "$TEAM_BASE_TYPE" == "ml" ]]; then
        export IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_ml-py${PYTHON_VERSION}"
    else
        export IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_build-py${PYTHON_VERSION}"
    fi
    export RAYCI_IS_GPU_BUILD=""
fi
export IMAGE_TO="$BUILD_IMAGE_NAME"

header "Building ${BUILD_IMAGE_NAME}..."
$WANDA_BIN "$TEAM_WANDA_FILE"

# Tag for local use
docker tag "cr.ray.io/rayproject/${BUILD_IMAGE_NAME}:latest" \
           "${BUILD_IMAGE_NAME}:latest"

#
# Step 4: Build final test image with Ray installed (using tests.env.wanda.yaml)
#
# This step installs Ray using pre-built artifacts from ray-core and ray-dashboard,
# avoiding the need to rebuild Ray from source (saves ~20-30 minutes).
#
# The tests.env.Dockerfile uses Docker multi-stage builds to mount the artifacts:
#   FROM "$RAY_CORE_IMAGE" AS ray_core
#   FROM "$RAY_DASHBOARD_IMAGE" AS ray_dashboard
#   FROM "$BASE_IMAGE"
#   RUN --mount=type=bind,from=ray_core,target=/opt/ray-core \
#       --mount=type=bind,from=ray_dashboard,target=/opt/ray-dashboard \
#       unzip /opt/ray-core/ray_pkg.zip -d python/ && \
#       SKIP_BAZEL_BUILD=1 pip install -e python/
#
# This is equivalent to what happens in CI when using:
#   bazel run //ci/ray_ci:test_in_docker -- ... --use-wanda-prebuilt
#
if [[ "$SKIP_RAY_INSTALL" != "true" ]]; then
    # Export variables for wanda
    export IMAGE_TO="$BUILD_IMAGE_NAME"
    export BUILD_TYPE="${BUILD_TYPE:-optimized}"

    FINAL_IMAGE_NAME="${BUILD_IMAGE_NAME}-ray"

    header "Building final test image with Ray installed (${FINAL_IMAGE_NAME})..."

    $WANDA_BIN ci/docker/tests.env.wanda.yaml

    # Tag for local use (wanda produces cr.ray.io/rayproject/... tags)
    docker tag "cr.ray.io/rayproject/${FINAL_IMAGE_NAME}:latest" \
               "${FINAL_IMAGE_NAME}:latest"

    header "Build complete!"
    echo ""
    echo "Images built:"
    echo "  - ${FINAL_IMAGE_NAME}:latest (final test container with Ray installed)"
    echo "  - ${BUILD_IMAGE_NAME}:latest (team base image with dependencies)"
    echo "  - ray-core-py${PYTHON_VERSION}:latest (pre-built C++ artifacts)"
    echo "  - ray-dashboard:latest (pre-built dashboard)"
    echo ""
    echo "To run tests:"
    echo "  ci/build/run-tests-local.sh python/ray/tests/test_basic.py --team ${TEAM}"
    echo ""
    echo "Or to enter an interactive shell:"
    echo "  ci/build/run-tests-local.sh --shell --team ${TEAM}"
else
    header "Build complete (Ray installation skipped)!"
    echo ""
    echo "Images built:"
    echo "  - ${BUILD_IMAGE_NAME}:latest (team base image with dependencies)"
    echo "  - ray-core-py${PYTHON_VERSION}:latest (pre-built C++ artifacts)"
    echo "  - ray-dashboard:latest (pre-built dashboard)"
    echo ""
    echo "To build the final image with Ray installed, run without --skip-ray-install"
fi
