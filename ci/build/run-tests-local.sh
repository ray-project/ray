#!/bin/bash
#
# Run Ray tests locally using pre-built test containers.
#
# This script runs tests inside the test containers built by build-test-container-local.sh.
# It supports both pytest and bazel test runners.
#
# ============================================================================
# IMAGE BUILD HIERARCHY (no wheel build needed!)
# ============================================================================
#
# Instead of building a wheel (which compiles C++ from scratch), we use pre-built
# artifacts from ray-core and ray-dashboard images. This saves ~20-30 minutes.
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                        BASE IMAGES                                      │
#   │                                                                         │
#   │   ubuntu:focal                     manylinux2014                        │
#   │        │                               │                                │
#   │        ▼                               ├──────────────┐                 │
#   │   oss-ci-base_test                     │              │                 │
#   │        │                               ▼              ▼                 │
#   │        ├────────────┐            ray-core       ray-dashboard           │
#   │        │            │            (C++ build)    (npm build)             │
#   │        ▼            ▼                 │              │                  │
#   │   oss-ci-base    oss-ci-base          │   ┌──────────┘                  │
#   │     _build         _ml                │   │                             │
#   │        │            │                 │   │  Pre-built artifacts:       │
#   │        │            │                 │   │  • ray_pkg.zip (C++)        │
#   │        │            │                 │   │  • dashboard.tar.gz         │
#   └────────┼────────────┼─────────────────┼───┼─────────────────────────────┘
#            │            │                 │   │
#            ▼            ▼                 │   │
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                     TEAM BUILD IMAGES                                   │
#   │                                                                         │
#   │   Teams using _build:          Teams using _ml:                         │
#   │   • corebuild                  • databuild                              │
#   │   • servebuild                 • mlbuild                                │
#   │   • docbuild                   • rllibuild                              │
#   │   • llmbuild                                                            │
#   │        │                              │                                 │
#   └────────┼──────────────────────────────┼─────────────────────────────────┘
#            │                              │
#            └──────────────┬───────────────┘
#                           │
#                           ▼
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                    FINAL TEST IMAGE                                     │
#   │                                                                         │
#   │   {team}build-py{ver}-ray                                               │
#   │                                                                         │
#   │   Built by tests.env.wanda.yaml:                                        │
#   │   1. Start from {team}build image                                       │
#   │   2. Mount ray-core artifacts (no C++ compile!)                         │
#   │   3. Mount ray-dashboard artifacts (no npm build!)                      │
#   │   4. SKIP_BAZEL_BUILD=1 pip install -e python/                          │
#   │                                                                         │
#   │   Result: Full test environment in ~2-3 minutes (vs 30-40 min wheel)    │
#   └─────────────────────────────────────────────────────────────────────────┘
#
# ============================================================================
# USAGE
# ============================================================================
#
# Examples:
#   # Run a single test file with pytest
#   ci/build/run-tests-local.sh python/ray/tests/test_basic.py
#
#   # Run with bazel target syntax
#   ci/build/run-tests-local.sh //python/ray/tests:test_basic
#
#   # Run tests for a specific team
#   ci/build/run-tests-local.sh python/ray/data/tests/test_dataset.py --team data
#
#   # Run with specific Python version
#   ci/build/run-tests-local.sh python/ray/tests/test_basic.py --python 3.11
#
#   # Run with bazel instead of pytest
#   ci/build/run-tests-local.sh //python/ray/tests:test_basic --bazel
#
#   # Pass additional arguments to pytest
#   ci/build/run-tests-local.sh python/ray/tests/test_basic.py -- -x -s
#
#   # Open interactive shell in test container
#   ci/build/run-tests-local.sh --shell
#
set -euo pipefail

# Source shared utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/local-build-utils.sh"

# Defaults
TEAM=""
PYTHON_VERSION="3.10"
USE_BAZEL=false
INTERACTIVE_SHELL=false
TEST_TARGET=""
EXTRA_ARGS=()

usage() {
    echo "Usage: $0 --team TEAM <test_target> [options] [-- extra_args]"
    echo ""
    echo "Run Ray tests locally using pre-built test containers."
    echo ""
    echo "Arguments:"
    echo "  test_target          Test file path or bazel target"
    echo "                       Examples: python/ray/tests/test_basic.py"
    echo "                                 //python/ray/tests:test_basic"
    echo ""
    echo "Required:"
    echo "  --team TEAM          Team container to use (REQUIRED)"
    echo "                       Options: core, data, serve, ml, rllib, doc, llm"
    echo ""
    echo "Options:"
    echo "  --python VERSION     Python version (default: 3.10)"
    echo "  --bazel              Use bazel test instead of pytest"
    echo "  --shell              Open interactive shell (no test target needed)"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Extra args:"
    echo "  Arguments after '--' are passed directly to pytest/bazel"
    echo "  Example: $0 test_basic.py -- -x -s --tb=short"
    echo ""
    echo "Prerequisites:"
    echo "  This script automatically builds all prerequisite images for the specified team."
    echo "  Wanda caches images, so unchanged images are reused instantly."
    echo "  First run may take 30-60 minutes. Subsequent runs use cached images."
    echo ""
    echo "Examples:"
    echo "  $0 --team core python/ray/tests/test_basic.py"
    echo "  $0 --team core python/ray/tests/test_basic.py --python 3.11"
    echo "  $0 --team data python/ray/data/tests/test_dataset.py"
    echo "  $0 --team core //python/ray/tests:test_basic --bazel"
    echo "  $0 --team core --shell"
    echo "  $0 --team core python/ray/tests/test_basic.py -- -x -s -k test_args"
    echo ""
    echo "Available teams: core, data, serve, ml, rllib, llm, doc"
    echo "See ci/docker/*.build.wanda.yaml for team-specific dependencies."
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --team)
            TEAM="$2"
            shift 2
            ;;
        --python)
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --bazel)
            USE_BAZEL=true
            shift
            ;;
        --shell)
            INTERACTIVE_SHELL=true
            shift
            ;;
        -h|--help|help)
            usage
            ;;
        --)
            shift
            EXTRA_ARGS=("$@")
            break
            ;;
        -*)
            echo "Error: Unknown option: $1"
            usage
            ;;
        *)
            if [[ -z "$TEST_TARGET" ]]; then
                TEST_TARGET="$1"
            else
                echo "Error: Multiple test targets specified"
                usage
            fi
            shift
            ;;
    esac
done

# Validate arguments - prompt for team if not provided
if [[ -z "$TEAM" ]]; then
    echo "Select a team container:"
    echo ""
    echo "  1) core   - Core Ray tests (uses oss-ci-base_build)"
    echo "  2) data   - Ray Data tests (uses oss-ci-base_ml)"
    echo "  3) serve  - Ray Serve tests (uses oss-ci-base_build)"
    echo "  4) ml     - Ray ML/Train/Tune tests (uses oss-ci-base_ml)"
    echo "  5) rllib  - RLlib tests (uses oss-ci-base_ml)"
    echo "  6) llm    - LLM tests (uses oss-ci-base_build)"
    echo "  7) doc    - Documentation tests (uses oss-ci-base_build)"
    echo ""
    read -p "Enter choice [1-7]: " choice
    case "$choice" in
        1) TEAM="core" ;;
        2) TEAM="data" ;;
        3) TEAM="serve" ;;
        4) TEAM="ml" ;;
        5) TEAM="rllib" ;;
        6) TEAM="llm" ;;
        7) TEAM="doc" ;;
        *) echo "Invalid choice"; exit 1 ;;
    esac
    echo ""
    echo "Selected team: $TEAM"
    echo "Hint: next time use --team $TEAM to skip this prompt"
fi

if [[ "$INTERACTIVE_SHELL" == "false" && -z "$TEST_TARGET" ]]; then
    echo "Error: Test target is required (or use --shell for interactive mode)"
    echo ""
    usage
fi

# Determine image name
BASE_IMAGE="${TEAM}build-py${PYTHON_VERSION}"
IMAGE="${BASE_IMAGE}-ray:latest"

# Map team to base type: "ml" teams need oss-ci-base_ml, others use oss-ci-base_build
get_team_base_type() {
    local team="$1"
    case "$team" in
        data|ml|rllib) echo "ml" ;;
        core|serve|doc|llm|*) echo "build" ;;
    esac
}

# Map team to its wanda file
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

# Setup environment and check prerequisites
setup_build_env
check_wanda_prerequisites "$WANDA_BIN"

# Export variables for wanda (needed by all wanda builds)
export PYTHON_VERSION
export PYTHON="$PYTHON_VERSION"
export ARCH_SUFFIX=""
export BUILD_TYPE="${BUILD_TYPE:-optimized}"
export HOSTTYPE="${HOSTTYPE:-x86_64}"
export MANYLINUX_VERSION="${MANYLINUX_VERSION:-251216.3835fc5}"

#
# Build prerequisite images if they don't exist
#
TEAM_BASE_TYPE=$(get_team_base_type "$TEAM")
TEAM_WANDA_FILE=$(get_team_wanda_file "$TEAM")

if [[ -z "$TEAM_WANDA_FILE" ]]; then
    echo "Error: Unknown team: $TEAM"
    exit 1
fi

# Step 1: Build oss-ci-base_test (wanda caches if unchanged)
header "Building oss-ci-base_test-py${PYTHON_VERSION}..."
$WANDA_BIN ci/docker/base.test.wanda.yaml

# Step 2: Build oss-ci-base_build or oss-ci-base_ml based on team
if [[ "$TEAM_BASE_TYPE" == "ml" ]]; then
    header "Building oss-ci-base_ml-py${PYTHON_VERSION} (required for $TEAM team)..."
    $WANDA_BIN ci/docker/base.ml.wanda.yaml
else
    header "Building oss-ci-base_build-py${PYTHON_VERSION}..."
    $WANDA_BIN ci/docker/base.build.wanda.yaml
fi

# Step 3: Build ray-core and ray-dashboard
header "Building ray-core-py${PYTHON_VERSION}..."
$WANDA_BIN ci/docker/ray-core.wanda.yaml

header "Building ray-dashboard..."
$WANDA_BIN ci/docker/ray-dashboard.wanda.yaml

# Step 4: Build team-specific container
header "Building ${BASE_IMAGE}..."

# Set IMAGE_FROM based on team base type
if [[ "$TEAM_BASE_TYPE" == "ml" ]]; then
    export IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_ml-py${PYTHON_VERSION}"
else
    export IMAGE_FROM="cr.ray.io/rayproject/oss-ci-base_build-py${PYTHON_VERSION}"
fi
export IMAGE_TO="$BASE_IMAGE"
export RAYCI_IS_GPU_BUILD=""

# LLM team needs RAY_CUDA_CODE (defaults to cpu for non-GPU builds)
if [[ "$TEAM" == "llm" ]]; then
    export RAY_CUDA_CODE="${RAY_CUDA_CODE:-cpu}"
fi

# Doc team needs REQUIREMENTS_FILE
if [[ "$TEAM" == "doc" ]]; then
    export REQUIREMENTS_FILE="python/deplocks/docs/docbuild_depset_py${PYTHON_VERSION}.lock"
fi

$WANDA_BIN "$TEAM_WANDA_FILE"

# Step 5: Build the final test image with Ray installed
header "Building test container ${BASE_IMAGE}-ray (wanda will use cache if available)..."
export IMAGE_TO="$BASE_IMAGE"
$WANDA_BIN ci/docker/tests.env.wanda.yaml

# Tag for local use
docker tag "cr.ray.io/rayproject/${BASE_IMAGE}-ray:latest" \
           "${IMAGE}" 2>/dev/null || true

echo ""

# Docker run options
# Mount local repo at /ray-mount and use it as workdir so test edits are reflected
DOCKER_OPTS=(
    --rm
    --shm-size=2.5gb
    --workdir=/ray-mount
    -v "$(pwd):/ray-mount"
    -e "RAY_USE_RANDOM_PORTS=1"
)

# Add -it only if we have a TTY
if [[ -t 0 ]]; then
    DOCKER_OPTS+=(-it)
fi

# Shell wrapper - needed to source conda environment in the container
# Based on linux_container.py:get_run_command_shell but without -u flag
# which causes issues with unbound variables in the container's bashrc
SHELL_CMD=("/bin/bash" "-ic")

# Interactive shell mode
if [[ "$INTERACTIVE_SHELL" == "true" ]]; then
    echo "Opening interactive shell in $IMAGE..."
    echo "  Working directory: /ray-mount (your local repo)"
    echo "  Edits to your local files are reflected immediately"
    echo ""
    docker run "${DOCKER_OPTS[@]}" "$IMAGE" /bin/bash
    exit 0
fi

# Convert test target to appropriate format
if [[ "$USE_BAZEL" == "true" ]]; then
    # Bazel mode: ensure target starts with //
    if [[ ! "$TEST_TARGET" =~ ^// ]]; then
        # Convert file path to bazel target
        # python/ray/tests/test_basic.py -> //python/ray/tests:test_basic
        TARGET_DIR=$(dirname "$TEST_TARGET")
        TARGET_NAME=$(basename "$TEST_TARGET" .py)
        TEST_TARGET="//${TARGET_DIR}:${TARGET_NAME}"
    fi

    echo "Running bazel test: $TEST_TARGET"
    echo "  Image: $IMAGE"
    echo "  Extra args: ${EXTRA_ARGS[*]:-none}"
    echo ""

    # Build the command string for bash
    BAZEL_CMD="bazel test $TEST_TARGET --test_output=all"
    if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
        BAZEL_CMD+=" ${EXTRA_ARGS[*]}"
    fi

    TEST_CMD="$BAZEL_CMD"
else
    # Pytest mode: convert bazel target to file path if needed
    if [[ "$TEST_TARGET" =~ ^// ]]; then
        # Convert //python/ray/tests:test_basic to python/ray/tests/test_basic.py
        TEST_TARGET="${TEST_TARGET#//}"
        TEST_TARGET="${TEST_TARGET/://}.py"
    fi

    # Ensure path doesn't have leading slash
    TEST_TARGET="${TEST_TARGET#/}"

    # Default pytest args
    PYTEST_ARGS=(-v)

    # Add extra args if provided
    if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
        PYTEST_ARGS+=("${EXTRA_ARGS[@]}")
    fi

    echo "Running pytest: $TEST_TARGET"
    echo "  Image: $IMAGE"
    echo "  Args: ${PYTEST_ARGS[*]}"
    echo ""

    # Build the command string for bash
    # The test container has Ray installed at /rayci
    TEST_CMD="pytest ${TEST_TARGET} ${PYTEST_ARGS[*]}"
fi

# Run the test and capture exit code
set +e
docker run "${DOCKER_OPTS[@]}" "$IMAGE" "${SHELL_CMD[@]}" "$TEST_CMD"
TEST_EXIT_CODE=$?
set -e

if [[ $TEST_EXIT_CODE -eq 0 ]]; then
    echo ""
    echo -e "\033[32;1m✓ Tests passed!\033[0m"
    exit 0
else
    echo ""
    echo -e "\033[31;1m✗ Tests failed (exit code: $TEST_EXIT_CODE)\033[0m"
    echo ""

    # Only drop into shell if we have a TTY
    if [[ -t 0 ]]; then
        # Create help message that can be reprinted
        HELP_MSG=$(cat <<EOF
\033[33;1m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m
\033[33;1m  To rerun the failed test (or press ↑):\033[0m

    \033[36m$TEST_CMD\033[0m

\033[33;1m  Useful pytest flags:\033[0m
    pytest ... -x              # Stop on first failure
    pytest ... -s              # Show print output
    pytest ... --pdb           # Drop into debugger on failure
    pytest ... -k 'test_name'  # Run specific test

\033[33;1m  Your local repo is mounted here - edits are reflected immediately!\033[0m

\033[33;1m  Type 'help' to show this message, 'exit' to leave\033[0m
\033[33;1m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m
EOF
)
        echo "Dropping into container shell for debugging..."
        echo ""
        echo -e "$HELP_MSG"
        echo ""

        # Debug shell runs from mounted repo so edits are reflected
        # Pass TEST_CMD and HELP_MSG, set up history and help function
        docker run "${DOCKER_OPTS[@]}" \
            -e "TEST_CMD=$TEST_CMD" \
            -e "HELP_MSG=$HELP_MSG" \
            "$IMAGE" /bin/bash -c "
                # Add test command to history so up-arrow shows it
                echo \"\$TEST_CMD\" >> ~/.bash_history
                # Add help function to bashrc
                echo 'help() { echo -e \"\$HELP_MSG\"; }' >> ~/.bashrc
                # Start interactive shell
                exec bash -i
            "
    fi

    exit $TEST_EXIT_CODE
fi
