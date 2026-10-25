#!/bin/bash
# ABOUTME: Builds Ray docker images with incremental caching for fast iteration.
# ABOUTME: Supports full, ray-ml, and python-only overlay builds.

set -euo pipefail

ECR_REGISTRY="${ECR_REGISTRY:-830883877497.dkr.ecr.us-west-2.amazonaws.com}"
ECR_REPO="${ECR_REPO:-anyscale/ray}"
BAZEL_CACHE_DIR="${BAZEL_CACHE_DIR:-${HOME}/.cache/ray-bazel}"
PYTHON_VERSION="3.10"
IMAGE_TAG=""
PYTHON_ONLY=""
BUILD_EXTRA=""
BUILD_ML=""
BASE_IMAGE_URI=""
CLEAN_CACHE=""

usage() {
    cat <<EOF
Usage: build_incremental_ray.sh --tag <tag> [options]

Builds a Ray docker image and pushes it to ECR with incremental caching.
Must be run from a ray or rayturbo source directory.

Modes:
  Full build (default):
    Builds wheel (with persistent Bazel cache), builds docker image
    (reusing cached base-deps when possible), pushes to ECR.

  Extra build (--extra):
    Full build + base-extra layer (profiling tools, gdb, cloud SDKs, etc.)
    on top. Uses docker/base-extra/Dockerfile from the repo. The base-extra
    layer is cached in ECR by hash of its Dockerfile and lock file.

  ML build (--ml):
    Full build + ray-ml layer (TensorFlow, PyTorch, etc.) on top.
    Uses docker/ray-ml/Dockerfile from the repo. The ray-ml layer is
    cached in ECR by hash of its requirements and install script.

  Python-only build (--python-only):
    Overlays changed Python source files onto an existing ray image.
    Skips wheel build and base-deps entirely. Takes ~1 minute.

Required:
  --tag <tag>                Docker image tag (e.g. my_feature_v1)

Build mode options:
  --extra                    Build base-extra image (profiling tools, cloud SDKs)
  --ml                       Build ray-ml image (includes ML frameworks)
  --python-only              Enable python-only overlay mode
  --base-image <ecr-uri>     Base image to overlay onto (required with --python-only)

Optional:
  --registry <url>           ECR registry (default: $ECR_REGISTRY)
  --repo <path>              ECR repository path (default: $ECR_REPO)
  --python-version <ver>     Python version (default: $PYTHON_VERSION)
  --clean-cache              Delete Bazel disk cache before building
  -h, --help                 Show this help message

Environment variables:
  ECR_REGISTRY               Override the default ECR registry URL
  ECR_REPO                   Override the default ECR repository path
  BAZEL_CACHE_DIR            Bazel disk cache location (default: ~/.cache/ray-bazel)

Examples:
  # Full build (first time)
  build_incremental_ray.sh --tag v1

  # Full rebuild after C++ changes (Bazel cache speeds this up)
  build_incremental_ray.sh --tag v2

  # Full build with profiling tools (nsys, perf, gdb, etc.)
  build_incremental_ray.sh --tag v2-extra --extra

  # Full build with profiling tools + ML frameworks
  build_incremental_ray.sh --tag v2-extra-ml --extra --ml

  # Full build with ML frameworks (PyTorch, TensorFlow, etc.)
  build_incremental_ray.sh --tag v2-ml --ml

  # Python-only overlay (after changing python/ray/data/*.py)
  build_incremental_ray.sh --tag v3 --python-only --base-image \\
      830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray:v2-ml
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            shift
            IMAGE_TAG="$1"
            ;;
        --python-only)
            PYTHON_ONLY=1
            ;;
        --extra)
            BUILD_EXTRA=1
            ;;
        --ml)
            BUILD_ML=1
            ;;
        --base-image)
            shift
            BASE_IMAGE_URI="$1"
            ;;
        --registry)
            shift
            ECR_REGISTRY="$1"
            ;;
        --repo)
            shift
            ECR_REPO="$1"
            ;;
        --python-version)
            shift
            PYTHON_VERSION="$1"
            ;;
        --clean-cache)
            CLEAN_CACHE=1
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            echo "Error: Unknown option '$1'" >&2
            usage 1
            ;;
    esac
    shift
done

if [[ -z "$IMAGE_TAG" ]]; then
    echo "Error: --tag is required" >&2
    usage 1
fi

if [[ -n "$PYTHON_ONLY" && -z "$BASE_IMAGE_URI" ]]; then
    echo "Error: --base-image is required when using --python-only" >&2
    exit 1
fi

# Validate we're in a ray source directory
if [[ ! -d "python/ray" ]]; then
    echo "Error: Must be run from a ray or rayturbo source directory." >&2
    exit 1
fi

# Extract AWS region from registry URL
ECR_REGION=$(echo "$ECR_REGISTRY" | grep -oP 'ecr\.\K[^.]+')
if [[ -z "$ECR_REGION" ]]; then
    echo "Error: Could not extract AWS region from registry URL: $ECR_REGISTRY" >&2
    exit 1
fi

# Validate AWS credentials before doing any work
echo "=== Checking AWS credentials for $ECR_REGISTRY ==="
if ! aws sts get-caller-identity --region "$ECR_REGION" >/dev/null 2>&1; then
    echo "Error: AWS credentials are not configured or have expired." >&2
    echo "  Run 'aws configure' or refresh your SSO session." >&2
    exit 1
fi

if ! aws ecr get-login-password --region "$ECR_REGION" \
    | docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null 2>&1; then
    echo "Error: Failed to authenticate with ECR registry $ECR_REGISTRY" >&2
    exit 1
fi
echo "AWS credentials verified."

ECR_FULL_TAG="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

# ---------------------------------------------------------------------------
# Python-only overlay mode
# ---------------------------------------------------------------------------
if [[ -n "$PYTHON_ONLY" ]]; then
    echo "=== Python-only overlay mode ==="

    # Safety check: ensure no C++/Cython files have uncommitted changes
    # relative to HEAD. If the user changed C++ files, they need a full build.
    CPP_CHANGES=$(git diff --name-only HEAD -- \
        'src/' \
        'python/ray/_raylet.pyx' \
        'python/ray/_raylet.pxd' \
        'python/ray/includes/' \
        'BUILD.bazel' \
        'WORKSPACE' \
    ) || true

    if [[ -n "$CPP_CHANGES" ]]; then
        echo "Error: C++/Cython/build files have uncommitted changes:" >&2
        echo "$CPP_CHANGES" >&2
        echo "" >&2
        echo "These require a full build. Run without --python-only." >&2
        exit 1
    fi

    # Build a thin overlay image
    OVERLAY_DIR=$(mktemp -d)
    trap 'rm -rf "$OVERLAY_DIR"' EXIT

    # Copy only the Python source tree (excludes C extensions, compiled files)
    # rsync is faster than cp for large trees and lets us exclude patterns
    rsync -a \
        --include='*.py' \
        --include='*/' \
        --exclude='*' \
        python/ray/ "$OVERLAY_DIR/ray/"

    SITE_PACKAGES="/home/ray/anaconda3/lib/python${PYTHON_VERSION}/site-packages"

    cat > "$OVERLAY_DIR/Dockerfile" <<DOCKERFILE
FROM ${BASE_IMAGE_URI}
COPY ray/ ${SITE_PACKAGES}/ray/
DOCKERFILE

    echo "=== Building overlay image ==="
    docker build -t "ray-build:${IMAGE_TAG}" "$OVERLAY_DIR"

    echo "=== Pushing to ECR ==="
    docker tag "ray-build:${IMAGE_TAG}" "$ECR_FULL_TAG"
    docker push "$ECR_FULL_TAG"

    echo ""
    echo "=== Done (python-only) ==="
    echo "Image pushed to: $ECR_FULL_TAG"
    exit 0
fi

# ---------------------------------------------------------------------------
# Full build mode
# ---------------------------------------------------------------------------

if [[ ! -f "python/build-wheel-manylinux2014.sh" ]]; then
    echo "Error: Cannot find python/build-wheel-manylinux2014.sh" >&2
    exit 1
fi

# --- Improvement 1: Persistent Bazel cache ---

if [[ -n "$CLEAN_CACHE" ]]; then
    echo "=== Cleaning Bazel cache at $BAZEL_CACHE_DIR ==="
    rm -rf "$BAZEL_CACHE_DIR"
fi

mkdir -p "$BAZEL_CACHE_DIR"
echo "=== Bazel disk cache: $BAZEL_CACHE_DIR ($(du -sh "$BAZEL_CACHE_DIR" 2>/dev/null | cut -f1)) ==="

# Remove stale wheels from previous builds to avoid picking the wrong version.
rm -f .whl/ray-*.whl .whl/ray_cpp-*.whl

# Build the manylinux wheel with persistent Bazel cache
echo "=== Building Ray wheel ==="
git_parent_args=()
if [[ -f .git ]] && grep ^gitdir .git >/dev/null 2>&1; then
	parent=$(grep ^gitdir .git | awk '{print $2}' | sed 's;/worktrees.*$;;')
	git_parent_args=(-v "${parent}:${parent}")
fi

docker run -ti --rm \
    -v "$BAZEL_CACHE_DIR:/bazel-cache" \
    -e BAZEL_ARGS="--disk_cache=/bazel-cache" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -e BUILDKITE_COMMIT="$(git rev-parse HEAD)" \
    -e BUILD_ONE_PYTHON_ONLY=py310 \
    "${git_parent_args[@]}" \
    -w /ray -v "$(pwd)":/ray \
    -e HOME=/tmp \
    quay.io/pypa/manylinux2014_x86_64:2026.01.02-1 \
    /ray/python/build-wheel-manylinux2014.sh

# --- Improvement 2: Cache base-deps by lock file hash ---

LOCK_FILE="python/deplocks/base_deps/ray_base_deps_py${PYTHON_VERSION}.lock"
if [[ ! -f "$LOCK_FILE" ]]; then
    echo "Error: Lock file not found: $LOCK_FILE" >&2
    exit 1
fi

# Hash all files that are COPYed into the base-deps image: the lock file,
# the constraints file (baked in as requirements_compiled.txt), and the Dockerfile.
CONSTRAINTS_FILE="python/requirements_compiled_py${PYTHON_VERSION}.txt"
if [[ ! -f "$CONSTRAINTS_FILE" ]]; then
    echo "Error: Constraints file not found: $CONSTRAINTS_FILE" >&2
    exit 1
fi
BASE_DEPS_HASH=$(cat "$LOCK_FILE" "$CONSTRAINTS_FILE" docker/base-deps/Dockerfile | sha256sum | cut -c1-12)
BASE_DEPS_ECR_TAG="base-deps-${BASE_DEPS_HASH}"
BASE_DEPS_ECR_IMAGE="${ECR_REGISTRY}/${ECR_REPO}:${BASE_DEPS_ECR_TAG}"

if aws ecr describe-images \
    --repository-name "$ECR_REPO" \
    --image-ids imageTag="$BASE_DEPS_ECR_TAG" \
    --region "$ECR_REGION" >/dev/null 2>&1; then

    echo "=== base-deps cached as $BASE_DEPS_ECR_TAG, pulling ==="
    docker pull "$BASE_DEPS_ECR_IMAGE"
    docker tag "$BASE_DEPS_ECR_IMAGE" "rayproject/base-deps:dev"
else
    echo "=== base-deps cache miss ($BASE_DEPS_ECR_TAG), building ==="

    # Build base-deps (replicated from build-docker.sh to avoid needing to
    # modify that script with a --base-deps-only flag)
    RAY_DEPS_BUILD_DIR="$(mktemp -d)"
    trap 'rm -rf "$RAY_DEPS_BUILD_DIR"' EXIT

    cp docker/base-deps/Dockerfile "${RAY_DEPS_BUILD_DIR}/"
    mkdir -p "${RAY_DEPS_BUILD_DIR}/python"
    cp "python/requirements_compiled.txt" "${RAY_DEPS_BUILD_DIR}/python/"
    cp "python/requirements_compiled_py${PYTHON_VERSION}.txt" "${RAY_DEPS_BUILD_DIR}/python/"
    cp "$LOCK_FILE" "${RAY_DEPS_BUILD_DIR}/"

    PYTHON_DEPSET_FILE_NAME="ray_base_deps_py${PYTHON_VERSION}.lock"

    export DOCKER_BUILDKIT=1
    docker build \
        --build-arg BASE_IMAGE="ubuntu:22.04" \
        --build-arg PYTHON_VERSION="${PYTHON_VERSION}" \
        --build-arg PYTHON_DEPSET="${PYTHON_DEPSET_FILE_NAME}" \
        -t "rayproject/base-deps:dev" "${RAY_DEPS_BUILD_DIR}"

    rm -rf "$RAY_DEPS_BUILD_DIR"
    trap - EXIT

    # Push base-deps to ECR for future reuse
    echo "=== Pushing base-deps cache as $BASE_DEPS_ECR_TAG ==="
    docker tag "rayproject/base-deps:dev" "$BASE_DEPS_ECR_IMAGE"
    docker push "$BASE_DEPS_ECR_IMAGE"
fi

# --- Build the ray image on top of cached base-deps ---

echo "=== Building ray image ==="

RAY_BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$RAY_BUILD_DIR"' EXIT

mkdir -p "$RAY_BUILD_DIR/.whl"
cp .whl/* "$RAY_BUILD_DIR/.whl"
cp docker/ray/Dockerfile "$RAY_BUILD_DIR"

WHEEL="$(basename .whl/ray-*.whl)"
WHEEL_HASH=$(sha256sum ".whl/${WHEEL}" | cut -c1-12)

LOCAL_TAG="ray-build:${IMAGE_TAG}"

export DOCKER_BUILDKIT=1
docker build \
    --build-arg FULL_BASE_IMAGE="rayproject/base-deps:dev" \
    --build-arg WHEEL_PATH=".whl/${WHEEL}" \
    -t "$LOCAL_TAG" "$RAY_BUILD_DIR"

rm -rf "$RAY_BUILD_DIR"
trap - EXIT

# ---------------------------------------------------------------------------
# base-extra layer (optional, on top of ray)
# ---------------------------------------------------------------------------
if [[ -n "$BUILD_EXTRA" ]]; then
    echo "=== Building base-extra layer ==="

    EXTRA_LOCK="python/deplocks/base_extra/ray_base_extra_py${PYTHON_VERSION}.lock"
    if [[ ! -f "$EXTRA_LOCK" ]]; then
        echo "Error: Lock file not found: $EXTRA_LOCK" >&2
        exit 1
    fi

    EXTRA_HASH=$(cat \
        docker/base-extra/Dockerfile \
        "$EXTRA_LOCK" \
        | sha256sum | cut -c1-12)
    BASE_EXTRA_ECR_TAG="base-extra-${BASE_DEPS_HASH}-${WHEEL_HASH}-${EXTRA_HASH}"
    BASE_EXTRA_ECR_IMAGE="${ECR_REGISTRY}/${ECR_REPO}:${BASE_EXTRA_ECR_TAG}"

    if aws ecr describe-images \
        --repository-name "$ECR_REPO" \
        --image-ids imageTag="$BASE_EXTRA_ECR_TAG" \
        --region "$ECR_REGION" >/dev/null 2>&1; then

        echo "=== base-extra cached as $BASE_EXTRA_ECR_TAG, pulling ==="
        docker pull "$BASE_EXTRA_ECR_IMAGE"
        docker tag "$BASE_EXTRA_ECR_IMAGE" "base-extra-build:${IMAGE_TAG}"
    else
        echo "=== base-extra cache miss ($BASE_EXTRA_ECR_TAG), building ==="

        BASE_EXTRA_BUILD_DIR="$(mktemp -d)"
        trap 'rm -rf "$BASE_EXTRA_BUILD_DIR"' EXIT

        cp docker/base-extra/Dockerfile "$BASE_EXTRA_BUILD_DIR/"
        mkdir -p "$BASE_EXTRA_BUILD_DIR/python/deplocks/base_extra"
        cp "$EXTRA_LOCK" "$BASE_EXTRA_BUILD_DIR/python/deplocks/base_extra/"

        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg BASE_IMAGE="$LOCAL_TAG" \
            --build-arg PYTHON_VERSION="${PYTHON_VERSION}" \
            -t "base-extra-build:${IMAGE_TAG}" "$BASE_EXTRA_BUILD_DIR"

        rm -rf "$BASE_EXTRA_BUILD_DIR"
        trap - EXIT

        echo "=== Pushing base-extra cache as $BASE_EXTRA_ECR_TAG ==="
        docker tag "base-extra-build:${IMAGE_TAG}" "$BASE_EXTRA_ECR_IMAGE"
        docker push "$BASE_EXTRA_ECR_IMAGE"
    fi

    LOCAL_TAG="base-extra-build:${IMAGE_TAG}"
fi

# ---------------------------------------------------------------------------
# Ray-ML layer (optional, on top of ray)
# ---------------------------------------------------------------------------
if [[ -n "$BUILD_ML" ]]; then
    echo "=== Building ray-ml layer ==="

    # Cache the ray-ml layer by hashing its inputs: requirements, install
    # script, and the base ray image (represented by base-deps hash + wheel).
    # This way a rebuild is only triggered when ML deps or the ray image change.
    ML_HASH_INPUTS=$(cat \
        python/requirements.txt \
        python/requirements_compiled.txt \
        python/requirements/ml/*requirements.txt \
        python/requirements/docker/*requirements.txt \
        docker/ray-ml/install-ml-docker-requirements.sh \
        docker/ray-ml/Dockerfile \
        | sha256sum | cut -c1-12)
    RAY_ML_ECR_TAG="ray-ml-${BASE_DEPS_HASH}-${WHEEL_HASH}-${ML_HASH_INPUTS}"
    RAY_ML_ECR_IMAGE="${ECR_REGISTRY}/${ECR_REPO}:${RAY_ML_ECR_TAG}"

    if aws ecr describe-images \
        --repository-name "$ECR_REPO" \
        --image-ids imageTag="$RAY_ML_ECR_TAG" \
        --region "$ECR_REGION" >/dev/null 2>&1; then

        echo "=== ray-ml cached as $RAY_ML_ECR_TAG, pulling ==="
        docker pull "$RAY_ML_ECR_IMAGE"
        docker tag "$RAY_ML_ECR_IMAGE" "ray-ml-build:${IMAGE_TAG}"
    else
        echo "=== ray-ml cache miss ($RAY_ML_ECR_TAG), building ==="

        RAY_ML_BUILD_DIR="$(mktemp -d)"
        trap 'rm -rf "$RAY_ML_BUILD_DIR"' EXIT

        # The Dockerfile expects paths relative to repo root:
        #   python/*requirements.txt
        #   python/requirements/ml/*requirements.txt
        #   python/requirements/docker/*requirements.txt
        #   docker/ray-ml/install-ml-docker-requirements.sh
        mkdir -p "$RAY_ML_BUILD_DIR/python/requirements/ml"
        mkdir -p "$RAY_ML_BUILD_DIR/python/requirements/docker"
        mkdir -p "$RAY_ML_BUILD_DIR/docker/ray-ml"

        cp python/*requirements*.txt "$RAY_ML_BUILD_DIR/python/"
        cp python/requirements/ml/*requirements.txt "$RAY_ML_BUILD_DIR/python/requirements/ml/"
        cp python/requirements/docker/*requirements.txt "$RAY_ML_BUILD_DIR/python/requirements/docker/"
        cp docker/ray-ml/install-ml-docker-requirements.sh "$RAY_ML_BUILD_DIR/docker/ray-ml/"
        cp docker/ray-ml/Dockerfile "$RAY_ML_BUILD_DIR/"

        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg FULL_BASE_IMAGE="$LOCAL_TAG" \
            -t "ray-ml-build:${IMAGE_TAG}" "$RAY_ML_BUILD_DIR"

        rm -rf "$RAY_ML_BUILD_DIR"
        trap - EXIT

        echo "=== Pushing ray-ml cache as $RAY_ML_ECR_TAG ==="
        docker tag "ray-ml-build:${IMAGE_TAG}" "$RAY_ML_ECR_IMAGE"
        docker push "$RAY_ML_ECR_IMAGE"
    fi

    LOCAL_TAG="ray-ml-build:${IMAGE_TAG}"
fi

# --- Tag and push ---

echo "=== Pushing to ECR ==="
docker tag "$LOCAL_TAG" "$ECR_FULL_TAG"
docker push "$ECR_FULL_TAG"

echo ""
DONE_LABEL="full build"
if [[ -n "$BUILD_EXTRA" ]]; then
    DONE_LABEL="$DONE_LABEL + base-extra"
fi
if [[ -n "$BUILD_ML" ]]; then
    DONE_LABEL="$DONE_LABEL + ray-ml"
fi
echo "=== Done ($DONE_LABEL) ==="
echo "Image pushed to: $ECR_FULL_TAG"
echo ""
echo "Bazel cache: $BAZEL_CACHE_DIR ($(du -sh "$BAZEL_CACHE_DIR" 2>/dev/null | cut -f1))"
echo "base-deps cache tag: $BASE_DEPS_ECR_TAG"
if [[ -n "$BUILD_EXTRA" ]]; then
    echo "base-extra cache tag: $BASE_EXTRA_ECR_TAG"
fi
if [[ -n "$BUILD_ML" ]]; then
    echo "ray-ml cache tag: $RAY_ML_ECR_TAG"
fi
echo ""
echo "For subsequent Python-only changes, use:"
echo "  $(basename "$0") --tag <new_tag> --python-only --base-image $ECR_FULL_TAG"
