#!/bin/bash
#
# [WIP] Test a locally built Ray docker image (CPU or CUDA).
#
# Usage (from repo root):
#   ci/build/test-ray-image-local.sh                                    # Test default CPU image
#   ci/build/test-ray-image-local.sh cpu                                # Test CPU image
#   ci/build/test-ray-image-local.sh gpu                                # Test GPU image (CUDA 12.1.1)
#   ci/build/test-ray-image-local.sh cuda 12.1.1-cudnn8                 # Test specific CUDA image
#   ci/build/test-ray-image-local.sh --image rayproject/ray:nightly    # Test specific image
#   ci/build/test-ray-image-local.sh --quick                            # Just verify import works
#
set -euo pipefail

# Default GPU platform (matches GPU_PLATFORM in ci/ray_ci/docker_container.py)
GPU_PLATFORM="12.1.1-cudnn8"

PYTHON_VERSION="${PYTHON_VERSION:-3.10}"
IMAGE_TYPE="cpu"
CUDA_VERSION=""
QUICK_TEST=""
IMAGE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)
            IMAGE="$2"
            shift 2
            ;;
        --quick)
            QUICK_TEST="true"
            shift
            ;;
        cpu)
            IMAGE_TYPE="cpu"
            shift
            ;;
        gpu)
            IMAGE_TYPE="cuda"
            CUDA_VERSION="$GPU_PLATFORM"
            shift
            ;;
        cuda)
            IMAGE_TYPE="cuda"
            shift
            # check if next argument is a version, not a flag
            if [[ -n "${1:-}" && ! "${1:-}" =~ ^-- ]]; then
                CUDA_VERSION="$1"
                shift
            else
                # If no version is provided, use default GPU platform
                CUDA_VERSION="$GPU_PLATFORM"
            fi
            ;;
        *)
            echo "Error: Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# Determine image name if not explicitly provided
if [[ -z "${IMAGE:-}" ]]; then
    if [[ "$IMAGE_TYPE" == "cpu" ]]; then
        IMAGE="cr.ray.io/rayproject/ray:nightly-py${PYTHON_VERSION}-cpu"
    elif [[ "$IMAGE_TYPE" == "cuda" ]]; then
        if [[ -z "$CUDA_VERSION" ]]; then
            echo "Error: CUDA version required for cuda image type"
            echo "Usage: $0 cuda <CUDA_VERSION>"
            echo "Example: $0 cuda 12.1.1-cudnn8"
            exit 1
        fi
        IMAGE="cr.ray.io/rayproject/ray:nightly-py${PYTHON_VERSION}-cu${CUDA_VERSION}"
    else
        echo "Error: Unknown image type: $IMAGE_TYPE"
        echo "Usage: $0 [cpu|cuda] [CUDA_VERSION]"
        exit 1
    fi
fi

echo "Testing image: $IMAGE"
echo ""

# Check if image exists
if ! docker image inspect "$IMAGE" &>/dev/null; then
    echo "Error: Image '$IMAGE' not found locally"
    echo ""
    echo "Build it first with:"
    if [[ "$IMAGE_TYPE" == "cpu" ]]; then
        echo "  ci/build/build-ray-image-local.sh ${PYTHON_VERSION} cpu"
    else
        echo "  ci/build/build-ray-image-local.sh ${PYTHON_VERSION} cuda ${CUDA_VERSION}"
    fi
    exit 1
fi

# Docker run options
DOCKER_OPTS=(
    --rm
    --shm-size=2g
)

# Quick test - just verify import
if [[ "$QUICK_TEST" == "true" ]]; then
    echo "Running quick import test..."
    docker run "${DOCKER_OPTS[@]}" "$IMAGE" python -c "
import ray
print(f'Ray version: {ray.__version__}')
print('Quick test passed!')
" 2>&1 | grep -v "^==" | grep -v "WARNING:" | grep -v "Container image"
    exit 0
fi

# Full test - run basic Ray operations
echo "Running basic Ray tests..."
docker run "${DOCKER_OPTS[@]}" "$IMAGE" python -c "
import ray
import sys

print(f'Ray version: {ray.__version__}')
print(f'Ray commit: {ray.__commit__}')

# Initialize Ray
ray.init(num_cpus=2)

# Test basic task
@ray.remote
def hello():
    return 'Hello from Ray!'

result = ray.get(hello.remote())
print(f'Task result: {result}')

# Test basic actor
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    def increment(self):
        self.count += 1
        return self.count

counter = Counter.remote()
results = ray.get([counter.increment.remote() for _ in range(5)])
print(f'Actor results: {results}')

# Shutdown
ray.shutdown()

print()
print('All basic tests passed!')
sys.exit(0)
" 2>&1 | grep -v "^==" | grep -v "WARNING:" | grep -v "Container image" | grep -v "FutureWarning" | grep -v "Usage stats"

echo ""
echo "Done."
