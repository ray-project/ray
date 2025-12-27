#!/bin/bash
#
# Basic test of the locally built Ray wheel.
#
# Usage (from repo root):
#   ci/build/test-wheel-local.sh              # Run basic tests on ray wheel
#   ci/build/test-wheel-local.sh --cpp        # Test ray_cpp wheel instead
#   ci/build/test-wheel-local.sh --quick      # Just verify import works
#   ci/build/test-wheel-local.sh --clean      # Remove test environment
#
set -euo pipefail

WHEEL_DIR="${WHEEL_DIR:-.whl}"
TEST_ENV="/tmp/ray-test-env"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Handle --clean flag early (doesn't require wheel file)
if [[ "${1:-}" == "--clean" ]]; then
    echo "Removing test environment..."
    rm -rf "${TEST_ENV}"
    echo "Done."
    exit 0
fi

# Find wheel files (both ray and ray_cpp)
shopt -s nullglob
RAY_WHEELS=("${WHEEL_DIR}"/ray-[0-9]*.whl)
RAY_CPP_WHEELS=("${WHEEL_DIR}"/ray_cpp-*.whl)
shopt -u nullglob

if [[ ${#RAY_WHEELS[@]} -eq 0 && ${#RAY_CPP_WHEELS[@]} -eq 0 ]]; then
    echo "Error: No wheel file found in ${WHEEL_DIR}/"
    echo "Run ci/build/build-wheel-local.sh first"
    exit 1
fi

# Default to ray wheel, allow override with --cpp flag
if [[ "${1:-}" == "--cpp" ]]; then
    shift
    if [[ ${#RAY_CPP_WHEELS[@]} -eq 0 ]]; then
        echo "Error: No ray_cpp wheel found in ${WHEEL_DIR}/"
        exit 1
    fi
    WHEEL_FILE="${RAY_CPP_WHEELS[0]}"
else
    if [[ ${#RAY_WHEELS[@]} -eq 0 ]]; then
        echo "Error: No ray wheel found in ${WHEEL_DIR}/"
        echo "Found ray_cpp wheels: ${RAY_CPP_WHEELS[*]:-none}"
        exit 1
    fi
    if [[ ${#RAY_WHEELS[@]} -gt 1 ]]; then
        echo "Error: Multiple ray wheel files found. Please clean up ${WHEEL_DIR}/."
        ls -1 "${RAY_WHEELS[@]}"
        exit 1
    fi
    WHEEL_FILE="${RAY_WHEELS[0]}"
fi

echo "Found wheel: ${WHEEL_FILE}"

# Create virtual environment if it doesn't exist
if [[ ! -d "${TEST_ENV}" ]]; then
    echo "Creating virtual environment at ${TEST_ENV} using ${PYTHON_BIN}..."
    "${PYTHON_BIN}" -m venv "${TEST_ENV}"
fi

# Activate and install
echo "Installing wheel..."
source "${TEST_ENV}/bin/activate"
pip install --upgrade pip -q
pip install "${WHEEL_FILE}" -q

# Quick test - just verify import
if [[ "${1:-}" == "--quick" ]]; then
    echo "Running quick import test..."
    python -c "import ray; print(f'Ray version: {ray.__version__}')"
    echo "Quick test passed!"
    exit 0
fi

# Full test - run basic Ray operations
echo "Running basic Ray tests..."
python3 << 'EOF'
import ray
import sys

print(f"Ray version: {ray.__version__}")
print(f"Ray commit: {ray.__commit__}")

# Initialize Ray
ray.init(num_cpus=2)

# Test basic task
@ray.remote
def hello():
    return "Hello from Ray!"

result = ray.get(hello.remote())
print(f"Task result: {result}")

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
print(f"Actor results: {results}")

# Shutdown
ray.shutdown()

print("\n✓ All basic tests passed!")
sys.exit(0)
EOF

echo "Done."
