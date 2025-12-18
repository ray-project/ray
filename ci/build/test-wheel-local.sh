#!/bin/bash
#
# [WIP] Test the locally built Ray wheel.
#
# Usage (from repo root):
#   ci/build/test-wheel-local.sh              # Run basic tests
#   ci/build/test-wheel-local.sh --quick      # Just verify import works
#   ci/build/test-wheel-local.sh --clean      # Remove test environment
#
set -euo pipefail

WHEEL_DIR="${WHEEL_DIR:-.whl}"
TEST_ENV="/tmp/ray-test-env"
PYTHON_BIN="${PYTHON_BIN:-$HOME/.conda/envs/py310/bin/python}"

# Find the wheel file
WHEEL_FILE=$(ls -1 "${WHEEL_DIR}"/ray-*.whl 2>/dev/null | head -1)
if [[ -z "${WHEEL_FILE}" ]]; then
    echo "Error: No wheel file found in ${WHEEL_DIR}/"
    echo "Run ci/build/build-wheel-local.sh 3.10 extract first"
    exit 1
fi

echo "Found wheel: ${WHEEL_FILE}"

# Handle --clean flag
if [[ "${1:-}" == "--clean" ]]; then
    echo "Removing test environment..."
    rm -rf "${TEST_ENV}"
    echo "Done."
    exit 0
fi

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
