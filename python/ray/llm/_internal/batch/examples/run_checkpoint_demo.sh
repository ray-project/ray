#!/bin/bash
# Bash script to run vLLM checkpoint demo in separate executions
# This ensures complete resource cleanup between runs

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="${SCRIPT_DIR}/vllm_checkpoint_demo.py"

echo "=========================================="
echo "vLLM Checkpoint Demo - Separate Executions"
echo "=========================================="

# Step 1: Setup - Create sample data
echo ""
echo "Step 1: Creating sample data..."
python3 "${PYTHON_SCRIPT}" setup

# Step 2: Run 1 - Pipeline with simulated failure
echo ""
echo "Step 2: Running pipeline with simulated failure..."
python3 "${PYTHON_SCRIPT}" run1 --fail-after-id 100 || true  # Allow failure

# Wait a bit to ensure cleanup
sleep 2

# Step 3: Run 2 - Resume from checkpoint
echo ""
echo "Step 3: Resuming from checkpoint..."
python3 "${PYTHON_SCRIPT}" run2

# Wait a bit to ensure cleanup
sleep 2

# Step 4: Verify results
echo ""
echo "Step 4: Verifying results..."
python3 "${PYTHON_SCRIPT}" verify

echo ""
echo "=========================================="
echo "Demo completed successfully!"
echo "=========================================="
