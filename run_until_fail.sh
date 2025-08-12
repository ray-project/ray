#!/bin/bash

# Script to run the bazel test command repeatedly until it fails, then sleep for 1 hour

echo "Starting continuous test execution until failure..."

# Set the environment variables
export CI="1"
export RAY_CI_POST_WHEEL_TESTS="1"

# Initialize run counter
run_count=0

while true; do
    ((run_count++))
    echo "=========================================="
    echo "Run #${run_count} - $(date)"
    echo "=========================================="

    if [ $run_count -eq 1 ]; then
        bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests:test_logging_2 core \
        --except-tags no_windows \
        --build-name windowsbuild \
        --operating-system windows \
        --test-env=CI="1" \
        --test-env=RAY_CI_POST_WHEEL_TESTS="1" \
        --test-env=USERPROFILE \
        --workers "${BUILDKITE_PARALLEL_JOB_COUNT}" --worker-id "${BUILDKITE_PARALLEL_JOB}"
    else
        bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests:test_logging_2 core \
        --except-tags no_windows \
        --build-name windowsbuild \
        --operating-system windows \
        --test-env=CI="1" \
        --test-env=RAY_CI_POST_WHEEL_TESTS="1" \
        --test-env=USERPROFILE \
        --workers "${BUILDKITE_PARALLEL_JOB_COUNT}" --worker-id "${BUILDKITE_PARALLEL_JOB}" \
        --skip-ray-installation
    fi

    # Check the exit status
    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "=========================================="
        echo "FAILURE DETECTED on run #${run_count}!"
        echo "Command failed with exit code: ${exit_code}"
        echo "Time of failure: $(date)"
        echo "=========================================="
        echo "Sleeping for 1 hour (3600 seconds)..."
        sleep 3600
        echo "Sleep completed at $(date). Exiting script."
        exit $exit_code
    else
        echo "Run #${run_count} completed successfully at $(date)"
        echo "Continuing to next run..."
        echo ""
    fi
done
sleep 3600
