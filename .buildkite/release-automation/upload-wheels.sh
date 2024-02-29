#!/usr/bin/env bash

set -euo pipefail

# Check if first argument is provided
if [[ "$#" -ne 1 ]]; then
    echo "Missing argument to specify PyPI environment"
    echo "Use: prod or test"
    exit 1
fi

# Check if first argument is valid
if [[ "$1" != "prod" ]] && [[ "$1" != "test" ]]; then
    echo "Invalid PyPI environment"
    echo "Use: prod or test"
    exit 1
fi

# PYPI_ENV can be "prod" or "test"
PYPI_ENV="$1"

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

retrieve_pypi_url() {
    if [[ "$PYPI_ENV" == "prod" ]]; then
        echo "https://upload.pypi.org/legacy/"
    else
        echo "https://test.pypi.org/legacy/"
    fi
}

retrieve_pypi_token() {
    bazel run //ci/ray_ci/automation:retrieve_pypi_token -- --env="$PYPI_ENV"
}

download_wheel() {
    # Download single wheel from given URL into TMP_DIR/<wheel_filename>.whl
    WHEEL_URL=$1
    OUTPUT_FILE="${TMP_DIR}/${WHEEL_URL##*/}"
    if [[ "${OVERWRITE-}" == "1" ]] || [[ ! -f "${OUTPUT_FILE}" ]]; then
        wget -O "${OUTPUT_FILE}" "${WHEEL_URL}"
    fi
}

download_wheels_from_s3() {
    # Iterate through all Python versions, platforms, and ray types and download wheels
    cd "$TMP_DIR" || exit 1

    PYTHON_VERSIONS=("cp38-cp38" "cp39-cp39" "cp310-cp310" "cp311-cp311")
    PLATFORMS=("manylinux2014_x86_64" "manylinux2014_aarch64" "macosx_10_15_x86_64" "macosx_11_0_arm64" "win_amd64")
    RAY_TYPES=("ray" "ray_cpp")

    URL_PREFIX="https://ray-wheels.s3-us-west-2.amazonaws.com/releases"
    for PLATFORM in "${PLATFORMS[@]}"; do
        for RAY_TYPE in "${RAY_TYPES[@]}"; do
            for PYTHON_VERSION in "${PYTHON_VERSIONS[@]}"; do
                echo "Downloading $RAY_TYPE $PLATFORM $PYTHON_VERSION wheel from S3"
                download_wheel "$URL_PREFIX/$RAY_VERSION/$RAY_HASH/$RAY_TYPE-$RAY_VERSION-$PYTHON_VERSION-$PLATFORM.whl"
            done
        done
    done

    # Check if there are accurate number of .whl files downloaded in the directory
    NUM_EXPECTED_WHEELS=$((${#PLATFORMS[@]} * ${#RAY_TYPES[@]} * ${#PYTHON_VERSIONS[@]}))
    NUM_EXISTING_WHEELS=$(find . -maxdepth 1 -name "*.whl" -print | wc -l)
    if [[ "$NUM_EXISTING_WHEELS" -ne "$NUM_EXPECTED_WHEELS" ]]; then
        echo "Error: $NUM_EXPECTED_WHEELS wheels are expected, but $NUM_EXISTING_WHEELS are found"
        exit 1
    fi
}

_clean_up() {
    rm -rf "$TMP_DIR" || true
}

# Set up a temporary directory to store S3 wheels in
TMP_DIR="$(mktemp -d "$HOME/tmp.XXXXXXXXXX")"
# Clean up the temporary directory on exit
trap _clean_up EXIT

download_wheels_from_s3

pip install twine

# Retrieve PyPI URL and token
PYPI_URL="$(retrieve_pypi_url)"
PYPI_TOKEN="$(retrieve_pypi_token)"

twine upload --repository-url "$PYPI_URL" ./*.whl -u __token__ -p "$PYPI_TOKEN"
