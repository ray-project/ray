#!/usr/bin/env bash

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

TEST_PYPI_TOKEN="test_pypi_token"

download_wheel() {
    # Download single wheel from given URL into TMP_DIR/<wheel_filename>.whl
    WHEEL_URL=$1
    OUTPUT_FILE="${TMP_DIR}/${WHEEL_URL##*/}"
    if [ "${OVERWRITE-}" == "1" ] || [ ! -f "${OUTPUT_FILE}" ]; then
        wget -O "${OUTPUT_FILE}" "${WHEEL_URL}"
    fi
}

download_wheels() {
    # Iterate through all Python versions, platforms, and ray types and download wheels
    python_versions=("cp38-cp38" "cp39-cp39" "cp310-cp310" "cp311-cp311")
    platforms=("manylinux2014_x86_64" "manylinux2014_aarch64" "macosx_10_15_x86_64" "macosx_11_0_arm64" "win_amd64")
    ray_types=("ray" "ray_cpp")

    url_prefix="https://ray-wheels.s3-us-west-2.amazonaws.com/releases"
    for platform in "${platforms[@]}"; do
        for ray_type in "${ray_types[@]}"; do
            for python_version in "${python_versions[@]}"; do
                echo "Downloading $ray_type $platform $python_version wheel"
                download_wheel "$url_prefix/$RAY_VERSION/$RAY_HASH/$ray_type-$RAY_VERSION-$python_version-$platform.whl"
            done
        done
    done

    # Check if there are exactly 40 .whl files in the directory
    if [ $(ls -1q *.whl | wc -l) -ne 40 ]; then
        echo "Error: 40 wheels are expected, but $(ls -1q *.whl | wc -l) are found"
        exit 1
    fi
}

_clean_up() {
    rm -rf "$TMP_DIR"
}

TMP_DIR="$(mktemp -d "$HOME/tmp.XXXXXXXXXX")"
cd "$TMP_DIR"
trap _clean_up EXIT
download_wheels
twine upload --repository-url https://test.pypi.org/legacy/ *.whl -u __token__ -p "$TEST_PYPI_TOKEN"