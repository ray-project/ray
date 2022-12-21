#!/usr/bin/env bash

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

download_wheel() {
  WHEEL_URL=$1
  OUTPUT_FILE=${WHEEL_URL##*/}
  if [ "${OVERWRITE-}" == "1" ] || [ ! -f "${OUTPUT_FILE}" ]; then
    wget "${WHEEL_URL}"
  fi
}

# Linux.
echo "Downloading Ray core Linux wheels"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp39-cp39-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp310-cp310-manylinux2014_x86_64.whl"

# macOS.
echo "Downloading Ray core MacOS wheels"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-macosx_10_15_intel.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-macosx_10_15_intel.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-macosx_10_15_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp39-cp39-macosx_10_15_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp310-cp310-macosx_10_15_universal2.whl"

# Windows.
echo "Downloading Ray core Windows wheels"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-win_amd64.whl"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-win_amd64.whl"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp39-cp39-win_amd64.whl"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp310-cp310-win_amd64.whl"

# Linux CPP.
echo "Downloading Ray CPP Linux wheels"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp36-cp36m-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp37-cp37m-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp38-cp38-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp39-cp39-manylinux2014_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp310-cp310-manylinux2014_x86_64.whl"

# macOS CPP.
echo "Downloading Ray CPP MacOS wheels"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp36-cp36m-macosx_10_15_intel.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp37-cp37m-macosx_10_15_intel.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp38-cp38-macosx_10_15_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp39-cp39-macosx_10_15_x86_64.whl"
download_wheel "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp310-cp310-macosx_10_15_universal2.whl"

# Windows CPP.
echo "Downloading Ray CPP Windows wheels"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp37-cp37m-win_amd64.whl"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp38-cp38-win_amd64.whl"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp39-cp39-win_amd64.whl"
download_wheel "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray_cpp-$RAY_VERSION-cp310-cp310-win_amd64.whl"
