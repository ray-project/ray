#!/usr/bin/env bash

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

# Linux.
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-manylinux2014_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-manylinux2014_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-manylinux2014_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp39-cp39-manylinux2014_x86_64.whl"

# macOS.
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-macosx_10_15_intel.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-macosx_10_15_intel.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-macosx_10_15_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp39-cp39-macosx_10_15_x86_64.whl"

# Windows.
wget "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-win_amd64.whl"
wget "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-win_amd64.whl"
wget "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-win_amd64.whl"
wget "https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp39-cp39-win_amd64.whl"
