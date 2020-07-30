#!/usr/bin/env bash

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

# Make sure Linux wheels are downloadable without errors.
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp35-cp35m-manylinux1_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-manylinux1_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-manylinux1_x86_64.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-manylinux1_x86_64.whl"
# Make sure macos wheels are downloadable without errors.
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp35-cp35m-macosx_10_13_intel.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-macosx_10_13_intel.whl"
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-macosx_10_13_intel.whl"
# Wheel name convention has been changed from Python 3.8.
wget "https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-macosx_10_13_x86_64.whl"
# Make sure Windows wheels are downloadable without errors.
wget https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-win_amd64.whl
wget https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-win_amd64.whl
wget https://ray-wheels.s3-us-west-2.amazonaws.com/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-win_amd64.whl
