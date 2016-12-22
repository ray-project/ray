#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Determine how many parallel jobs to use for make based on the number of cores
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=$(nproc)
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo "Unrecognized platform."
  exit 1
fi

COMMON_DIR="$ROOT_DIR/src/common"
PLASMA_DIR="$ROOT_DIR/src/plasma"
PHOTON_DIR="$ROOT_DIR/src/photon"
GLOBAL_SCHEDULER_DIR="$ROOT_DIR/src/global_scheduler"

PYTHON_DIR="$ROOT_DIR/lib/python"
PYTHON_COMMON_DIR="$PYTHON_DIR/common"
PYTHON_PLASMA_DIR="$PYTHON_DIR/plasma"
PYTHON_PHOTON_DIR="$PYTHON_DIR/photon"
PYTHON_GLOBAL_SCHEDULER_DIR="$PYTHON_DIR/global_scheduler"

# First clean up old build files.
pushd "$COMMON_DIR"
  make clean
popd
pushd "$PLASMA_DIR"
  make clean
popd
pushd "$PHOTON_DIR"
  make clean
popd
pushd "$GLOBAL_SCHEDULER_DIR"
  make clean
popd

# Now build everything.
pushd "$COMMON_DIR"
  make
popd
cp "$COMMON_DIR/thirdparty/redis/src/redis-server" "$PYTHON_COMMON_DIR/thirdparty/redis/src/"
cp "$COMMON_DIR/redis_module/ray_redis_module.so" "$PYTHON_COMMON_DIR/redis_module/ray_redis_module.so"

pushd "$PLASMA_DIR"
  make
  pushd "$PLASMA_DIR/build"
    cmake ..
    make install
  popd
popd
cp "$PLASMA_DIR/build/plasma_store" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_DIR/build/plasma_manager" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_DIR/plasma/plasma.py" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_DIR/plasma/utils.py" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_DIR/plasma/libplasma.so" "$PYTHON_PLASMA_DIR/"

pushd "$PHOTON_DIR"
  make
  pushd "$PHOTON_DIR/build"
    cmake ..
    make install
  popd
popd
cp "$PHOTON_DIR/build/photon_scheduler" "$PYTHON_PHOTON_DIR/build/"
cp "$PHOTON_DIR/photon/libphoton.so" "$PYTHON_PHOTON_DIR/photon/"
cp "$PHOTON_DIR/photon/photon_services.py" "$PYTHON_PHOTON_DIR/photon/"

pushd "$GLOBAL_SCHEDULER_DIR"
  make
popd
cp "$GLOBAL_SCHEDULER_DIR/build/global_scheduler" "$PYTHON_GLOBAL_SCHEDULER_DIR/build/"
cp "$GLOBAL_SCHEDULER_DIR/lib/python/global_scheduler_services.py" "$PYTHON_GLOBAL_SCHEDULER_DIR/lib/python/"
