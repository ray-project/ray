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

COMMON_BUILD_DIR="$ROOT_DIR/build/src/common"
COMMON_SOURCE_DIR="$ROOT_DIR/src/common"
PLASMA_BUILD_DIR="$ROOT_DIR/build/src/plasma"
PLASMA_SOURCE_DIR="$ROOT_DIR/src/plasma"
PHOTON_BUILD_DIR="$ROOT_DIR/build/src/photon"
PHOTON_SOURCE_DIR="$ROOT_DIR/src/photon"
GLOBAL_SCHEDULER_BUILD_DIR="$ROOT_DIR/build/src/global_scheduler"
GLOBAL_SCHEDULER_SOURCE_DIR="$ROOT_DIR/src/global_scheduler"
NUMBUF_BUILD_DIR="$ROOT_DIR/build/src/numbuf"
NUMBUF_SOURCE_DIR="$ROOT_DIR/src/numbuf"

PYTHON_DIR="$ROOT_DIR/lib/python"
PYTHON_COMMON_DIR="$PYTHON_DIR/common"
PYTHON_PLASMA_DIR="$PYTHON_DIR/plasma"
PYTHON_PHOTON_DIR="$PYTHON_DIR/photon"
PYTHON_GLOBAL_SCHEDULER_DIR="$PYTHON_DIR/global_scheduler"
PYTHON_NUMBUF_DIR="$PYTHON_DIR/numbuf"

# First clean up old build files.
rm -rf "$ROOT_DIR/build"
mkdir "$ROOT_DIR/build"

pushd ./src/common/thirdparty/
  bash build-redis.sh
popd

bash ./src/numbuf/thirdparty/download_thirdparty.sh
bash ./src/numbuf/thirdparty/build_thirdparty.sh

# Now build everything.
pushd "$ROOT_DIR/build"
  cmake ..
  make
popd
cp "$COMMON_BUILD_DIR/thirdparty/redis/src/redis-server" "$PYTHON_COMMON_DIR/thirdparty/redis/src/"
cp "$COMMON_BUILD_DIR/redis_module/libray_redis_module.so" "$PYTHON_COMMON_DIR/redis_module/ray_redis_module.so" # XXX

cp "$PLASMA_BUILD_DIR/plasma_store" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_BUILD_DIR/plasma_manager" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_BUILD_DIR/libplasma.so" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_SOURCE_DIR/plasma/plasma.py" "$PYTHON_PLASMA_DIR/"
cp "$PLASMA_SOURCE_DIR/plasma/utils.py" "$PYTHON_PLASMA_DIR/"

cp "$PHOTON_BUILD_DIR/photon_scheduler" "$PYTHON_PHOTON_DIR/build/"
cp "$PHOTON_BUILD_DIR/libphoton.so" "$PYTHON_PHOTON_DIR/photon/"
cp "$PHOTON_SOURCE_DIR/photon/photon_services.py" "$PYTHON_PHOTON_DIR/photon/"

cp "$GLOBAL_SCHEDULER_BUILD_DIR/global_scheduler" "$PYTHON_GLOBAL_SCHEDULER_DIR/build/"
cp "$GLOBAL_SCHEDULER_SOURCE_DIR/lib/python/global_scheduler_services.py" "$PYTHON_GLOBAL_SCHEDULER_DIR/lib/python/"

cp "$NUMBUF_BUILD_DIR/libnumbuf.so" "$PYTHON_NUMBUF_DIR/"
cp "$NUMBUF_SOURCE_DIR/thirdparty/arrow/cpp/build/release/libarrow.so" "$PYTHON_NUMBUF_DIR/"
cp "$NUMBUF_SOURCE_DIR/numbuf/__init__.py" "$PYTHON_NUMBUF_DIR/"
