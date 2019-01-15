#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Copy the libs to thirdparty/external_project_libs.
# -n means do not build again.
bash $ROOT_DIR/../thirdparty/scripts/collect_dependent_libs.sh -n

# Import all the libs cached in local to environment variables.
source $ROOT_DIR/../thirdparty/external_project_libs/resource.txt

# Clean the arrow building libs and use the cache libs.
pushd $ROOT_DIR/../build/external/arrow/src/arrow_ep
echo "Clean Arrow building files."
rm -rf $ROOT_DIR/../build/external/arrow/src/arrow_ep-build/
popd

# Rebuild ray with libs cache environment variables, which is fast.
bash $ROOT_DIR/install-ray.sh
