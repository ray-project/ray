#!/usr/bin/env bash

set -e

pip install oss2

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
pushd $ROOT_DIR/../..

# Ray root dir
./ci/travis/install-bazel.sh
./ci/travis/install-dependencies.sh

BAZEL_EXECUTABLE="$HOME/bin/bazel"

# Build Ray java for some dependencies ready.
"$BAZEL_EXECUTABLE" build //java:all

popd

pushd java/

# Remove unecessary native dependencies.
rm -rf runtime/runtime/native_dependencies

# Download the native depedencies with all platforms.
COMMIT_ID=$(git rev-parse HEAD)
python ./build_jars/oss_updater.py $COMMIT_ID download

# Package to api and runtime jars.
mvn clean package -DskipTests -Dcheckstyle.skip=true

# Upload the api and runtime jars to oss.
python ./build_jars/oss_updater.py $COMMIT_ID upload-jars

popd
