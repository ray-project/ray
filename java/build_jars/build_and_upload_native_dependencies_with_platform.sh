#!/usr/bin/env bash

set -e

pip install oss2 wheel

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
pushd $ROOT_DIR/..

pushd ../
# Ray project root dir

./ci/travis/install-bazel.sh
./ci/travis/install-dependencies.sh
./build.sh -l java
echo "Build Java successfully."
popd

# Determine the platform suffix.
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PLATFORM_SUFFIX="os_linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  PLATFORM_SUFFIX="os_mac_osx"
else
  echo "Unrecognized platform."
  exit 1
fi

for filename in `ls ./runtime/native_dependencies/*`
do
  if [[ $filename == *.so ]];
  then
    mv $filename ${filename%.so}_$PLATFORM_SUFFIX.so
  elif [[ $filename == *.dylib ]];
  then
    mv $filename ${filename%.dylib}_$PLATFORM_SUFFIX.dylib
  else
    mv $filename ${filename}_$PLATFORM_SUFFIX;
  fi
done

# Upload the files to oss.
COMMIT_ID=$(git rev-parse HEAD)
python ./build_jars/oss_updater.py $COMMIT_ID upload

popd
