#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

# As the supported Python versions change, edit this array:
SUPPORTED_PYTHONS=( "3.5" "3.6" "3.7" )

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function usage()
{
  cat <<EOF
Usage: build.sh [<args>]

Options:
  -h|--help               print the help info
  -l|--language language1[,language2]
                          a list of languages to build native libraries.
                          Supported languages include "python" and "java".
                          If not specified, only python library will be built.
  -p|--python  mypython   which python executable (default: result of "which python")
EOF
}

# Determine how many parallel jobs to use for make based on the number of cores
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=1
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo "Unrecognized platform."
  exit 1
fi

RAY_BUILD_PYTHON="YES"
RAY_BUILD_JAVA="NO"
PYTHON_EXECUTABLE=""
BUILD_DIR=""

# Parse options
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -l|--language)
      LANGUAGE="$2"
      RAY_BUILD_PYTHON="NO"
      RAY_BUILD_JAVA="NO"
      if [[ "$LANGUAGE" == *"python"* ]]; then
        RAY_BUILD_PYTHON="YES"
      fi
      if [[ "$LANGUAGE" == *"java"* ]]; then
        RAY_BUILD_JAVA="YES"
      fi
      if [ "$RAY_BUILD_PYTHON" == "NO" ] && [ "$RAY_BUILD_JAVA" == "NO" ]; then
        echo "Unrecognized language: $LANGUAGE"
        exit -1
      fi
      shift
      ;;
    -p|--python)
      PYTHON_EXECUTABLE="$2"
      shift
      ;;
    *)
      echo "ERROR: unknown option \"$key\""
      echo
      usage
      exit -1
      ;;
  esac
  shift
done

if [[ -z  "$PYTHON_EXECUTABLE" ]]; then
  PYTHON_EXECUTABLE=$(which python)
fi

PYTHON_VERSION=`"$PYTHON_EXECUTABLE" -c 'import sys; version=sys.version_info[:3]; print("{0}.{1}".format(*version))'`
found=
for allowed in ${SUPPORTED_PYTHONS[@]}
do
  if [[ "$PYTHON_VERSION" == $allowed ]]
  then
    found=$allowed
    break
  fi
done
if [[ -z $found ]]
then
  cat <<EOF
ERROR: Detected Python version $PYTHON_VERSION, which is not supported.
       Please use version 3.6 or 3.7.
EOF
  exit 1
fi

echo "Using Python executable $PYTHON_EXECUTABLE."

# Find the bazel executable. The script ci/travis/install-bazel.sh doesn't
# always put the bazel executable on the PATH.
BAZEL_EXECUTABLE=$(PATH="$PATH:$HOME/.bazel/bin" which bazel)
echo "Using Bazel executable $BAZEL_EXECUTABLE."

# Now we build everything.
BUILD_DIR="$ROOT_DIR/build/"
if [ ! -d "${BUILD_DIR}" ]; then
mkdir -p "${BUILD_DIR}"
fi

pushd "$BUILD_DIR"

# The following line installs pyarrow from S3, these wheels have been
# generated from https://github.com/ray-project/arrow-build from
# the commit listed in the command.
if [ -z "$SKIP_THIRDPARTY_INSTALL" ]; then
    "$PYTHON_EXECUTABLE" -m pip install -q \
        --target="$ROOT_DIR/python/ray/pyarrow_files" pyarrow==0.14.0.RAY \
        --find-links https://s3-us-west-2.amazonaws.com/arrow-wheels/3a11193d9530fe8ec7fdb98057f853b708f6f6ae/index.html
fi

WORK_DIR=`mktemp -d`
pushd $WORK_DIR
git clone https://github.com/suquark/pickle5-backport
pushd pickle5-backport
  git checkout 43551fbb9add8ac2e8551b96fdaf2fe5a3b5997d
  "$PYTHON_EXECUTABLE" setup.py bdist_wheel
  unzip -o dist/*.whl -d "$ROOT_DIR/python/ray/pickle5_files"
popd
popd


if [ -z "$SKIP_THIRDPARTY_INSTALL" ]; then
    "$PYTHON_EXECUTABLE" -m pip install -q psutil setproctitle \
            --target="$ROOT_DIR/python/ray/thirdparty_files"
fi

export PYTHON3_BIN_PATH="$PYTHON_EXECUTABLE"

if [ "$RAY_BUILD_JAVA" == "YES" ]; then
  "$BAZEL_EXECUTABLE" build //java:ray_java_pkg --verbose_failures
fi

if [ "$RAY_BUILD_PYTHON" == "YES" ]; then
  "$BAZEL_EXECUTABLE" build //:ray_pkg --verbose_failures
fi

popd
