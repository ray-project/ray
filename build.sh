#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function usage()
{
  echo "Usage: build.sh [<args>]"
  echo
  echo "Options:"
  echo "  -h|--help               print the help info"
  echo "  -l|--language language1[,language2]"
  echo "                          a list of languages to build native libraries."
  echo "                          Supported languages include \"python\" and \"java\"."
  echo "                          If not specified, only python library will be built."
  echo "  -p|--python             which python executable (default from which python)"
  echo
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
    -l|--languags)
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
echo "Using Python executable $PYTHON_EXECUTABLE."

# Find the bazel executable. The script ci/travis/install-bazel.sh doesn't
# always put the bazel executable on the PATH.
BAZEL_EXECUTABLE=$(PATH="$PATH:$HOME/.bazel/bin" which bazel)
echo "Using Bazel executable $BAZEL_EXECUTABLE."

RAY_BUILD_PYTHON=$RAY_BUILD_PYTHON \
RAY_BUILD_JAVA=$RAY_BUILD_JAVA \
bash "$ROOT_DIR/setup_thirdparty.sh" "$PYTHON_EXECUTABLE"

# Now we build everything.
BUILD_DIR="$ROOT_DIR/build/"
if [ ! -d "${BUILD_DIR}" ]; then
mkdir -p "${BUILD_DIR}"
fi

pushd "$BUILD_DIR"

# The following line installs pyarrow from S3, these wheels have been
# generated from https://github.com/ray-project/arrow-build from
# the commit listed in the command.
$PYTHON_EXECUTABLE -m pip install -q \
    --target="$ROOT_DIR/python/ray/pyarrow_files" pyarrow==0.14.0.RAY \
    --find-links https://s3-us-west-2.amazonaws.com/arrow-wheels/516e15028091b5e287200b5df77d77f72d9a6c9a/index.html
export PYTHON_BIN_PATH="$PYTHON_EXECUTABLE"

if [ "$RAY_BUILD_JAVA" == "YES" ]; then
  $BAZEL_EXECUTABLE build //java:all --verbose_failures
fi

if [ "$RAY_BUILD_PYTHON" == "YES" ]; then
  $BAZEL_EXECUTABLE build //:ray_pkg --verbose_failures
fi

popd
