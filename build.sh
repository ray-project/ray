#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

# As the supported Python versions change, edit this array:
SUPPORTED_PYTHONS=( "3.5" "3.6" "3.7" "3.8" )

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
elif [[ "${OSTYPE}" == "msys" ]]; then
  PARALLEL="${NUMBER_OF_PROCESSORS-1}"
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
if [ -z "${BAZEL_EXECUTABLE-}" ]; then
  BAZEL_EXECUTABLE=$(PATH="$PATH:$HOME/.bazel/bin" which bazel)
fi
if [ -f "${BAZEL_EXECUTABLE}" ]; then
  echo "Using Bazel executable $BAZEL_EXECUTABLE."
else
  echo "Bazel not found: BAZEL_EXECUTABLE=\"${BAZEL_EXECUTABLE}\""
  exit 1
fi

# Now we build everything.
BUILD_DIR="$ROOT_DIR/build/"
if [ ! -d "${BUILD_DIR}" ]; then
  mkdir -p "${BUILD_DIR}"
fi

pushd "$BUILD_DIR"


if [ "$RAY_BUILD_JAVA" == "YES" ]; then
  "$BAZEL_EXECUTABLE" build ${ENABLE_ASAN-} //java:ray_java_pkg --verbose_failures
fi

if [ "$RAY_BUILD_PYTHON" == "YES" ]; then
  pickle5_available=0
  pickle5_path="$ROOT_DIR/python/ray/pickle5_files"
  # Check if the current Python alrady has pickle5 (either comes with newer Python versions, or has been installed by us before).
  check_pickle5_command="import sys\nif sys.version_info < (3, 8, 2): import pickle5;"
  if PYTHONPATH="$pickle5_path:$PYTHONPATH" "$PYTHON_EXECUTABLE" -s -c "exec(\"$check_pickle5_command\")" 2>/dev/null; then
    pickle5_available=1
  fi
  if [ 1 -ne "${pickle5_available}" ]; then
    # Install pickle5-backport.
    TEMP_DIR="$(mktemp -d)"
    pushd "$TEMP_DIR"
    curl -f -s -L -R -o "pickle5-backport.zip" "https://github.com/pitrou/pickle5-backport/archive/c0c1a158f59366696161e0dffdd10cfe17601372.zip"
    unzip -q pickle5-backport.zip
    pushd pickle5-backport-c0c1a158f59366696161e0dffdd10cfe17601372
      CC=gcc "$PYTHON_EXECUTABLE" setup.py --quiet bdist_wheel
      unzip -q -o dist/*.whl -d "$pickle5_path"
    popd
    popd
    rm -rf "$TEMP_DIR"
  fi

  if [ -z "$SKIP_THIRDPARTY_INSTALL" ]; then
      CC=gcc "$PYTHON_EXECUTABLE" -m pip install -q psutil setproctitle \
              --target="$ROOT_DIR/python/ray/thirdparty_files"
  fi

  export PYTHON3_BIN_PATH="$PYTHON_EXECUTABLE"

  "$BAZEL_EXECUTABLE" build ${ENABLE_ASAN-} //:ray_pkg --verbose_failures
fi

popd
