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
  echo "  -d|--debug              CMAKE_BUILD_TYPE=Debug (default is RelWithDebInfo)"
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
  PARALLEL=$(nproc)
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
if [ "$VALGRIND" = "1" ]; then
  CBUILD_TYPE="Debug"
else
  CBUILD_TYPE="RelWithDebInfo"
fi

# Parse options
while [[ $# > 0 ]]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -d|--debug)
      CBUILD_TYPE=Debug
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
  PYTHON_EXECUTABLE=`which python`
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

RAY_BUILD_PYTHON=$RAY_BUILD_PYTHON \
RAY_BUILD_JAVA=$RAY_BUILD_JAVA \
bash $ROOT_DIR/setup_thirdparty.sh $PYTHON_EXECUTABLE

# Now we build everything.
BUILD_DIR="$ROOT_DIR/build/"
if [ ! -d "${BUILD_DIR}" ]; then
mkdir -p ${BUILD_DIR}
fi

pushd "$BUILD_DIR"
TP_PKG_DIR=$ROOT_DIR/thirdparty/pkg
# We use these variables to set PKG_CONFIG_PATH, which is important so that
# in cmake, pkg-config can find plasma.
ARROW_HOME=$TP_PKG_DIR/arrow/cpp/build/cpp-install
BOOST_ROOT=$TP_PKG_DIR/boost \
PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
cmake -DCMAKE_BUILD_TYPE=$CBUILD_TYPE \
      -DCMAKE_RAY_LANG_JAVA=$RAY_BUILD_JAVA \
      -DCMAKE_RAY_LANG_PYTHON=$RAY_BUILD_PYTHON \
      -DRAY_USE_NEW_GCS=$RAY_USE_NEW_GCS \
      -DPYTHON_EXECUTABLE:FILEPATH=$PYTHON_EXECUTABLE $ROOT_DIR

make clean
make -j${PARALLEL}
popd

# Move stuff from Arrow to Ray.
cp $ROOT_DIR/thirdparty/pkg/arrow/cpp/build/cpp-install/bin/plasma_store_server $BUILD_DIR/src/plasma/
if [[ "$RAY_BUILD_PYTHON" == "YES" ]]; then
  cp $ROOT_DIR/thirdparty/pkg/arrow/cpp/build/cpp-install/bin/plasma_store_server $BUILD_DIR/../python/ray/core/src/plasma/
fi
if [[ "$RAY_BUILD_JAVA" == "YES" ]]; then
  cp $ROOT_DIR/thirdparty/build/arrow/cpp/build/release/libplasma_java.* $BUILD_DIR/src/plasma/
fi
