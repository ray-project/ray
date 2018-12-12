#!/bin/bash
set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function usage() {
  echo "Usage: collect_dependent_libs.sh [<args>]"
  echo
  echo "Options:"
  echo "  -h|--help               print the help info"
  echo "  -d|--target-dir         the target directory to put all the thirdparty libs"
  echo "  -n|--no-build           do not build ray, used in case that ray is already built"
  echo "  -r|--resource           the resource file name (default: resource.txt)"
  echo
}

# By default all the libs will be put to ./thirdparty/external_project_libs.
# However, this directory could be cleaned by `git clean`.
# Users can provide another directory using -d option.
DIR="$ROOT_DIR/thirdparty/external_project_libs"
# By default ray will be build before copying the libs.
# Users can diable the building process if they have built ray.
BUILD="YES"

RESOURCE="resource.txt"

# Parse options
while [[ $# > 0 ]]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -d|--target-dir)
      DIR="$2"
      shift
      ;;
    -n|--no-build)
      BUILD="NO"
      ;;
    -r|--resource)
      RESOURCE="$2"
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

echo "External project libs will be put to $DIR"
if [ ! -d "$DIR" ]; then
  mkdir -p $DIR
fi

pushd $ROOT_DIR
if [ "$BUILD" = "YES" ]; then
  echo "Build Ray First."
  ./build.sh
fi

RAY_BUILD_DIR=$ROOT_DIR/build/external/
ARROW_BUILD_DIR=$ROOT_DIR/build/external/arrow/src/arrow_ep-build/

function cp_one_lib() {
  if [[ ! -d "$1" ]]; then
    echo "Lib root dir $1 does not exist!"
    exit -1
  fi
  if [[ ! -d "$1/include" ]]; then
    echo "Lib inlcude dir $1 does not exist!"
    exit -1
  fi
  if [[ ! -d "$1/lib" && ! -d "$1/lib64" ]]; then
    echo "Lib dir $1 does not exist!"
    exit -1
  fi
  cp -rf $1 $DIR
}

# copy libs that ray needs.
cp_one_lib $RAY_BUILD_DIR/boost-install
cp_one_lib $RAY_BUILD_DIR/flatbuffers-install
cp_one_lib $RAY_BUILD_DIR/glog-install
cp_one_lib $RAY_BUILD_DIR/googletest-install

# copy libs that arrow needs.
cp_one_lib $ARROW_BUILD_DIR/snappy_ep/src/snappy_ep-install
cp_one_lib $ARROW_BUILD_DIR/thrift_ep/src/thrift_ep-install

# generate the export script.
echo "Output the exporting resource file to $DIR/$RESOURCE."
echo "export BOOST_ROOT=$DIR/boost-install" > $DIR/$RESOURCE
echo "export RAY_BOOST_ROOT=\$BOOST_ROOT" >> $DIR/$RESOURCE

echo "export FLATBUFFERS_HOME=$DIR/flatbuffers-install" >> $DIR/$RESOURCE
echo "export RAY_FLATBUFFERS_HOME=\$FLATBUFFERS_HOME" >> $DIR/$RESOURCE

echo "export GTEST_HOME=$DIR/googletest-install" >> $DIR/$RESOURCE
echo "export RAY_GTEST_HOME=\$GTEST_HOME" >> $DIR/$RESOURCE

echo "export GLOG_HOME=$DIR/glog-install" >> $DIR/$RESOURCE
echo "export RAY_GLOG_HOME=\$GLOG_HOME" >> $DIR/$RESOURCE

echo "export THRIFT_HOME=$DIR/snappy_ep-install" >> $DIR/$RESOURCE
echo "export SNAPPY_HOME=$DIR/thrift_ep-install" >> $DIR/$RESOURCE

popd
