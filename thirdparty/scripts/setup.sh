#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
TP_DIR=$TP_SCRIPT_DIR/..

mkdir -p $TP_DIR/build
mkdir -p $TP_DIR/pkg

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

LANGUAGE="python"
if [[ -n  "$2" ]]; then
  LANGUAGE=$2
fi
echo "Build language is $LANGUAGE."

unamestr="$(uname)"

##############################################
# boost
##############################################
bash "$TP_SCRIPT_DIR/build_boost.sh"

##############################################
# redis
##############################################
bash "$TP_SCRIPT_DIR/build_redis.sh"

##############################################
# credis
##############################################
bash "$TP_SCRIPT_DIR/build_credis.sh"

##############################################
# flatbuffers if necessary
##############################################
if [[ "$unamestr" == "Linux" ]]; then
  echo "building flatbuffers"
  bash "$TP_SCRIPT_DIR/build_flatbuffers.sh"
fi

##############################################
# arrow
##############################################
bash "$TP_SCRIPT_DIR/build_arrow.sh" $PYTHON_EXECUTABLE $LANGUAGE

##############################################
# parquet (skipped as it is inlined in build_arrow.sh)
##############################################
# bash "$TP_SCRIPT_DIR/build_parquet.sh"

##############################################
# catapult
##############################################
# Clone catapult and build the static HTML needed for the UI.
bash "$TP_SCRIPT_DIR/build_ui.sh"

##############################################
# rDSN (optional)
##############################################
# bash "$TP_SCRIPT_DIR/build_rdsn.sh"
