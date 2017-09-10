#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

CATAPULT_COMMIT=33a9271eb3cf5caf925293ec6a4b47c94f1ac968
CATAPULT_HOME=$TP_DIR/catapult
VULCANIZE_BIN=$CATAPULT_HOME/tracing/bin/vulcanize_trace_viewer

CATAPULT_FILES=$TP_DIR/../../python/ray/core/src/catapult_files

# This is where we will copy the files that need to be packaged with the wheels.
mkdir -p $CATAPULT_FILES

if [ ! type python2 > /dev/null ]; then
  echo "cannot properly set up UI without a python2 executable"
  if [ "$INCLUDE_UI" == "1" ]; then
    # Since the UI is explicitly supposed to be included, fail here.
    exit 1
  else
    # Let installation continue without building the UI.
    exit 0
  fi
fi

# Download catapult and use it to autogenerate some static html if it isn't
# already present.
if [ ! -d $CATAPULT_HOME ]; then
  echo "setting up catapult"
  # Clone the catapult repository.
  git clone https://github.com/catapult-project/catapult.git $CATAPULT_HOME
  # Check out the appropriate commit from catapult.
  pushd $CATAPULT_HOME
    git checkout $CATAPULT_COMMIT
  popd

  python2 $VULCANIZE_BIN --config chrome --output $CATAPULT_FILES/trace_viewer_full.html
  cp $CATAPULT_HOME/tracing/bin/index.html $CATAPULT_FILES/index.html
fi
