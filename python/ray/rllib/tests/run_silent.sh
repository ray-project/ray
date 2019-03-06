#!/bin/bash

DIRECTORY=`dirname $0`
SUPPRESS_OUTPUT=$DIRECTORY/../../../../ci/suppress_output
SCRIPT=$1
shift

if [ -x $DIRECTORY/../$SCRIPT ]; then
    exec $SUPPRESS_OUTPUT $DIRECTORY/../$SCRIPT "$@"
else
    exec $SUPPRESS_OUTPUT python $DIRECTORY/../$SCRIPT "$@"
fi
