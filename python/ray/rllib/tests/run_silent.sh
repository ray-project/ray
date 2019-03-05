#!/bin/bash

TIMEOUT=30m
TMPFILE=`mktemp`
DIRECTORY=`dirname $0`
SCRIPT=$1
shift

if [ -x $DIRECTORY/../$SCRIPT ]; then
    if which timeout >/dev/null; then
        timeout -k $TIMEOUT $TIMEOUT $DIRECTORY/../$SCRIPT "$@" >$TMPFILE 2>&1
    else
        $DIRECTORY/../$SCRIPT "$@" >$TMPFILE 2>&1
    fi
else
    if which timeout >/dev/null; then
        timeout -k $TIMEOUT $TIMEOUT python $DIRECTORY/../$SCRIPT "$@" >$TMPFILE 2>&1
    else
        python $DIRECTORY/../$SCRIPT "$@" >$TMPFILE 2>&1
    fi
fi

CODE=$?
if [ $CODE != 0 ]; then
    cat $TMPFILE
    echo "FAILED $CODE"
    exit $CODE
fi

exit 0
