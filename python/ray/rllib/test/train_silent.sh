#!/bin/bash

TMPFILE=`mktemp`
DIRECTORY=`dirname $0`
echo "Testing $@"

$DIRECTORY/../train.py "$@" >$TMPFILE 2>&1
if [ $? != 0 ]; then
    cat $TMPFILE
    echo "FAILED $?"
    exit $?
fi

echo "OK"
