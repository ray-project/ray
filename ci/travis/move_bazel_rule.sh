#!/bin/bash
set -e
if [ -z "$1" ]; then
  cat << EOF
Usages: 
  $0 NEW_BAZEL_FILE
    Search for BUILD targets with these names in the git repo
  $0 NEW_BAZEL_FILE FIXUP_BAZEL_FILE
    Find instances of the BUILD targets in the to-be-fixed-file
  $0 NEW_BAZEL_FILE FIXUP_BAZEL_FILE PREFIX
    Find instances of that BUILD target with an old prefix (eg: '//:')
  $0 NEW_BAZEL_FILE FIXUP_BAZEL_FILE PREFIX NEW_PREFIX
    Apply the NEW_PREFIX (eg, a package move) in place of the PREFIX in the FIXUP file.
EOF
  exit
fi
INCOMING_SET=$(grep "name" $1 | cut -d '"' -f 2)
TO_CHANGE=$2
PREVIOUS_PATH=$3
NEW_PATH=$4

for TARGET in $INCOMING_SET; do
  if [ -n "$TO_CHANGE" ]; then
    grep ${PREVIOUS_PATH}${TARGET} $TO_CHANGE
    if [ -n "$NEW_PATH" ]; then
      sed -i "s^${PREVIOUS_PATH}${TARGET}^${NEW_PATH}${TARGET}^g" $TO_CHANGE
    fi
  else
    git grep ${TARGET}
  fi

done
