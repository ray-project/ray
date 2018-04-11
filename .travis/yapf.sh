#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $ROOT_DIR/../test
  find . -name '*.py' -type f -exec yapf --style=pep8 -i -r {} \;
popd

pushd $ROOT_DIR/../python
  find . -name '*.py' -type f -not -path './ray/dataframe/*' -not -path './ray/rllib/*' -not -path './ray/cloudpickle/*' -exec yapf --style=pep8 -i -r {} \;
popd

CHANGED_FILES=(`git diff --name-only`)
if [ "$CHANGED_FILES" ]; then
  echo 'Reformatted staged files. Please review and stage the changes.'
  echo
  echo 'Files updated:'
  for file in ${CHANGED_FILES[@]}; do
    echo "  $file"
  done
  exit 1
else
  exit 0
fi
