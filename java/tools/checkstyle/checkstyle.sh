#!/usr/bin/env bash

COMMAND="{command}"
ALLOW_FAILURE="{allow_failure}"

echo "ALLOW_FAILURE IS $ALLOW_FAILURE"

source ~/.bash_profile

${COMMAND}
result=$?

if [[ ${ALLOW_FAILURE} -eq 1 ]]; then
  exit 0
fi

exit ${result}
