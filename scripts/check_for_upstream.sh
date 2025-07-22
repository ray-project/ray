#!/bin/bash

FILE=".UPSTREAM"

check_for_upstream() {
if [ -f "$FILE" ]; then
  echo "Error: The file '$FILE' exists. You are attempting to push ray turbo to ray."
  exit 1
  fi

  exit 0
}

check_for_upstream "$@"
