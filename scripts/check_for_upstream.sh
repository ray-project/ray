#!/bin/bash

FILE=".UPSTREAM"

check_for_upstream() {
if find . -type f -name "$FILE" -quit | grep -q .; then
    echo "Error: The file '$FILE' exists. You may be attempting to push ray turbo to ray."
    exit 1
fi
}

check_for_upstream "$@"
