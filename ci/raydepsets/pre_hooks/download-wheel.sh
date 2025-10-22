#!/bin/bash

set -euo pipefail

WHEEL_URL=$1
WHEEL_NAME=$(basename "$WHEEL_URL")

echo WHEEL_NAME: "$WHEEL_NAME"
echo WHEEL_URL: "$WHEEL_URL"

curl -Lv "$WHEEL_URL" -o .whl/"$WHEEL_NAME"
