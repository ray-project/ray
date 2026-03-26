#!/bin/bash

set -euo pipefail

pip install pyrefly==0.51.0

find python/ray/data -name '*.py' \
  | grep -vFf ci/lint/pyrefly-excluded-files.txt \
  | xargs pyrefly check
