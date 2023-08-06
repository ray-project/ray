#!/bin/bash
# This script is used to setup test environment for running core tests.

set -exo pipefail

python3 -m pip install -U \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/ml/dl-cpu-requirements.txt
