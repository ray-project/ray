#!/bin/bash
# This script is used to setup test environment for running core tests.

set -exo pipefail

pip install -U --ignore-installed \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/ml/dl-cpu-requirements.txt
pip install ray[client]
