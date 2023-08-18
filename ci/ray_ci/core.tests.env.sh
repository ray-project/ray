#!/bin/bash
# This script is used to setup test environment for running core tests.

set -exo pipefail

apt-get install -y graphviz openjdk-8-jdk tmux gdb

pip install -U --ignore-installed  \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/ml/dl-cpu-requirements.txt
