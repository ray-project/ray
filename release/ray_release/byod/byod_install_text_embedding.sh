#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

pip3 install --no-cache-dir --upgrade-strategy only-if-needed \
  transformers==4.56.2 \
  sentence-transformers==5.1.0 \
  torch==2.8.0
