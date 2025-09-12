#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

pip3 install --no-cache-dir --upgrade-strategy only-if-needed sentence-transformers==5.0.1 torch==2.8.0
