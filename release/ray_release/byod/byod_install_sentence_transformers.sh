#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

pip3 install --no-cache-dir sentence-transformers==3.4.1 torch==2.6.0 --upgrade-strategy only-if-needed
