#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

# tensorflow doesn't have 3.14 support (latest version 2.21)
pip3 install --no-cache-dir tf-nightly
