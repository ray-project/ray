#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

# TODO: Pin these versions.
pip3 install --no-cache-dir daft==0.6.2 transformers Pillow pybase64 torch
