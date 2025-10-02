#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

# TODO: Pin these versions.
pip3 install --no-cache-dir daft==0.6.2 numpy==1.26.4 av==15.1.0 ultralytics==8.3.200 pillow==11.3.0 decord==0.6.0
