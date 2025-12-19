#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

pip3 install torch==2.6.0 \
  torchrec==1.1.0 \
  fbgemm-gpu==1.1.0 \
  --extra-index-url https://download.pytorch.org/whl/cu121
