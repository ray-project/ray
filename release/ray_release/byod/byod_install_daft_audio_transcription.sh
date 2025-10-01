#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

# TODO: Pin these versions.
pip3 install --no-cache-dir daft==0.6.2 numpy==1.26.4 accelerate==1.10.1 transformers==4.56.2 torchaudio==2.7.0 soundfile==0.13.1
