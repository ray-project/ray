#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

pip3 install --no-cache-dir daft==0.6.2 numpy==1.26.4 accelerate==1.10.1 transformers==4.56.2 sentence-transformers==5.1.1 langchain==0.3.27 pymupdf==1.26.4
