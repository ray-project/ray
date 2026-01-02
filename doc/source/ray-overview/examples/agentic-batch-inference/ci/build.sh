#!/bin/bash

set -euxo pipefail

# Python dependencies
pip3 install --no-cache-dir \
    "fastapi==0.115.12" \
    "openai==2.7.2" \
    "httpx==0.27.1"