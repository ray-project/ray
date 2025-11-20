#!/bin/bash

set -exo pipefail

# Install dependencies
pip3 install --no-cache-dir \
    "ray[serve-async-inference]>=2.50.0" \
    "requests>=2.31.0" \
    "PyPDF2>=3.0.0" \
    "celery[redis]>=5.3.0"
