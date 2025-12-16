#!/bin/bash

set -exo pipefail

pip3 install --no-cache-dir \
    "ray[serve-async-inference]>=2.52.0" \
    "requests>=2.31.0" \
    "PyPDF2>=3.0.0" \
    "celery[redis]>=5.4.0"
