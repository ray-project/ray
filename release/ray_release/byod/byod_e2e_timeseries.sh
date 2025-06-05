#!/bin/bash

set -exo pipefail

# Install Python dependencies.
pip3 install --no-cache-dir numpy pandas scikit-learn torch==2.7.0 aiohttp pyyaml s3fs nbformat
