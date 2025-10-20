#!/bin/bash

set -exo pipefail

# Install system dependencies
sudo apt-get update && \
    sudo apt-get install -y libgl1-mesa-glx libmagic1 poppler-utils tesseract-ocr libreoffice && \
    sudo rm -rf /var/lib/apt/lists/*

# Install python dependencies
pip3 install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match
