#!/bin/bash

set -exo pipefail

sudo apt-get update -y
sudo apt-get install --no-install-recommends -y libgl1-mesa-glx libmagic1 poppler-utils tesseract-ocr libreoffice
sudo rm -f /etc/apt/sources.list.d/*

# Install runtime deps
pip install "unstructured[all-docs]==0.18.21"
pip install --force-reinstall --no-cache-dir pandas==2.3.3
