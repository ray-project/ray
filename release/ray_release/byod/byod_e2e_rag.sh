#!/bin/bash

set -exo pipefail

# Install system dependencies
sudo apt-get update && \
    sudo apt-get install -y libgl1-mesa-glx libmagic1 poppler-utils tesseract-ocr libreoffice && \
    sudo rm -rf /var/lib/apt/lists/*

# Install python dependencies
pip3 install --no-cache-dir \
    "unstructured[all-docs]==0.16.23" \
    "sentence-transformers==3.4.1" \
    "chromadb==0.6.3" \
    "langchain_text_splitters==0.3.6" \
    "pandas==2.2.3" \
    "tiktoken==0.9.0"
