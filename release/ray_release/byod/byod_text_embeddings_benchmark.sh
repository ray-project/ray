#!/bin/bash

set -exo pipefail

# Install Python dependencies
python3 -m pip install --no-cache-dir \
    "unstructured[all-docs]" \
    "sentence-transformers" \
    "langchain_text_splitters" \
    "pandas" \
    "tiktoken" \
    "vllm==0.7.2" \
    "xgrammar==0.1.11" \
    "pynvml==12.0.0"
