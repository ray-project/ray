#!/bin/bash

set -exo pipefail

# Python dependencies
pip3 install --no-cache-dir \
    "mcp==1.8.0" \
    "asyncio==3.4.3"

# Podman (used in stdio examples)
sudo apt-get update && sudo apt-get install -y podman
