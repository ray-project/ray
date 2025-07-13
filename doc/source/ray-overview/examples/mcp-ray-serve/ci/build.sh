#!/bin/bash

set -exo pipefail

# Python dependencies
pip3 install --no-cache-dir \
    "mcp==1.11.0"

# Podman (used in stdio examples)
sudo apt-get update && sudo apt-get install -y podman
