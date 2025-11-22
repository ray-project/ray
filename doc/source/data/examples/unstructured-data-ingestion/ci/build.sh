#!/bin/bash

set -exo pipefail

# Install runtime deps
pip install "unstructured[all-docs]==0.16.23"