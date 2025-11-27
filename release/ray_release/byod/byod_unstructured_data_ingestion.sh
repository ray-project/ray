#!/bin/bash

set -exo pipefail

# Install runtime deps
pip install unstructured[pdf]
pip install --force-reinstall --no-cache-dir pandas
