#!/bin/bash

# Test script for model multiplexing forecast example
# Converts notebook to Python and runs it

set -exo pipefail

# Convert and run the notebook
python ci/nb2py.py "content/README.ipynb" "content/README.py"
python "content/README.py"
rm "content/README.py"
