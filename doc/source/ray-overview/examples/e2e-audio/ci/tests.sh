#!/bin/bash

set -euxo pipefail

jupyter nbconvert --to script README.ipynb  # Jupyter will convert even non-python code logic
ipython README.py  # Use ipython to ensure even non-python cells are executed properly
rm README.py  # Remove the generated script
