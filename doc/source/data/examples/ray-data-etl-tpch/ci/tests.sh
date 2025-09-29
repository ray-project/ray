#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

python ci/nb2py.py README.ipynb README.py
python README.py
rm README.py
