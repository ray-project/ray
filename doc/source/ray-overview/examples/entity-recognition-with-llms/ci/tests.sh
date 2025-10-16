#!/bin/bash

set -euxo pipefail

python ci/nb2py.py README.ipynb README.py  # convert notebook to py script
python README.py  # be sure to use ipython to ensure even non-python cells are executed properly
rm README.py  # remove the generated script
