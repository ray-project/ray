#!/bin/bash
set -exuo pipefail

ls -a

uv pip install -r requirements.in -c "$HOME"/python_depset.lock --system --no-deps --index-strategy unsafe-best-match

python ci/nb2py.py README.ipynb README.py  # convert notebook to py script
python README.py  # be sure to use ipython to ensure even non-python cells are executed properly
rm README.py  # remove the generated script