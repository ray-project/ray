#!/bin/bash
set -exo pipefail

python ci/nb2py.py "content/README.ipynb" "content/README.py"
python "content/README.py"
rm "content/README.py"

