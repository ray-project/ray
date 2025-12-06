#!/bin/bash
set -exo pipefail

python ci/nb2py.py "content/notebook.ipynb" "content/notebook.py"
python "content/notebook.py"
rm "content/notebook.py"

