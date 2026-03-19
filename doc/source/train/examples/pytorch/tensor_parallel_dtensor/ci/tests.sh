#!/bin/bash
set -euo pipefail
echo "=== Driver environment before notebook conversion ==="
which python
python -V
if command -v pip >/dev/null 2>&1; then
  which pip
  pip --version
fi
python -m pip --version
python -m pip show torch transformers datasets || true
python ci/nb2py.py README.ipynb README.py  # convert notebook to py script
echo "=== Generated script preamble ==="
sed -n '1,30p' README.py
python README.py  # run the converted python script
rm README.py  # remove the generated script
