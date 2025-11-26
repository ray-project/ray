#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

python ci/nb2py.py content/unstructured-data-ingestion.ipynb unstructured-data-ingestion.py
python unstructured-data-ingestion.py
rm unstructured-data-ingestion.py
