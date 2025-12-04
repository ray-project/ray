#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

python ci/nb2py.py content/unstructured_data_ingestion.ipynb unstructured_data_ingestion.py
python unstructured_data_ingestion.py
rm unstructured_data_ingestion.py
