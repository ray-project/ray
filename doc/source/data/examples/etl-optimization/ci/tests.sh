#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

# TODO once runnable on nightly, uncomment these lines to properly test
python ci/nb2py.py content/etl-optimization.ipynb etl-optimization.py
python etl-optimization.py
rm etl-optimization.py
