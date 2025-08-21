#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden.

set -exo pipefail

for nb in \
  "notebooks/small-size-llm" \
  "notebooks/medium-size-llm" \
  "notebooks/vision-llm" \
  "notebooks/reasoning-llm" \
  "notebooks/hybrid-reasoning-llm"
do
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py" --ignore-cmds
  python "${nb}.py"
  rm "${nb}.py"
done
