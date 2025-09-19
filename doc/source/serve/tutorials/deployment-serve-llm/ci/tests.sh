#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

for nb in \
  "small-size-llm/notebook" \
  "medium-size-llm/notebook" \
  "large-size-llm/notebook" \
  "vision-llm/notebook" \
  "reasoning-llm/notebook" \
  "hybrid-reasoning-llm/notebook"
do
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py" --ignore-cmds
  python "${nb}.py"
  rm "${nb}.py"
done
