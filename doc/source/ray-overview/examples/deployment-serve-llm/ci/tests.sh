#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden.

set -exo pipefail

for nb in \
  "small-size-llm/README" \
  "medium-size-llm/README" \
  "vision-llm/README" \
  "reasoning-llm/README" \
  "hybrid-reasoning-llm/README"
do
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py" --ignore-cmds
  python "${nb}.py"
  rm "${nb}.py"
done
