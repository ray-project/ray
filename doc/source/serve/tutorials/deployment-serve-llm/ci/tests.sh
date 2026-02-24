#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

for nb in \
  "content/small-size-llm/README" \
  "content/medium-size-llm/README" \
  "content/large-size-llm/README" \
  "content/vision-llm/README" \
  "content/reasoning-llm/README" \
  "content/hybrid-reasoning-llm/README" \
  "content/gpt-oss/README" 
do
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py" --ignore-cmds
  python "${nb}.py"
  rm "${nb}.py"
done
