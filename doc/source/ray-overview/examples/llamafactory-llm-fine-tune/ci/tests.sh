#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden.

set -euxo pipefail

for nb in \
  "notebooks/dpo_qlora" \
  "notebooks/kto_lora" \
  "notebooks/sft_lora_deepspeed"
do
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py" --ignore-cmds
  python "${nb}.py"
  rm "${nb}.py"
done
