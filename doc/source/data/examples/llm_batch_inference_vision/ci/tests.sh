#!/bin/bash

# Install requirements first (done by CI automatically): 
# release/ray_release/byod/byod_llm_batch_inference_vision.sh

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

python ci/nb2py.py "content/README.ipynb" "content/README.py" --ignore-cmds
python "content/README.py"
rm "content/README.py"

