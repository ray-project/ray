#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

set -exo pipefail

# Limit the dataset size to 10000 images for faster testing.
export LARGE_DATASET_LIMIT=10000

python ci/nb2py.py "content/llm_batch_inference_vision.ipynb" "content/llm_batch_inference_vision.py" --ignore-cmds
python "content/llm_batch_inference_vision.py"
rm "content/llm_batch_inference_vision.py"

