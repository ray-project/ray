#!/bin/bash

set -euxo pipefail

# don't use nbcovert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

for nb in 1.object_detection_train 2.object_detection_batch_inference_eval 3.video_processing_batch_inference 4.object_detection_serve; do
  # Convert .ipynb → .py (in the current dir)
  python ci/nb2py.py "${nb}.ipynb" "${nb}.py"
  # Run the generated script (also in the current dir)
  python "${nb}.py"
  # Remove the generated .py
  rm "${nb}.py"
done
