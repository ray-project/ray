#!/bin/bash

# don't use nbcovert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

for nb in object_detection_train 2.object_detection_batch_inference_eval 3.video_processing_batch_inference 4.object_detection_serve; do
  python ci/nb2py.py notebooks/${nb}.ipynb notebooks/${nb}.py  # convert notebook to script
  (cd notebooks && python ${nb}.py)  # run generated script
  (cd notebooks && rm ${nb}.py)  # remove the generated script
done