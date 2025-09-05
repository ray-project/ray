#!/bin/bash

# don't use nbcovert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden

for nb in 01-Batch-Inference 02-Distributed-Training 03-Online-Serving; do
  python ci/nb2py.py notebooks/${nb}.ipynb notebooks/${nb}.py  # convert notebook to script
  (cd notebooks && python ${nb}.py)  # run generated script
  (cd notebooks && rm ${nb}.py)  # remove the generated script
done
