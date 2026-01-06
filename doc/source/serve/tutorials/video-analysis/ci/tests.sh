#!/bin/bash

set -euxo pipefail

S3_BUCKET=anyscale-example-video-analysis-test-bucket

export S3_BUCKET

python ci/nb2py.py content/video_analysis_pipeline.ipynb content/video_analysis_pipeline.py # convert notebook to py script
cd content && python video_analysis_pipeline.py  # be sure to use ipython to ensure even non-python cells are executed properly
rm video_analysis_pipeline.py  # remove the generated script
cd ..
