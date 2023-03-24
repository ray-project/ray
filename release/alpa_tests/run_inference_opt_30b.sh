#!/bin/bash

# Integration test for Alpa and Ray.

# Exit if any of the test commands fail.
set -x -e pipeline

S3_MODEL_DIR=s3://air-example-data-2/alpa/opt/models/models--facebook--opt-30b/
LOCAL_MODEL_DIR=/tmp/opt-30b/

mkdir -p $LOCAL_MODEL_DIR

# Download weights and tokenizer. Excluding the original
# FLAX weights. We only need the alpa converted np weights
# for this test.
aws s3 sync $S3_MODEL_DIR $LOCAL_MODEL_DIR --exclude="*.msgpack"

# Run training.
python inference_opt_30b.py --model_dir $LOCAL_MODEL_DIR
