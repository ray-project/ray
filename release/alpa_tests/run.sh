#!/bin/bash

# Integration test for Alpa and Ray.

# Exit if any of the test commands fail.
set -x -e pipeline

TRAIN_FILE=https://air-example-data-2.s3.us-west-2.amazonaws.com/alpa/alllines.txt
S3_MODEL_DIR=s3://air-example-data-2/alpa/opt/models/models--facebook--opt-2.7b/
LOCAL_MODEL_DIR=/tmp/opt-2.7b/
OUTPUT_DIR=/tmp/alpa_outputs/

mkdir -p $LOCAL_MODEL_DIR
mkdir -p $OUTPUT_DIR

# Download weights and tokenizer.
aws s3 sync $S3_MODEL_DIR $LOCAL_MODEL_DIR

# Run training.
python train_opt_2_7b_minimum.py \
    --operator_parallel 1 \
    --pipeline_parallel 4 \
    --model_name_or_path $LOCAL_MODEL_DIR \
    --output_dir $OUTPUT_DIR \
    --train_file $TRAIN_FILE \
    --max_train_samples 100
