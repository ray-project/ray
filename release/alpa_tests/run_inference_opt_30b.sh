#!/bin/bash

# Integration test for Alpa and Ray.

# Exit if any of the test commands fail.
set -x -e -o pipefail

# Parse command line args
STORAGE_PROVIDER="aws"

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        --storage)
            STORAGE_PROVIDER="$2"
            shift
            shift
            ;;
        *)  # Unknown option
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ "$STORAGE_PROVIDER" != "aws" ] && [ "$STORAGE_PROVIDER" != "gcs" ]; then
    echo "Invalid storage provider: $STORAGE_PROVIDER"
    exit 1
fi

S3_MODEL_DIR=s3://air-example-data-2/alpa/opt/models/models--facebook--opt-30b/
GS_MODEL_DIR=gs://air-example-data/alpa/opt/models/models--facebook--opt-30b/

LOCAL_MODEL_DIR=/tmp/opt-30b/

mkdir -p $LOCAL_MODEL_DIR

# Download weights and tokenizer. Excluding the original
# FLAX weights. We only need the alpa converted np weights
# for this test.
if [ "$STORAGE_PROVIDER" = "aws" ]; then
  aws s3 sync $S3_MODEL_DIR $LOCAL_MODEL_DIR --exclude="*.msgpack"
else
  gsutil rsync -r -x ".*\.msgpack" $GS_MODEL_DIR $LOCAL_MODEL_DIR
fi

# Run training.
python inference_opt_30b.py --model_dir $LOCAL_MODEL_DIR
