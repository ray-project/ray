#!/bin/bash

# ATTN: This should be kept in sync with python/ray/air/examples/dreambooth/README.md

set -xe

# Step 0
pushd dreambooth || true
pip install -Ur requirements.txt

# Step 0 cont
export DATA_PREFIX="/tmp"
export ORIG_MODEL_NAME="CompVis/stable-diffusion-v1-4"
export ORIG_MODEL_HASH="249dd2d739844dea6a0bc7fc27b3c1d014720b28"
export ORIG_MODEL_DIR="$DATA_PREFIX/model-orig"
export ORIG_MODEL_PATH="$ORIG_MODEL_DIR/models--${ORIG_MODEL_NAME/\//--}/snapshots/$ORIG_MODEL_HASH"
export TUNED_MODEL_DIR="$DATA_PREFIX/model-tuned"
export IMAGES_REG_DIR="$DATA_PREFIX/images-reg"
export IMAGES_OWN_DIR="$DATA_PREFIX/images-own"
export IMAGES_NEW_DIR="$DATA_PREFIX/images-new"

export CLASS_NAME="lego car"

mkdir -p $ORIG_MODEL_DIR $TUNED_MODEL_DIR $IMAGES_REG_DIR $IMAGES_OWN_DIR $IMAGES_NEW_DIR

# Copy own images into IMAGES_OWN_DIR
cp -rf ./images/unqtkn/*.jpg "$IMAGES_OWN_DIR/"

# Step 1
python cache_model.py --model_dir=$ORIG_MODEL_DIR --model_name=$ORIG_MODEL_NAME --revision=$ORIG_MODEL_HASH

# Clear reg dir
rm -rf "$IMAGES_REG_DIR"/*.jpg

# Step 2
# ATTN: Reduced the number of samples per prompt for faster testing
python run_model.py \
  --model_dir=$ORIG_MODEL_PATH \
  --output_dir=$IMAGES_REG_DIR \
  --prompts="photo of a $CLASS_NAME" \
  --num_samples_per_prompt=20

# Step 3
python train.py \
  --model_dir=$ORIG_MODEL_PATH \
  --output_dir=$TUNED_MODEL_DIR \
  --instance_images_dir=$IMAGES_OWN_DIR \
  --instance_prompt="a photo of unqtkn $CLASS_NAME" \
  --class_images_dir=$IMAGES_REG_DIR \
  --class_prompt="a photo of a $CLASS_NAME"

# Clear new dir
rm -rf "$IMAGES_NEW_DIR"/*.jpg

# Step 4
# ATTN: Reduced the number of samples per prompt for faster testing
python run_model.py \
  --model_dir=$TUNED_MODEL_DIR \
  --output_dir=$IMAGES_NEW_DIR \
  --prompts="photo of a unqtkn $CLASS_NAME" \
  --num_samples_per_prompt=5

# Save artifact
mkdir -p /tmp/artifacts
cp -f "$IMAGES_NEW_DIR"/0-*.jpg /tmp/artifacts/example_out.jpg

# Exit
popd || true
