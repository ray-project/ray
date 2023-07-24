#!/bin/bash
# shellcheck disable=SC2086

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

mkdir -p $ORIG_MODEL_DIR $TUNED_MODEL_DIR $IMAGES_REG_DIR $IMAGES_OWN_DIR $IMAGES_NEW_DIR

# Unique token to identify our subject (e.g., a random dog vs. our unqtkn dog)
export UNIQUE_TOKEN="unqtkn"

# Step 1
# Only uncomment one of the following:

# Option 1: Use the dog dataset ---------
export CLASS_NAME="dog"
python download_example_dataset.py ./images/dog
export INSTANCE_DIR=./images/dog
# ---------------------------------------

# Option 2: Use the lego car dataset ----
# export CLASS_NAME="car"
# export INSTANCE_DIR=./images/lego-car
# ---------------------------------------

# Option 3: Use your own images ---------
# export CLASS_NAME="<class-of-your-subject>"
# export INSTANCE_DIR="/path/to/images/of/subject"
# ---------------------------------------

# Copy own images into IMAGES_OWN_DIR
cp -rf $INSTANCE_DIR/* "$IMAGES_OWN_DIR/"

# Step 2
python cache_model.py --model_dir=$ORIG_MODEL_DIR --model_name=$ORIG_MODEL_NAME --revision=$ORIG_MODEL_HASH

# Clear reg dir
rm -rf "$IMAGES_REG_DIR"/*.jpg

# Step 3: START
python generate.py \
  --model_dir=$ORIG_MODEL_PATH \
  --output_dir=$IMAGES_REG_DIR \
  --prompts="photo of a $CLASS_NAME" \
  --num_samples_per_prompt=200 \
  --use_ray_data
# Step 3: END

# Step 4: START
python train.py \
  --model_dir=$ORIG_MODEL_PATH \
  --output_dir=$TUNED_MODEL_DIR \
  --instance_images_dir=$IMAGES_OWN_DIR \
  --instance_prompt="photo of $UNIQUE_TOKEN $CLASS_NAME" \
  --class_images_dir=$IMAGES_REG_DIR \
  --class_prompt="photo of a $CLASS_NAME" \
  --train_batch_size=2 \
  --lr=5e-6 \
  --max_train_steps=800
# Step 4: END

# Clear new dir
rm -rf "$IMAGES_NEW_DIR"/*.jpg

# ATTN: Reduced the number of samples per prompt for faster testing
# Step 5: START
python generate.py \
  --model_dir=$TUNED_MODEL_DIR \
  --output_dir=$IMAGES_NEW_DIR \
  --prompts="photo of a $UNIQUE_TOKEN $CLASS_NAME" \
  --num_samples_per_prompt=5
# Step 5: END

# Save artifact
mkdir -p /tmp/artifacts
cp -f "$IMAGES_NEW_DIR"/0-*.jpg /tmp/artifacts/example_out.jpg

# Exit
popd || true
