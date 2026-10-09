#!/bin/bash
# shellcheck disable=SC2086

set -xe

# Step 0
pushd dreambooth || true

# Step 0 cont
# __preparation_start__
# TODO: If running on multiple nodes, change this path to a shared directory (ex: NFS)
export DATA_PREFIX="/tmp"
export ORIG_MODEL_NAME="CompVis/stable-diffusion-v1-4"
export ORIG_MODEL_HASH="b95be7d6f134c3a9e62ee616f310733567f069ce"
export ORIG_MODEL_DIR="$DATA_PREFIX/model-orig"
export ORIG_MODEL_PATH="$ORIG_MODEL_DIR/models--${ORIG_MODEL_NAME/\//--}/snapshots/$ORIG_MODEL_HASH"
export TUNED_MODEL_DIR="$DATA_PREFIX/model-tuned"
export IMAGES_REG_DIR="$DATA_PREFIX/images-reg"
export IMAGES_OWN_DIR="$DATA_PREFIX/images-own"
export IMAGES_NEW_DIR="$DATA_PREFIX/images-new"
# TODO: Add more worker nodes and increase NUM_WORKERS for more data-parallelism
export NUM_WORKERS=2

mkdir -p $ORIG_MODEL_DIR $TUNED_MODEL_DIR $IMAGES_REG_DIR $IMAGES_OWN_DIR $IMAGES_NEW_DIR
# __preparation_end__

# Unique token to identify our subject (e.g., a random dog vs. our unqtkn dog)
export UNIQUE_TOKEN="unqtkn"

skip_image_setup=false
use_lora=false
# parse args
for arg in "$@"; do
  case $arg in
    --skip_image_setup)
      echo "Option --skip_image_setup is set"
      skip_image_setup=true
      ;;
    --lora)
      echo "Option --lora is set"
      use_lora=true
      ;;
    *)
      echo "Invalid option: $arg"
      ;;
  esac
done

# Step 1
# __cache_model_start__
python cache_model.py --model_dir=$ORIG_MODEL_DIR --model_name=$ORIG_MODEL_NAME --revision=$ORIG_MODEL_HASH
# __cache_model_end__

download_image() {
  # Step 2
  # __supply_own_images_start__
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
  # __supply_own_images_end__

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
}

# Skip step 2 and 3 if skip_image_setup=true

if $skip_image_setup; then
  echo "Skipping image downloading..."
else
  download_image
fi

if [ "$use_lora" = false ]; then
  echo "Start full-finetuning..."
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
    --num_epochs=4 \
    --max_train_steps=200 \
    --num_workers $NUM_WORKERS
  # Step 4: END
else
  echo "Start LoRA finetuning..."
  python train.py \
  --use_lora \
  --model_dir=$ORIG_MODEL_PATH \
  --output_dir=$TUNED_MODEL_DIR \
  --instance_images_dir=$IMAGES_OWN_DIR \
  --instance_prompt="photo of $UNIQUE_TOKEN $CLASS_NAME" \
  --class_images_dir=$IMAGES_REG_DIR \
  --class_prompt="photo of a $CLASS_NAME" \
  --train_batch_size=2 \
  --lr=1e-4 \
  --num_epochs=4 \
  --max_train_steps=200 \
  --num_workers $NUM_WORKERS
fi

# Clear new dir
rm -rf "$IMAGES_NEW_DIR"/*.jpg

if [ "$use_lora" = false ]; then
  # Step 5: START
  python generate.py \
    --model_dir=$TUNED_MODEL_DIR \
    --output_dir=$IMAGES_NEW_DIR \
    --prompts="photo of a $UNIQUE_TOKEN $CLASS_NAME in a bucket" \
    --num_samples_per_prompt=5
  # Step 5: END
else
  python generate.py \
  --model_dir=$ORIG_MODEL_PATH \
  --lora_weights_dir=$TUNED_MODEL_DIR \
  --output_dir=$IMAGES_NEW_DIR \
  --prompts="photo of a $UNIQUE_TOKEN $CLASS_NAME in a bucket" \
  --num_samples_per_prompt=5
fi

# Save artifact
mkdir -p /tmp/artifacts
cp -f "$IMAGES_NEW_DIR"/0-*.jpg /tmp/artifacts/example_out.jpg

# Exit
popd || true
