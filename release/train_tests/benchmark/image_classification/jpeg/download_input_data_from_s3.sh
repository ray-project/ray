#!/bin/bash

set -e  # Exit on any error

DATA_DIR="/mnt/local_storage/imagenet"
ZIP_NAME="imagenet-64k.zip"
ZIP_URL="s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC/$ZIP_NAME"
ZIP_PATH="$DATA_DIR/$ZIP_NAME"
TRAIN_DIR="$DATA_DIR/train"

if [ ! -d "$TRAIN_DIR" ]; then
    echo "Downloading and extracting ImageNet subset to $DATA_DIR..."
    mkdir -p "$DATA_DIR"
    pushd "$DATA_DIR" || exit

    echo "Fetching $ZIP_URL..."
    aws s3 cp "$ZIP_URL" "$ZIP_PATH"

    echo "Unzipping..."
    unzip -q "$ZIP_NAME"
    rm "$ZIP_NAME"

    popd || exit
else
    echo "Dataset already exists at $TRAIN_DIR. Skipping download and unzip."
fi

echo "Duplicating images in-place..."

python3 <<EOF
import shutil
from pathlib import Path
from tqdm import tqdm

dataset_dir = Path("$TRAIN_DIR")

for class_dir in tqdm(sorted(dataset_dir.iterdir()), desc="Processing classes"):
    if not class_dir.is_dir():
        continue

    # Skip if already duplicated
    if any(class_dir.glob("*_copy1.JPEG")):
        print(f"Skipping {class_dir.name} (already duplicated)")
        continue

    for img_path in class_dir.glob("*.JPEG"):
        for i in range(1, 8):
            copy_name = img_path.stem + f"_copy{i}" + img_path.suffix
            copy_path = class_dir / copy_name
            shutil.copy2(img_path, copy_path)
EOF

echo "Image duplication complete."
