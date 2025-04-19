#!/bin/bash

# Function to download the first N items from an S3 directory to a local path
# Parameters:
# 1. S3 directory path
# 2. Local output path
# 3. Number of items to download
download_from_s3() {
    local S3_DIR=$1
    local LOCAL_PATH=$2
    local NUM_ITEMS=$3

    # Ensure the local path exists
    mkdir -p "$LOCAL_PATH"

    echo "Listing items in $S3_DIR..."
    # List all items in the S3 directory
    ITEMS=$(aws s3 ls "$S3_DIR" | head -n "$NUM_ITEMS" | awk '{print $NF}')

    # Check if we found any items
    if [ -z "$ITEMS" ]; then
        echo "No items found in $S3_DIR"
        return 1
    fi

    echo "Found items. Will download the first $NUM_ITEMS."

    # Download each item
    for ITEM in $ITEMS; do
        echo "Downloading $S3_DIR$ITEM to $LOCAL_PATH$ITEM"

        # Check if item is a file or directory
        if [[ "$ITEM" == */ ]]; then
            # It's a directory
            mkdir -p "$LOCAL_PATH$ITEM"
            aws s3 cp --recursive "$S3_DIR$ITEM" "$LOCAL_PATH$ITEM"
        else
            # It's a file
            aws s3 cp "$S3_DIR$ITEM" "$LOCAL_PATH$ITEM"
        fi

        # Check if the download was successful
        if [ $? -ne 0 ]; then
            echo "Error downloading $S3_DIR$ITEM"
        else
            echo "Successfully downloaded $S3_DIR$ITEM"
        fi
    done

    echo "Download complete. Downloaded items to $LOCAL_PATH"
}

download_from_s3 s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC/train/ /mnt/local_storage/imagenet/train/ 20  # 26k images
download_from_s3 s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC/train/ /mnt/local_storage/imagenet/val/ 2
