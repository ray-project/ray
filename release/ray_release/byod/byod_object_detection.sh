#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the object detection example notebooks.

set -exo pipefail

# Install system dependencies
sudo apt-get update
sudo apt-get install -y \
    libgl1-mesa-glx \
    ffmpeg

# Create cluster storage directory for model files
mkdir -p /mnt/cluster_storage
chmod 777 /mnt/cluster_storage

# Enable Ray Train V2 for notebook compatibility
echo "export RAY_TRAIN_V2_ENABLED=1" >> ~/.bashrc 