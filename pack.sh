#!/bin/bash

# Exit on error
set -e

# Create temporary directory for packaging
TEMP_DIR=$(mktemp -d)
ARCHIVE_NAME="ray_release_oct_21.zip"

# Copy autoscaler directory to temp location
cp -r /home/ubuntu/ray-project/ray/python/ray/autoscaler/ "$TEMP_DIR"

# Create zip archive
cd "$TEMP_DIR"
zip -r "$ARCHIVE_NAME" autoscaler/

# Upload to Azure blob storage using azcopy
# Note: Assumes azcopy is installed and Azure credentials are configured
azcopy copy "$ARCHIVE_NAME" "https://rayreleasetests.dfs.core.windows.net/working-dirs/working_dirs/azure_cluster_launcher.v2/$ARCHIVE_NAME"

# Clean up
cd -
rm -rf "$TEMP_DIR"

echo "Successfully uploaded $ARCHIVE_NAME to Azure storage"
