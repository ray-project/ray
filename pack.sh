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

# Upload to S3
# Note: Assumes AWS credentials are configured
aws s3 cp "$ARCHIVE_NAME" "s3://ray-release-automation-results/working_dirs/azure_cluster_launcher.v2/$ARCHIVE_NAME"

# Clean up
cd -
rm -rf "$TEMP_DIR"

echo "Successfully uploaded $ARCHIVE_NAME to S3"
