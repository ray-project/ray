#!/usr/bin/env bash
# This script gets newest wheels from S3 and uploads them PyPI

WHEEL_DIR="./.whl"
mkdir -p $WHEEL_DIR

# Clone Ray to get commit hash
git clone https://github.com/ray-project/ray.git

# Sync newest wheels from AWS
COUNT=0
while ! [[ $WHEELS ]]; do
    # Get commit hash
    pushd ray
    COMMIT_HASH=$(git rev-parse "HEAD~$COUNT")
    popd

    # Attempt to sync wheels
    aws s3 sync --no-sign-request "s3://ray-wheels/$COMMIT_HASH/" $WHEEL_DIR
    WHEELS=$(ls $WHEEL_DIR)

    ((COUNT++))
done

# Rename wheels to ray_nightly-{version}.dev{date}-{info}.whl
pushd $WHEEL_DIR
DATE=$(date +"%Y%m%d")
VERSION_PATTERN="[0-9]\+\.[0-9]\+\.[0-9]\+"
for FILE in $WHEELS; do
    VERSION=$(echo $FILE | grep -o $VERSION_PATTERN)
    INFO_PATTERN="(?<=$VERSION-).*(?=\.whl)"
    INFO=$(echo $FILE | grep -P -o $INFO_PATTERN)
    NEW_FILE="ray_nightly-$VERSION.dev$DATE-$INFO.whl"
    mv $FILE $NEW_FILE
done
popd

# Upload to PyPI
# TODO(rliaw)

# Cleanup
rm -rf ray
rm $WHEEL_DIR/*
