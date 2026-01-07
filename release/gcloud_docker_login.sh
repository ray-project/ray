#!/bin/bash
# This script is used to login to gcloud docker registry using GCP workload identity
# federation service account

set -euo pipefail

# Detect architecture and download appropriate gcloud SDK
ARCH=$(uname -m)
if [[ "$ARCH" == "x86_64" ]]; then
    GCLOUD_ARCH="x86_64"
elif [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
    GCLOUD_ARCH="arm"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

GCLOUD_VERSION="550.0.0"
GCLOUD_TARBALL="google-cloud-cli-${GCLOUD_VERSION}-linux-${GCLOUD_ARCH}.tar.gz"

curl -fO "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_TARBALL}"
tar -xf "$GCLOUD_TARBALL"
./google-cloud-sdk/install.sh -q
PATH="$(pwd)/google-cloud-sdk/bin:$PATH"
export PATH
gcloud auth login --cred-file="$1" --quiet
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet
