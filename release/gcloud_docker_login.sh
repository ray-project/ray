#!/bin/bash
# This script is used to login to gcloud docker registry using GCP workload identity
# federation service account

set -euo pipefail

# Only install gcloud if not already available
if ! command -v gcloud &> /dev/null; then
  ARCH=$(uname -m)
  case "$ARCH" in
      x86_64)
          GCLOUD_ARCH="x86_64"
          ;;
      aarch64|arm64)
          GCLOUD_ARCH="arm"
          ;;
      *)
          echo "Unsupported architecture: $ARCH"
          exit 1
          ;;
  esac

  GCLOUD_VERSION="550.0.0"
  GCLOUD_TARBALL="google-cloud-cli-${GCLOUD_VERSION}-linux-${GCLOUD_ARCH}.tar.gz"

  curl -fO "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_TARBALL}"
  tar -xf "$GCLOUD_TARBALL"
  ./google-cloud-sdk/install.sh -q
  PATH="$(pwd)/google-cloud-sdk/bin:$PATH"
  export PATH
fi

gcloud auth login --cred-file="$1" --quiet
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet
