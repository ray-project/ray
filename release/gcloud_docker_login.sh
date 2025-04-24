#!/bin/bash
# This script is used to login to gcloud docker registry using GCP workload identity
# federation service account

set -euo pipefail

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-441.0.0-linux-arm.tar.gz
tar -xf google-cloud-cli-441.0.0-linux-arm.tar.gz
./google-cloud-sdk/install.sh -q
PATH="$(pwd)/google-cloud-sdk/bin:$PATH"
export PATH
gcloud auth login --cred-file="$1" --quiet
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet
