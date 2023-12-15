#!/usr/bin/env bash

# This scripts installs helm.

set -exo pipefail

# Install helm
curl -sfL "https://get.helm.sh/helm-v3.12.2-linux-amd64.tar.gz" | tar -xzf - linux-amd64/helm
mv ./linux-amd64/helm /usr/bin/helm
helm --help
