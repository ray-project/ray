#!/usr/bin/env bash

# This scripts installs helm.

set -xe

# Install helm
wget https://get.helm.sh/helm-v3.12.2-linux-amd64.tar.gz
tar -xvzf helm-v3.12.2-linux-amd64.tar.gz
chmod +x linux-amd64/helm
mv ./linux-amd64/helm /usr/bin/helm
helm --help
