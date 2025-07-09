#!/bin/bash

set -exo pipefail

# Python dependencies
pip3 install --no-cache-dir \
    "mcp==1.8.0" \
    "asyncio==3.4.3"

# Podman (used in stdio examples)
sudo apt-get update && sudo apt-get install -y podman

# Use the AWS CLI to fetch BRAVE_API_KEY from Secrets Manager
# Replace 'my-brave-api-key-secret' with the actual secret name
BRAVE_API_KEY=$(aws secretsmanager get-secret-value \
  --secret-id brave-search-api-key \
  --query SecretString \
  --output text)

export BRAVE_API_KEY
