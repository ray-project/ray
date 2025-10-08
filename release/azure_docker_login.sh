#!/bin/bash
# This script is used to login to azure docker registry using azure cli

set -euo pipefail

# Retrieve credentials from Secrets Manager
SECRET=$(aws secretsmanager get-secret-value \
  --secret-id azure-service-principal-oss-release \
  --query SecretString \
  --region us-west-2 \
  --output text)

CLIENT_ID=$(echo "$SECRET" | jq -r '.client_id')
CLIENT_SECRET=$(echo "$SECRET" | jq -r '.client_secret')
TENANT_ID=$(echo "$SECRET" | jq -r '.tenant_id')

# Login to azure
az login --service-principal --username "$CLIENT_ID" --password "$CLIENT_SECRET" --tenant "$TENANT_ID"
