#!/bin/bash
# This script is used to login to azure docker registry using azure cli

set -euo pipefail

# Retrieve credentials from Secrets Manager
SECRET="$(aws secretsmanager get-secret-value \
  --secret-id arn:aws:secretsmanager:us-west-2:029272617770:secret:azure-service-principal-oss-release-XLR5pz \
  --query SecretString \
  --region us-west-2 \
  --output text)"

CLIENT_ID="$(echo "$SECRET" | jq -r '.client_id')"
TENANT_ID="$(echo "$SECRET" | jq -r '.tenant_id')"

temp_dir=$(mktemp -d)

aws secretsmanager get-secret-value \
--secret-id arn:aws:secretsmanager:us-west-2:029272617770:secret:azure-service-principal-certificate-I5Zyp0 \
--query SecretString \
--region us-west-2 \
--output text > "${temp_dir}/azure_cert.pem"

# Login to azure
az login --service-principal --username "$CLIENT_ID" --certificate "${temp_dir}/azure_cert.pem" --tenant "$TENANT_ID"
