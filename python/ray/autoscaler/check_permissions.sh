#!/bin/bash

# Get the active account email
ACCOUNT_EMAIL=$(gcloud config get-value account)
echo "Active account: $ACCOUNT_EMAIL"

# Get the current project
PROJECT=$(gcloud config get-value project)
echo "Active project: $PROJECT"

# List all roles and permissions for the active account in the current project
gcloud projects get-iam-policy "$PROJECT" --flatten="bindings[].members" --format="table(bindings.role,bindings.members)" --filter="bindings.members:$ACCOUNT_EMAIL"
