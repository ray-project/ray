#!/usr/bin/env bash
set -euxo pipefail

# Test the asynchronous inference tutorial using two methods:
# 1. Using an actual Redis instance in AWS
# 2. Using a Redis instance started locally by the user


# -------------------------------------------------
# Test 1: Using an actual redis instance in the AWS
# -------------------------------------------------

cp content/server.py server.py
cp content/client.py client.py
cp content/requirements.txt requirements.txt
cp content/service.yaml service.yaml

pip uninstall aws -y

sudo apt-get install awscli jq redis-tools -y

pip install -r requirements.txt

anyscale service deploy --config-file=service.yaml

SERVICE_NAME="asynchronous-inference-service"

anyscale service wait --name "$SERVICE_NAME" --state RUNNING --timeout-s 600

SERVICE_JSON=$(anyscale service status --name "$SERVICE_NAME" --json)
SERVICE_URL=$(echo "$SERVICE_JSON" | jq -r '.query_url // .[0].query_url')
AUTH_TOKEN=$(echo "$SERVICE_JSON" | jq -r '.query_auth_token // .[0].query_auth_token')

python client.py -H "Bearer $AUTH_TOKEN" "$SERVICE_URL"

anyscale service terminate --name "$SERVICE_NAME"

anyscale service wait --name "$SERVICE_NAME" --state TERMINATED --timeout-s 600

anyscale service delete --name "$SERVICE_NAME"

rm server.py client.py requirements.txt service.yaml


# -------------------------------------------------------------
# Test 2: Using the redis instance started locally by the user.
# -------------------------------------------------------------

python ci/nb2py.py content/asynchronous-inference.ipynb /tmp/asynchronous-inference.py
python /tmp/asynchronous-inference.py

rm /tmp/asynchronous-inference.py
