#!/usr/bin/env bash
set -euxo pipefail

cp content/server.py server.py
cp content/client.py client.py
cp content/requirements.txt requirements.txt
cp content/service.yaml service.yaml

pip uninstall aws -y
pip install awscli

sudo apt-get install redis-tools -y
sudo apt install jq -y

pip install -r requirements.txt

anyscale service deploy --config-file=service.yaml

SERVICE_NAME="asynchronous-inference-service"
anyscale service wait --name "$SERVICE_NAME" --state RUNNING --timeout-s 600

SERVICE_JSON=$(anyscale service status --name "$SERVICE_NAME" --json)
SERVICE_URL=$(echo "$SERVICE_JSON" | jq -r '.query_url // .[0].query_url')
AUTH_TOKEN=$(echo "$SERVICE_JSON" | jq -r '.query_auth_token // .[0].query_auth_token')

echo "URL: $SERVICE_URL"
echo "Token: $AUTH_TOKEN"

python client.py -H "Bearer $AUTH_TOKEN" "$SERVICE_URL"


rm server.py client.py requirements.txt service.yaml


# -----------------------------------------------------------------------------
# Second way of testing the tutorial

python ci/nb2py.py content/asynchronous-inference.ipynb /tmp/asynchronous-inference.py
python /tmp/asynchronous-inference.py

rm /tmp/asynchronous-inference.py
