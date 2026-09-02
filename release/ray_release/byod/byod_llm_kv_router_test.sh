#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-cache-dir fastokens==0.2.0

# TODO (jeffreywang): Move to official wheels once dynamo changes are upstreamed.
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/0e1d1d8/ai_dynamo_runtime-1.4.0-cp310-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:5de340bef2e135b4720f94a51211605cffd9e5c3f1c8e8d621dd7e1d2be7ee62
EOF
pip3 install --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
