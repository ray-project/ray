#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-cache-dir fastokens==0.2.0

# TODO (jeffreywang): Move to official wheels once a dynamo release includes
# ai-dynamo/dynamo#16969. Built from dynamo main @ f9c22d7.
# --no-deps: ai-dynamo-runtime constrains pydantic and uvloop, both already
# pinned by the base llm image's vLLM install.
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/f9c22d7/ai_dynamo_runtime-1.7.0-cp311-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:733c7c57e15b37ac23b0ba7b2bbcd35f75f521f1349766742b867c10f89e5562
EOF
pip3 install --no-cache-dir --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
