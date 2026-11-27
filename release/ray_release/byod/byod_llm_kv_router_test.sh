#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-cache-dir fastokens==0.2.0

# TODO (jeffreywang): Move to official wheels once a dynamo release includes
# SelectionService worker publishers. Built with --features select-service from
# ai-dynamo/dynamo@28697ef5c4.
# --no-deps: ai-dynamo-runtime constrains pydantic and uvloop, both already
# pinned by the base llm image's vLLM install.
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/28697ef/ai_dynamo_runtime-1.8.0-cp311-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:b258679bb5d36e1cee3aa0bda4f5ae6443813358dbae16021b95e4131fd9110e
EOF
pip3 install --no-cache-dir --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
