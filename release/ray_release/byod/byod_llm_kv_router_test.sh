#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-cache-dir fastokens==0.2.0

# TODO (jeffreywang): Move to official wheels once a dynamo release includes
# SelectionService worker publishers. Built with --features select-service from
# jeffreywang88/dynamo@8e6c406628.
# --no-deps: ai-dynamo-runtime constrains pydantic and uvloop, both already
# pinned by the base llm image's vLLM install.
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/8e6c406/ai_dynamo_runtime-1.7.0-cp311-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:b6a98dbb05e4a8cdc31a8c9b2cb7078e9792355330aef1283407d463eddf9299
EOF
pip3 install --no-cache-dir --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
