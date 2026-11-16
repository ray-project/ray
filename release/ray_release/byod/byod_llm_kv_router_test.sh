#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-cache-dir fastokens==0.2.0

# TODO (jeffreywang): Move to official wheels once a dynamo release includes
# SelectionService worker publishers. Built with --features select-service from
# jeffreywang88/dynamo@3d8b918, one commit on dynamo main @ 4a046df.
# --no-deps: ai-dynamo-runtime constrains pydantic and uvloop, both already
# pinned by the base llm image's vLLM install.
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/3d8b918/ai_dynamo_runtime-1.7.0-cp311-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:87d1d773635a8b6559652928767994e06f5741e700f463c7e7ad05669f2159ac
EOF
pip3 install --no-cache-dir --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
