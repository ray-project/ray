#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

# TODO (jeffreywang): Move to official wheels once dynamo changes are upstreamed.
# Built from dynamo token-cache branch @ c41ee4c (select() caches booking inputs
# so create_reservation replays by reservation_id, feature=select-service).
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/c41ee4c/ai_dynamo_runtime-1.3.0-cp310-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:c93968c58d9af571432189816fd0d1cebf70f327269aa8ea7ae538808e964d36
EOF
pip3 install --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
