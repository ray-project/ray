#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

# TODO (jeffreywang): Move to official wheels once dynamo changes are upstreamed.
cat > /tmp/ai-dynamo-runtime.txt <<'EOF'
ai-dynamo-runtime @ https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/dynamo/298f1e0/ai_dynamo_runtime-1.3.0-cp310-abi3-manylinux_2_35_x86_64.whl \
    --hash=sha256:107d78a5714962f568cb0578890362f8eeaf389b41c02b119925ed3d9830f484
EOF
pip3 install --no-deps --require-hashes -r /tmp/ai-dynamo-runtime.txt
