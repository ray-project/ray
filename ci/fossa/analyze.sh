#!/bin/bash

set -euo pipefail

FOSSA_BIN="$HOME/fossa/fossa"

FOSSA_API_KEY="$(
    aws secretsmanager get-secret-value --region us-west-2 \
    --secret-id oss-ci/fossa-api-key \
    --query SecretString --output text
)"
export FOSSA_API_KEY

"$FOSSA_BIN" analyze

git clone https://"$GITHUB_TOKEN"@github.com/ray-project/ray-cpp-wheel-analyzer.git "$HOME"/ray-cpp-wheel-analyzer

python "$HOME"/ray-cpp-wheel-analyzer/run_fossa_analysis_with_bazel.py \
        --run-fossa --fossa-api-key "$FOSSA_API_KEY" \
        --output-dir ../fossa-scan-dir \
        --fossa-exec "$FOSSA_BIN"
