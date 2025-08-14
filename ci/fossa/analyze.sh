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
