#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-cache-dir fastokens==0.2.0

# --no-deps: ai-dynamo-runtime constrains pydantic and uvloop, both already
# pinned by the base llm image's vLLM install.
pip3 install --no-cache-dir --no-deps ai-dynamo-runtime==1.4.0
