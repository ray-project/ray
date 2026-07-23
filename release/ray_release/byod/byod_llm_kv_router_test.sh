#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the KV aware router release tests.

set -exo pipefail

pip3 install --no-deps ai-dynamo-runtime==1.3.0.post1
