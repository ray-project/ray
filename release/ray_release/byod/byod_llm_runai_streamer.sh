#!/bin/bash
# Install the optional S3 dependency used by the RunAI Streamer release test.

set -exo pipefail

pip3 install "runai-model-streamer[s3]==0.16.1"
