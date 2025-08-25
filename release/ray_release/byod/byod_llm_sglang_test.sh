#!/bin/bash
# This script is used to build an extra layer on top of the base llm image
# to run the llm sglang release tests

set -exo pipefail

pip3 install "sglang[all]==0.5.1.post2"
