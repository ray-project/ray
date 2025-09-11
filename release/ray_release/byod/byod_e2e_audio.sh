#!/bin/bash

set -exo pipefail

# Install Python dependencies
uv pip sync llm_example_py311_cu128.lock --system --index-strategy unsafe-best-match
