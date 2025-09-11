#!/bin/bash

set -exo pipefail

# Install Python dependencies
uv pip sync e2e_audio_py311_cu128.lock --system
