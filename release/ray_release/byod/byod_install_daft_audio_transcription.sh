#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

# TODO: Pin these versions.
uv pip install -r audio_transcription_py3.10.lock --system --no-deps --index-strategy unsafe-best-match