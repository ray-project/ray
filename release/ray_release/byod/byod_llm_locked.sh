#!/bin/bash

set -exo pipefail

ARG LOCK_FILE
COPY "${LOCK_FILE}" .

RUN uv pip sync "${LOCK_FILE}"

# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1
