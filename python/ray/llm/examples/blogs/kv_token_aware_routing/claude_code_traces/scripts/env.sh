#!/usr/bin/env bash
# Runtime settings for the driver and direct ingress.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="${SCRIPT_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
# Strip the managed cluster's inherited working_dir before EngineCore reconnects.
export RAY_RUNTIME_ENV_HOOK=raynoophook.hook

: "${HF_HOME:?Set HF_HOME to a cache containing, or allowed to download, openai/gpt-oss-120b}"
export HF_HOME
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_SESSION_ID_HEADER_KEY=x-correlation-id
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
export RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S=20
export RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE=1048576
export RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS=1

export MODEL=openai/gpt-oss-120b

export AIPERF_DATASET_CONFIGURATION_TIMEOUT=1800
export AIPERF_SERVICE_PROFILE_CONFIGURE_TIMEOUT=1800
export AIPERF_DATASET_WEKA_LIVE_ASSISTANT_RESPONSES=0
