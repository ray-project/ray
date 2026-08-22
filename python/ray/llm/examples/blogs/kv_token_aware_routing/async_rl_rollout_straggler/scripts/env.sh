#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Make the runtime hook available to benchmark drivers and model subprocesses.
export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"
export RAY_RUNTIME_ENV_HOOK=raynoophook.hook
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
export RAY_SERVE_SESSION_ID_HEADER_KEY=x-correlation-id
export RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS=1
export VLLM_MEMORY_PROFILER_ESTIMATE_CUDAGRAPHS=0
export VLLM_DISABLE_COMPILE_CACHE=1
export HF_HOME="${HF_HOME:-/mnt/cluster_storage/hf_cache}"
export AIPERF_SRC="${AIPERF_SRC:-/home/ray/default/aiperf/src}"
export DYNAMO_SOURCE="${DYNAMO_SOURCE:-/home/ray/default/dynamo}"
export AGENTIC_RUNTIME_PYTHON="${AGENTIC_RUNTIME_PYTHON:-/mnt/cluster_storage/agentic/venv-router-benchmark-raycurrent-vllm026/bin/python}"
export AIPERF_BASE_PYTHON="${AIPERF_BASE_PYTHON:-python3}"

export MODEL=openai/gpt-oss-120b
