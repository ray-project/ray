#!/usr/bin/env bash
# This script is used to build an extra layer on top of the base llm image
# to cache the model and dataset for the single-node vLLM benchmark.

set -euxo pipefail

export HF_HOME="${HF_HOME:-/home/ray/.cache/huggingface}"
export RAY_LLM_BENCHMARK_DATASET_PATH="${RAY_LLM_BENCHMARK_DATASET_PATH:-/tmp/ray_llm_benchmark_dataset}"

PYTHON_BIN="${PYTHON:-python}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    PYTHON_BIN=python3
fi

"$PYTHON_BIN" - <<'PY'
import os
from pathlib import Path

from datasets import load_dataset
from huggingface_hub import snapshot_download

model_id = "facebook/opt-1.3b"
dataset_id = "Crystalcareai/Code-feedback-sharegpt-renamed"
dataset_path = Path(os.environ["RAY_LLM_BENCHMARK_DATASET_PATH"])

print(f"Caching Hugging Face model: {model_id}")
snapshot_download(repo_id=model_id)

if dataset_path.exists():
    print(f"Benchmark dataset already cached at: {dataset_path}")
else:
    print(f"Caching benchmark dataset {dataset_id} at: {dataset_path}")
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    dataset = load_dataset(dataset_id, split="train")
    dataset.save_to_disk(str(dataset_path))
PY

if id ray >/dev/null 2>&1; then
    chown -R ray: "$HF_HOME" "$(dirname "$RAY_LLM_BENCHMARK_DATASET_PATH")" || true
fi
