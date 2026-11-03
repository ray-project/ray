import os
import subprocess
import sys
from pathlib import Path

import pytest

import ray
from ray import serve


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.mark.parametrize(
    "example",
    [
        "source/data/llm/doc_code/basic_llm_example.py",
        "source/data/llm/doc_code/classification_example.py",
        "source/data/llm/doc_code/custom_tokenizer_example.py",
        "source/data/llm/doc_code/embedding_example.py",
        "source/data/llm/doc_code/minimal_quickstart.py",
        "source/data/llm/doc_code/omni_audio_example.py",
        "source/data/llm/doc_code/openai_api_example.py",
        "source/data/llm/doc_code/tokenization_aggregation_example.py",
        "source/data/llm/doc_code/tokenization_disaggregation_example.py",
        "source/data/llm/doc_code/vlm_image_example.py",
        "source/data/llm/doc_code/vlm_video_example.py",
        "source/serve/llm/doc_code/qwen/qwen_example.py",
        pytest.param(
            "source/serve/llm/doc_code/qwen/llm_yaml_config_example.py",
            marks=pytest.mark.requires_2GPU,
        ),
        "source/serve/llm/doc_code/transcription/transcription_example.py",
        "source/serve/llm/doc_code/direct_streaming/direct_streaming_example.py",
        "source/serve/llm/doc_code/direct_streaming/direct_streaming_custom_router_example.py",
        "source/serve/llm/doc_code/direct_streaming/direct_streaming_yaml_config_example.py",
        "source/serve/llm/doc_code/custom_vllm/custom_vllm_example.py",
        "source/serve/llm/doc_code/custom_vllm/custom_vllm_yaml_config_example.py",
        "source/data/examples/llm/vllm-with-lora.ipynb",
        "source/data/examples/llm/vllm-with-structural-output.ipynb",
        pytest.param(
            "source/serve/llm/doc_code/multi_gpu/bundle_per_worker_example.py",
            marks=pytest.mark.requires_2GPU,
        ),
        pytest.param(
            "source/serve/llm/doc_code/multi_gpu/dp_autoscaling_example.py",
            marks=pytest.mark.requires_2GPU,
        ),
        pytest.param(
            "source/serve/llm/doc_code/multi_gpu/dp_basic_example.py",
            marks=pytest.mark.requires_4GPU,
        ),
        pytest.param(
            "source/serve/llm/doc_code/multi_gpu/dp_pd_example.py",
            marks=pytest.mark.requires_4GPU,
        ),
        pytest.param(
            "source/serve/llm/doc_code/prefix_aware_router/prefix_aware_example.py",
            marks=pytest.mark.requires_4GPU,
        ),
    ],
)
def test_llm_example(example):
    env = os.environ.copy()
    if "/direct_streaming/" in example or "/custom_vllm/" in example:
        env["RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING"] = "1"
        env["RAY_SERVE_ENABLE_HA_PROXY"] = "1"
    command = [sys.executable]
    if example.endswith(".ipynb"):
        command.extend(["test_myst_doc.py", "--path"])
    command.append(example)
    subprocess.run(command, check=True, env=env, cwd=Path(__file__).parent, timeout=900)
