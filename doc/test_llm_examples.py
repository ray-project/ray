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
        "source/data/doc_code/working-with-llms/basic_llm_example.py",
        "source/data/doc_code/working-with-llms/classification_example.py",
        "source/data/doc_code/working-with-llms/custom_tokenizer_example.py",
        "source/data/doc_code/working-with-llms/embedding_example.py",
        "source/data/doc_code/working-with-llms/minimal_quickstart.py",
        "source/data/doc_code/working-with-llms/omni_audio_example.py",
        "source/data/doc_code/working-with-llms/openai_api_example.py",
        "source/data/doc_code/working-with-llms/tokenization_aggregation_example.py",
        "source/data/doc_code/working-with-llms/tokenization_disaggregation_example.py",
        "source/data/doc_code/working-with-llms/vlm_image_example.py",
        "source/data/doc_code/working-with-llms/vlm_video_example.py",
        "source/llm/doc_code/serve/qwen/qwen_example.py",
        "source/llm/doc_code/serve/qwen/llm_yaml_config_example.py",
        "source/llm/doc_code/serve/transcription/transcription_example.py",
        "source/llm/doc_code/serve/direct_streaming/direct_streaming_example.py",
        "source/llm/doc_code/serve/direct_streaming/direct_streaming_custom_router_example.py",
        "source/llm/doc_code/serve/direct_streaming/direct_streaming_yaml_config_example.py",
        "source/llm/doc_code/serve/custom_vllm/custom_vllm_example.py",
        "source/llm/doc_code/serve/custom_vllm/custom_vllm_yaml_config_example.py",
        "source/llm/examples/batch/vllm-with-lora.ipynb",
        "source/llm/examples/batch/vllm-with-structural-output.ipynb",
        pytest.param(
            "source/llm/doc_code/serve/multi_gpu/bundle_per_worker_example.py",
            marks=pytest.mark.requires_2GPU,
        ),
        pytest.param(
            "source/llm/doc_code/serve/multi_gpu/dp_autoscaling_example.py",
            marks=pytest.mark.requires_2GPU,
        ),
        pytest.param(
            "source/llm/doc_code/serve/multi_gpu/dp_basic_example.py",
            marks=pytest.mark.requires_4GPU,
        ),
        pytest.param(
            "source/llm/doc_code/serve/multi_gpu/dp_pd_example.py",
            marks=pytest.mark.requires_4GPU,
        ),
        pytest.param(
            "source/llm/doc_code/serve/prefix_aware_router/prefix_aware_example.py",
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
