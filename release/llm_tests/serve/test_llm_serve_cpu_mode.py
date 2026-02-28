"""
Release test for CPU-only mode in Ray Serve LLM.

This test validates end-to-end serving on CPU-only infrastructure:
1. Model can be loaded and served on CPU with vLLM CPU backend
2. Completions and chat completions work correctly

"""

import pytest
import sys
import time

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus
from openai import OpenAI

MODEL_SRC = "Qwen/Qwen2.5-0.5B-Instruct"
MODEL_ID = "qwen-0.5b"
MAX_OUTPUT_TOKENS = 64
SEED = 42


def get_cpu_llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=MODEL_ID,
            model_source=MODEL_SRC,
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            dtype="float16",
            max_model_len=2048,
            enforce_eager=True,
        ),
        use_cpu=True,
    )


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def test_cpu_mode_serving():
    """Test end-to-end serving on CPU with completions and chat."""
    base_url = "http://localhost:8000"

    llm_config = get_cpu_llm_config()
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    client = OpenAI(base_url=f"{base_url}/v1", api_key="fake-key")

    try:
        wait_for_condition(is_default_app_running, timeout=300)
        time.sleep(5)

        # Test completion
        completion = client.completions.create(
            model=MODEL_ID,
            prompt="The capital of France is",
            temperature=0.0,
            max_tokens=MAX_OUTPUT_TOKENS,
            seed=SEED,
        )
        completion_text = completion.choices[0].text
        print(f"Completion: {completion_text}")
        assert completion_text and len(completion_text.strip()) > 0

        # Test chat completion
        chat = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": "What is 2+2?"}],
            temperature=0.0,
            max_tokens=MAX_OUTPUT_TOKENS,
            seed=SEED,
        )
        chat_text = chat.choices[0].message.content
        print(f"Chat: {chat_text}")
        assert chat_text and len(chat_text.strip()) > 0

        print("CPU mode serving test passed!")

    finally:
        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
