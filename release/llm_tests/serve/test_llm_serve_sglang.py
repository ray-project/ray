import sys
import time

import pytest
import requests
from openai import OpenAI

from ray import serve
from ray.llm.examples.sglang.modules.sglang_engine import SGLangServer
from ray.serve.llm import LLMConfig, build_openai_app

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b-sglang"
SERVER_URL = "http://localhost:8000"


def wait_for_server_ready(url: str, timeout: int = 300, retry_interval: int = 5):
    """Poll the server until it can serve a completion request."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            resp = requests.post(
                f"{url}/v1/completions",
                json={
                    "model": RAY_MODEL_ID,
                    "prompt": "test",
                    "max_tokens": 5,
                    "temperature": 0,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"Server at {url} is ready.")
                return
        except Exception:
            pass

        print(f"Waiting for server at {url} to be ready...")
        time.sleep(retry_interval)

    raise TimeoutError(f"Server at {url} did not become ready within {timeout}s")


def test_sglang_serve_e2e():
    """End-to-end test for the SGLang serve example integration pattern."""
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 1,
                "max_replicas": 1,
            }
        },
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 1,
            "mem_fraction_static": 0.8,
        },
    )

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    try:
        wait_for_server_ready(SERVER_URL, timeout=300)
        time.sleep(5)

        client = OpenAI(base_url=f"{SERVER_URL}/v1", api_key="fake-key")

        # Chat completions
        chat_resp = client.chat.completions.create(
            model=RAY_MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=64,
            temperature=0.0,
        )
        assert len(chat_resp.choices) > 0
        chat_content = chat_resp.choices[0].message.content
        assert (
            chat_content and len(chat_content.strip()) > 0
        ), f"Empty chat response: {chat_resp}"
        print(f"Chat response: {chat_content}")

        # Text completions
        comp_resp = client.completions.create(
            model=RAY_MODEL_ID,
            prompt="The capital of France is",
            max_tokens=64,
            temperature=0.0,
        )
        assert len(comp_resp.choices) > 0
        comp_text = comp_resp.choices[0].text
        assert (
            comp_text and len(comp_text.strip()) > 0
        ), f"Empty completion response: {comp_resp}"
        print(f"Completion response: {comp_text}")

    finally:
        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
