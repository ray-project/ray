"""Test pause/resume control plane API for Ray Serve LLM.

This test verifies that the DevIngress pause/resume endpoints work correctly:
1. Engine starts in unpaused state (is_paused=False)
2. Pause command halts generation (is_paused=True) while keeping weights in GPU
3. Resume command restores generation (is_paused=False)
4. Model can still serve requests after resume

Unlike sleep/wakeup which offloads weights to CPU, pause/resume keeps model
weights in GPU memory. This is useful for quick pause/resume cycles during
RL training where you want to pause generation for weight updates without
the overhead of offloading/reloading weights.

NOTE (Kourosh): This is part of a design in progress for integrating Ray Serve
LLM with RL workloads. The API is not public and won't be documented until the
end-to-end story is finalized. Class names and endpoint names may change.
"""

import time

import pytest
import requests
from openai import OpenAI
from ray import serve
from ray.llm._internal.serve.core.ingress.dev_ingress import build_dev_openai_app
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus

MODEL_ID = "Qwen/Qwen2-0.5B-Instruct"
BASE_URL = "http://localhost:8000"


def get_llm_config() -> LLMConfig:
    """Create LLMConfig for pause/resume testing."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=MODEL_ID,
        ),
        deployment_config=dict(
            num_replicas=2,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
        ),
    )


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def wait_for_server_ready(timeout: int = 240) -> None:
    """Wait for the server to be ready to handle requests."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            test_data = {
                "model": MODEL_ID,
                "messages": [{"role": "user", "content": "test"}],
                "max_tokens": 5,
                "temperature": 0,
            }
            response = requests.post(
                f"{BASE_URL}/v1/chat/completions", json=test_data, timeout=10
            )
            if response.status_code == 200:
                print(f"Server at {BASE_URL} is ready to handle requests!")
                return
        except Exception as e:
            print(f"Waiting for server to be ready... (error: {e})")

        time.sleep(2)

    raise TimeoutError(
        f"Server at {BASE_URL} did not become ready within {timeout} seconds"
    )


def test_pause_resume_lifecycle():
    """Test the complete pause/resume lifecycle."""
    # Start Ray Serve with DevIngress
    llm_config = get_llm_config()
    app = build_dev_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    # Wait for application to be running
    wait_for_condition(is_default_app_running, timeout=300)
    wait_for_server_ready(timeout=240)

    try:
        # Step 1: Verify initial state - engine should not be paused
        print("\n=== Step 1: Checking initial state ===")
        response = requests.get(
            f"{BASE_URL}/is_paused?model={MODEL_ID}",
            timeout=10,
        )
        assert response.status_code == 200, f"is_paused returned {response.status_code}"
        initial_pause_state = response.json().get("is_paused", None)
        assert (
            initial_pause_state is False
        ), f"Expected is_paused=False, got {initial_pause_state}"
        print(f"✓ Initial paused state: {initial_pause_state}")

        # Step 2: Verify model can serve requests before pause
        print("\n=== Step 2: Verifying model serves requests before pause ===")
        client = OpenAI(base_url=f"{BASE_URL}/v1", api_key="fake-key")
        chat_response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=10,
            temperature=0,
        )
        assert chat_response.choices[0].message.content is not None
        print(
            f"✓ Pre-pause response: {chat_response.choices[0].message.content[:50]}..."
        )

        # Step 3: Pause the engine
        print("\n=== Step 3: Pausing engine ===")
        pause_response = requests.post(
            f"{BASE_URL}/pause",
            json={
                "model": MODEL_ID,
                "options": {"wait_for_inflight_requests": False, "clear_cache": True},
            },
            timeout=60,
        )
        assert (
            pause_response.status_code == 200
        ), f"pause returned {pause_response.status_code}"
        print("✓ Pause command executed successfully")

        # Step 4: Verify engine is paused
        print("\n=== Step 4: Verifying engine is paused ===")
        # Wait for pause to complete
        wait_for_condition(
            lambda: requests.get(f"{BASE_URL}/is_paused?model={MODEL_ID}", timeout=5)
            .json()
            .get("is_paused")
            is True,
            timeout=30,
            retry_interval_ms=1000,
        )

        # Step 5: Resume the engine
        print("\n=== Step 5: Resuming engine ===")
        resume_response = requests.post(
            f"{BASE_URL}/resume",
            json={"model": MODEL_ID, "options": {}},
            timeout=60,
        )
        assert (
            resume_response.status_code == 200
        ), f"resume returned {resume_response.status_code}"
        print("✓ Resume command executed successfully")

        # Step 6: Verify engine is no longer paused
        print("\n=== Step 6: Verifying engine is resumed ===")
        wait_for_condition(
            lambda: requests.get(f"{BASE_URL}/is_paused?model={MODEL_ID}", timeout=5)
            .json()
            .get("is_paused")
            is False,
            timeout=30,
            retry_interval_ms=1000,
        )

        # Step 7: Verify model can still serve requests after resume
        print("\n=== Step 7: Verifying model can serve requests after resume ===")
        chat_response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=10,
            temperature=0,
        )
        assert chat_response.choices[0].message.content is not None
        print(
            f"✓ Post-resume response: {chat_response.choices[0].message.content[:50]}..."
        )

        print("\n=== All tests passed! ===")

    finally:
        # Cleanup
        serve.shutdown()
        time.sleep(1)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
