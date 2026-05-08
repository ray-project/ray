"""Test sleep/wakeup control plane API for Ray Serve LLM.

This test verifies that the DevIngress sleep/wakeup endpoints work correctly:
1. Engine starts in awake state (is_sleeping=False)
2. Sleep command puts engine to sleep (is_sleeping=True) and frees GPU memory
3. Wakeup command restores engine (is_sleeping=False) and restores GPU memory
4. Model can still serve requests after wakeup

NOTE (Kourosh): This is part of a design in progress for integrating Ray Serve
LLM with RL workloads. The API is not public and won't be documented until the
end-to-end story is finalized. Class names and endpoint names may change.
"""

import subprocess
import time
from typing import List

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
    """Create LLMConfig with sleep mode enabled."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=MODEL_ID,
        ),
        deployment_config=dict(
            num_replicas=2,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            enable_sleep_mode=True,
        ),
    )


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def get_gpu_memory_used_mb() -> List[float]:
    """Get GPU memory used per device via nvidia-smi.

    Returns:
        List of memory used in MB for each GPU device.
    """
    result = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [float(x.strip()) for x in result.stdout.strip().split("\n") if x.strip()]


def get_total_gpu_memory_mb() -> float:
    """Get total GPU memory used across all devices."""
    return sum(get_gpu_memory_used_mb())


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


def test_sleep_wakeup_lifecycle():
    """Test the complete sleep/wakeup lifecycle with GPU memory verification."""
    # Start Ray Serve with DevIngress
    llm_config = get_llm_config()
    app = build_dev_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    # Wait for application to be running
    wait_for_condition(is_default_app_running, timeout=300)
    wait_for_server_ready(timeout=240)
    time.sleep(5)  # Buffer time for server to be fully ready

    try:
        # Step 1: Verify initial state - engine should be awake
        print("\n=== Step 1: Checking initial state ===")
        response = requests.get(
            f"{BASE_URL}/is_sleeping?model={MODEL_ID}",
            timeout=10,
        )
        assert (
            response.status_code == 200
        ), f"is_sleeping returned {response.status_code}"
        initial_sleep_state = response.json().get("is_sleeping", None)
        assert (
            initial_sleep_state is False
        ), f"Expected is_sleeping=False, got {initial_sleep_state}"
        print(f"✓ Initial sleeping state: {initial_sleep_state}")

        # Step 2: Record baseline GPU memory
        print("\n=== Step 2: Recording baseline GPU memory ===")
        baseline_memory_mb = get_total_gpu_memory_mb()
        print(f"Baseline GPU memory: {baseline_memory_mb:.2f} MB")
        assert baseline_memory_mb > 0, "Baseline GPU memory should be > 0"

        # Step 3: Put engine to sleep
        print("\n=== Step 3: Putting engine to sleep ===")
        sleep_response = requests.post(
            f"{BASE_URL}/sleep",
            json={"model": MODEL_ID, "options": {"level": 1}},
            timeout=60,
        )
        assert (
            sleep_response.status_code == 200
        ), f"sleep returned {sleep_response.status_code}"
        print("✓ Sleep command executed successfully")

        # Wait a bit for sleep to complete
        time.sleep(5)

        # Step 4: Verify engine is sleeping
        print("\n=== Step 4: Verifying engine is sleeping ===")
        response = requests.get(
            f"{BASE_URL}/is_sleeping?model={MODEL_ID}",
            timeout=10,
        )
        assert response.status_code == 200
        sleep_state = response.json().get("is_sleeping", None)
        assert (
            sleep_state is True
        ), f"Expected is_sleeping=True after sleep, got {sleep_state}"
        print(f"✓ Sleeping state: {sleep_state}")

        # Step 5: Verify GPU memory reduction
        print("\n=== Step 5: Verifying GPU memory reduction ===")
        sleep_memory_mb = get_total_gpu_memory_mb()
        memory_reduction_mb = baseline_memory_mb - sleep_memory_mb
        memory_reduction_pct = (memory_reduction_mb / baseline_memory_mb) * 100
        print(f"GPU memory after sleep: {sleep_memory_mb:.2f} MB")
        print(
            f"Memory reduction: {memory_reduction_mb:.2f} MB ({memory_reduction_pct:.1f}%)"
        )
        assert (
            memory_reduction_pct > 50
        ), f"Expected >50% memory reduction, got {memory_reduction_pct:.1f}%"
        print("✓ GPU memory reduced significantly after sleep")

        # Step 6: Wake up the engine
        print("\n=== Step 6: Waking up engine ===")
        wakeup_response = requests.post(
            f"{BASE_URL}/wakeup",
            json={"model": MODEL_ID, "options": {"tags": ["weights", "kv_cache"]}},
            timeout=60,
        )
        assert (
            wakeup_response.status_code == 200
        ), f"wakeup returned {wakeup_response.status_code}"
        print("✓ Wakeup command executed successfully")

        # Wait a bit for wakeup to complete
        time.sleep(5)

        # Step 7: Verify engine is awake
        print("\n=== Step 7: Verifying engine is awake ===")
        response = requests.get(
            f"{BASE_URL}/is_sleeping?model={MODEL_ID}",
            timeout=10,
        )
        assert response.status_code == 200
        wake_state = response.json().get("is_sleeping", None)
        assert (
            wake_state is False
        ), f"Expected is_sleeping=False after wakeup, got {wake_state}"
        print(f"✓ Sleeping state: {wake_state}")

        # Step 8: Verify GPU memory restoration
        print("\n=== Step 8: Verifying GPU memory restoration ===")
        wake_memory_mb = get_total_gpu_memory_mb()
        memory_diff_mb = abs(wake_memory_mb - baseline_memory_mb)
        memory_diff_pct = (memory_diff_mb / baseline_memory_mb) * 100
        print(f"GPU memory after wakeup: {wake_memory_mb:.2f} MB")
        print(f"Baseline memory: {baseline_memory_mb:.2f} MB")
        print(f"Memory difference: {memory_diff_mb:.2f} MB ({memory_diff_pct:.1f}%)")
        assert (
            memory_diff_pct < 20
        ), f"Expected <20% memory difference from baseline, got {memory_diff_pct:.1f}%"
        print("✓ GPU memory restored to near baseline after wakeup")

        # Step 9: Verify model can still serve requests
        print("\n=== Step 9: Verifying model can serve requests ===")
        client = OpenAI(base_url=f"{BASE_URL}/v1", api_key="fake-key")
        chat_response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=10,
            temperature=0,
        )
        assert chat_response.choices[0].message.content is not None
        print(
            f"✓ Model successfully generated response: {chat_response.choices[0].message.content[:50]}..."
        )

        print("\n=== All tests passed! ===")

    finally:
        # Cleanup
        serve.shutdown()
        time.sleep(1)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
