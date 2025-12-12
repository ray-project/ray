import asyncio
import time

import httpx
from fastapi import Request

from ray import serve
from ray.llm._internal.serve.core.ingress.ingress import DEFAULT_ENDPOINTS
from ray.llm._internal.serve.utils.dispatch import dispatch
from ray.serve.llm import LLMConfig, build_llm_deployment
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress


# Create a custom OpenAiIngress that exposes an endpoint for kv-cache reset
class MyOpenAiIngress(OpenAiIngress):
    async def reset_prefix_cache(self, request: Request):
        """Reset the KV cache on all replicas."""

        model_id = request.query_params.get("model")
        handle = self._get_configured_serve_handle(model_id)
        dispatch(handle, "reset_prefix_cache")


# Extend the default endpoints with the new endpoint
CUSTOM_ENDPOINTS = {
    "reset_prefix_cache": lambda app: app.post("/reset_prefix_cache"),
    **DEFAULT_ENDPOINTS,
}


def start_server(model: str):
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model,
        ),
        deployment_config=dict(num_replicas=2, name="llm"),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enforce_eager=True,
            max_num_batched_tokens=128,
        ),
    )

    # Build the LLM deployment and ingress
    llm_deployment = build_llm_deployment(llm_config)
    ingress_cls = make_fastapi_ingress(MyOpenAiIngress, endpoint_map=CUSTOM_ENDPOINTS)
    ingress_options = MyOpenAiIngress.get_deployment_options([llm_config])
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[llm_deployment],
    )
    serve.run(ingress_app)


async def send_request(prompt: str, model: str, measure_time: bool = False):
    """Send a request and optionally measure the response time.

    Args:
        prompt: The prompt to send.
        model: The model name.
        measure_time: If True, measure and return the request time.

    Returns:
        If measure_time=True, returns the request time in seconds.
        Otherwise, returns None.
    """
    url = "http://localhost:8000/v1/completions"
    data = {
        "model": model,
        "prompt": prompt,
        "max_tokens": 1,
    }

    start_time = time.time() if measure_time else None
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=data)
        if response.status_code != 200:
            error_text = response.text
            raise Exception(
                f"Request failed with status {response.status_code}: {error_text}"
            )
        response.raise_for_status()

    if measure_time:
        return time.time() - start_time


async def reset_prefix_cache(model: str):
    """Reset the KV cache on all replicas via HTTP endpoint."""
    url = f"http://localhost:8000/reset_prefix_cache?model={model}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url)
        response.raise_for_status()
    return response


async def populate_cache(prompts: list[str], model: str, repeat: int = 20):
    """Send requests multiple times to populate cache on both replicas."""
    print(
        f"Populating cache with {len(prompts)} prompt(s), repeating {repeat} times..."
    )
    tasks = []
    for _ in range(repeat):
        for prompt in prompts:
            tasks.append(send_request(prompt, model, measure_time=False))

    await asyncio.gather(*tasks)
    print("Cache populated.")


async def main():
    model = "Qwen/Qwen2.5-0.5B-Instruct"

    # Start the server
    start_server(model)

    # Use long prompts to ensure prefill time is significant
    TEST_PROMPT = "The quick brown fox jumps over the lazy dog." * 3000

    # 1. Populate cache on both replicas
    print("\nStep 1: Populating cache on both replicas...")
    await populate_cache([TEST_PROMPT], model, repeat=20)

    # 2. Measure request time for cached request (control)
    print("\nStep 2: Measuring request time for cached request (control)...")
    control_time = await send_request(TEST_PROMPT, model, measure_time=True)
    print(f"Request time (cached): {control_time:.4f}s")

    # 3. Reset the KV cache using HTTP endpoint
    print("\nStep 3: Resetting KV cache on all replicas via HTTP endpoint...")
    await reset_prefix_cache(model)
    print("KV cache reset complete.")

    # 4. Measure request time after cache reset (test)
    print("\nStep 4: Measuring request time after cache reset (test)...")
    test_time = await send_request(TEST_PROMPT, model, measure_time=True)
    print(f"Request time (after reset): {test_time:.4f}s")

    # 5. Verify the results
    print("\nStep 5: Verifying results...")
    print(f"Control (cached) time: {control_time:.4f}s")
    print(f"Test (after reset) time: {test_time:.4f}s")
    print(f"Speedup factor: {test_time / control_time:.2f}x slower after reset")

    if test_time > control_time * 10:  # At least 10x slower
        print(
            "✓ SUCCESS: Request time increased after cache reset, indicating cache was cleared."
        )
    else:
        print(
            "✗ WARNING: Request time did not increase significantly. Cache may not have been reset properly."
        )

    print("\nDone. Server is still running.")


if __name__ == "__main__":
    asyncio.run(main())
