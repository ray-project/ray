"""
Example: Using the dispatch API for control plane operations in Ray Serve LLM.

This example demonstrates how to use the dispatch API to send control plane messages
to all replicas of a Ray Serve LLM deployment. Specifically, it shows how to reset the KV cache on all replicas using the dispatch function.

The example:
1. Starts a Serve application with 2 replicas
2. Populates the KV cache on both replicas by sending multiple requests
3. Measures request time for a cached request (control)
4. Uses the dispatch API to reset the KV cache on all replicas
5. Measures request time after cache reset (test)
6. Verifies that the cache was cleared by comparing request times

The dispatch API is useful for control plane operations that need to be executed
on all replicas, such as cache resets, weight updates, or configuration changes.
"""

import asyncio
import httpx
import time
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.llm._internal.serve.utils.dispatch import dispatch


def start_server(model: str):
    """Start the Serve application with 2 replicas."""
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model,
        ),
        deployment_config=dict(
            num_replicas=2,
            name="llm"
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enforce_eager=True,
            max_num_batched_tokens=128,
        )
    )
    
    app = build_openai_app({"llm_configs": [llm_config]})
    print("Starting deployment...")
    serve.run(app)
    print("Deployment started.")


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
        response.raise_for_status()
    
    if measure_time:
        return time.time() - start_time


async def populate_cache(prompts: list[str], model: str, repeat: int = 20):
    """Send requests multiple times to populate cache on both replicas."""
    print(f"Populating cache with {len(prompts)} prompt(s), repeating {repeat} times...")
    tasks = []
    for _ in range(repeat):
        for prompt in prompts:
            tasks.append(send_request(prompt, model, measure_time=False))
    
    await asyncio.gather(*tasks)
    print("Cache populated.")


async def main():
    model = "Qwen/Qwen2.5-0.5B-Instruct"
    
    # 1. Start a serve application with 2 replicas
    # start_server(model)
    
    # Get handle to the LLMServer deployment for control plane operations
    llm_handle = serve.get_deployment_handle("LLMServer:llm", app_name="default")

    # Use long prompts to ensure prefill time is significant
    TEST_PROMPT = "The quick brown fox jumps over the lazy dog." * 3000

    # 2. Populate cache on both replicas
    print("\nStep 1: Populating cache on both replicas...")
    await populate_cache([TEST_PROMPT], model, repeat=20)
    exit()
    # 3. Measure request time for cached request (control)
    print("\nStep 2: Measuring request time for cached request (control)...")
    control_time = await send_request(TEST_PROMPT, model, measure_time=True)
    print(f"Request time (cached): {control_time:.4f}s")
    
    # 4. Reset the KV cache using the dispatch API
    print("\nStep 3: Resetting KV cache on all replicas...")
    dispatch(llm_handle, "reset_prefix_cache")
    print("KV cache reset complete.")
    
    # 5. Measure request time after cache reset (test)
    print("\nStep 4: Measuring request time after cache reset (test)...")
    test_time = await send_request(TEST_PROMPT, model, measure_time=True)
    print(f"Request time (after reset): {test_time:.4f}s")
    
    # 6. Verify the results
    print("\nStep 5: Verifying results...")
    print(f"Control (cached) time: {control_time:.4f}s")
    print(f"Test (after reset) time: {test_time:.4f}s")
    print(f"Speedup factor: {test_time / control_time:.2f}x slower after reset")
    
    if test_time > control_time * 10:  # At least 10x slower
        print("✓ SUCCESS: Request time increased after cache reset, indicating cache was cleared.")
    else:
        print("✗ WARNING: Request time did not increase significantly. Cache may not have been reset properly.")
    
    print("\nWaiting for 5 seconds before shutting down...")
    time.sleep(5)
    print("Shutting down...")
    serve.shutdown()
    print("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
