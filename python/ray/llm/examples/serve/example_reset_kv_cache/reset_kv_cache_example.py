"""
Example: Resetting KV Cache in Ray Serve LLM via Control Plane Messages.

This example demonstrates two approaches to reset the KV cache on all replicas
of a Ray Serve LLM deployment using DevIngress:

1. **HTTP Endpoint Path** (`--use-http`):
   Calls the built-in `/reset_prefix_cache` HTTP endpoint provided by
   DevIngress via CacheManagerIngressMixin. Useful for external clients.

2. **In-Cluster Serve Handle Path** (default):
   Uses Ray Serve's deployment handles and the broadcast API to send control
   plane messages directly to all replicas. This keeps cache reset logic
   within the cluster, avoiding HTTP overhead.

Both approaches use the same DevIngress server which provides control plane
endpoints (/sleep, /wakeup, /is_sleeping, /reset_prefix_cache).

The example:
1. Starts a Serve application with DevIngress and 2 replicas
2. Populates the KV cache on both replicas by sending multiple requests
3. Measures request time for a cached request (control)
4. Resets the KV cache using the selected method
5. Measures request time after cache reset (test)
6. Verifies that the cache was cleared by comparing request times

Usage:
    # In-cluster path (using serve handles directly)
    python reset_kv_cache_example.py

    # HTTP endpoint path
    python reset_kv_cache_example.py --use-http
"""

import argparse
import asyncio
import time

import httpx

from ray import serve
from ray.llm._internal.serve.core.ingress.dev_ingress import build_dev_openai_app
from ray.llm._internal.serve.utils.broadcast import broadcast
from ray.serve.llm import LLMConfig

# =============================================================================
# Server Startup
# =============================================================================


def create_llm_config(model: str) -> LLMConfig:
    """Create the LLM configuration."""
    return LLMConfig(
        model_loading_config=dict(model_id=model),
        deployment_config=dict(num_replicas=2, name="llm"),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enforce_eager=True,
            max_num_batched_tokens=128,
        ),
    )


def start_server(llm_config: LLMConfig):
    """Start the server with DevIngress for control plane endpoints.

    DevIngress provides built-in control plane endpoints:
    - /reset_prefix_cache (via CacheManagerIngressMixin)
    - /sleep, /wakeup, /is_sleeping (via SleepableIngressMixin)
    """
    app = build_dev_openai_app({"llm_configs": [llm_config]})
    print("Starting server with DevIngress...")
    serve.run(app)
    print("Server started. Control plane endpoints available.")


# =============================================================================
# Cache Reset Functions
# =============================================================================


async def reset_cache_via_http(model: str):
    """Reset KV cache via HTTP endpoint.

    This calls the /reset_prefix_cache endpoint provided by DevIngress
    via CacheManagerIngressMixin.
    """
    url = "http://localhost:8000/reset_prefix_cache"
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json={"model": model})
        response.raise_for_status()


def reset_cache_via_handle(model: str):
    """Reset KV cache via in-cluster serve handle.

    This uses the broadcast API to send control plane messages directly to
    all replicas without exposing functionality over HTTP.
    """
    llm_handle = serve.get_deployment_handle("LLMServer:llm", app_name="default")
    broadcast(llm_handle, "reset_prefix_cache")


# =============================================================================
# Test Utilities
# =============================================================================


async def send_request(prompt: str, model: str, measure_time: bool = False):
    """Send a completion request and optionally measure response time."""
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
    """Send requests multiple times to populate cache on all replicas."""
    print(
        f"Populating cache with {len(prompts)} prompt(s), repeating {repeat} times..."
    )
    tasks = []
    for _ in range(repeat):
        for prompt in prompts:
            tasks.append(send_request(prompt, model, measure_time=False))

    await asyncio.gather(*tasks)
    print("Cache populated.")


# =============================================================================
# Main
# =============================================================================


async def main(use_http: bool):
    model = "Qwen/Qwen2.5-0.5B-Instruct"

    # Create LLM config and start server
    llm_config = create_llm_config(model)
    start_server(llm_config)

    # Determine reset method
    reset_method = (
        "HTTP endpoint (/reset_prefix_cache)"
        if use_http
        else "in-cluster serve handle (broadcast API)"
    )
    print(f"\nUsing {reset_method} for cache reset.\n")

    # Use long prompts to ensure prefill time is significant
    TEST_PROMPT = "The quick brown fox jumps over the lazy dog." * 3000

    # 2. Populate cache on all replicas
    print("Step 1: Populating cache on all replicas...")
    await populate_cache([TEST_PROMPT], model, repeat=20)

    # 3. Measure request time for cached request (control)
    print("\nStep 2: Measuring request time for cached request (control)...")
    control_time = await send_request(TEST_PROMPT, model, measure_time=True)
    print(f"Request time (cached): {control_time:.4f}s")

    # 4. Reset the KV cache
    print(f"\nStep 3: Resetting KV cache via {reset_method}...")
    if use_http:
        await reset_cache_via_http(model)
    else:
        reset_cache_via_handle(model)
    print("KV cache reset complete.")

    # 5. Measure request time after cache reset (test)
    print("\nStep 4: Measuring request time after cache reset (test)...")
    test_time = await send_request(TEST_PROMPT, model, measure_time=True)
    print(f"Request time (after reset): {test_time:.4f}s")

    # 6. Verify the results
    print("\nStep 5: Verifying results...")
    print(f"Control (cached) time: {control_time:.4f}s")
    print(f"Test (after reset) time: {test_time:.4f}s")
    print(f"Slowdown factor: {test_time / control_time:.2f}x slower after reset")

    if test_time > control_time * 10:  # At least 10x slower on L4 instances
        print(
            "✓ SUCCESS: Request time increased after cache reset, "
            "indicating cache was cleared."
        )
    else:
        print(
            "✗ WARNING: Request time did not increase significantly. "
            "Cache may not have been reset properly."
        )

    print("\nDone. Shutting down...")
    time.sleep(2)
    serve.shutdown()
    print("Shutdown complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Demonstrate KV cache reset in Ray Serve LLM.",
    )
    parser.add_argument(
        "--use-http",
        action="store_true",
        help="Reset cache via HTTP /reset_prefix_cache endpoint instead of "
        "in-cluster serve handles. Both use the same DevIngress server.",
    )
    args = parser.parse_args()

    asyncio.run(main(use_http=args.use_http))
