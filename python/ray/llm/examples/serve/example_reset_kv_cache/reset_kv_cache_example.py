"""
Example: Resetting KV Cache in Ray Serve LLM which is Control Plane Messages.

This example demonstrates two approaches to reset the KV cache on all replicas
of a Ray Serve LLM deployment:

1. **HTTP Endpoint Path** (`--use-http`):
   Extends the OpenAI-compatible ingress with a custom `/reset_prefix_cache`
   endpoint. External clients can call this endpoint to trigger a cache reset.

   NOTE: This approach exposes cache reset functionality over HTTP, which may
   not be suitable for production environments where you want tighter control
   over when cache resets occur.

2. **In-Cluster Serve Handle Path** (default):
   Uses Ray Serve's deployment handles and the dispatch API to send control
   plane messages directly to all replicas. This approach keeps cache reset
   logic within the cluster, providing better security and control.

The example:
1. Starts a Serve application with 2 replicas
2. Populates the KV cache on both replicas by sending multiple requests
3. Measures request time for a cached request (control)
4. Resets the KV cache using the selected method
5. Measures request time after cache reset (test)
6. Verifies that the cache was cleared by comparing request times

Usage:
    # In-cluster path (using serve handles directly)
    python reset_kv_cache_example.py

    # HTTP endpoint path (extending OpenAI ingress)
    python reset_kv_cache_example.py --use-http
"""

import argparse
import asyncio
import time

import httpx

from ray import serve
from ray.llm._internal.serve.core.ingress.ingress import DEFAULT_ENDPOINTS
from ray.llm._internal.serve.utils.dispatch import broadcast
from ray.serve.llm import LLMConfig, build_llm_deployment, build_openai_app
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress

# Only import Request when needed for HTTP path
try:
    from fastapi import Request
except ImportError:
    Request = None


# =============================================================================
# HTTP Endpoint Path: Custom Ingress with /reset_prefix_cache endpoint
# =============================================================================


class KVCacheResetIngress(OpenAiIngress):
    """Custom OpenAI-compatible ingress with KV cache reset endpoint.

    This ingress extends the standard OpenAI endpoints with a custom
    /reset_prefix_cache endpoint that allows external clients to trigger
    a cache reset via HTTP.

    WARNING: Exposing cache reset over HTTP may not be suitable for production
    environments. Consider using the in-cluster serve handle approach for
    better security and control.
    """

    async def reset_prefix_cache(self, request: Request):
        """Reset the KV cache on all replicas for the specified model.

        Args:
            request: The FastAPI request object. Expects a `model` query
                parameter specifying the model ID to reset cache for.
        """
        model_id = request.query_params.get("model")
        handle = self._get_configured_serve_handle(model_id)
        broadcast(handle, "reset_prefix_cache")


# Endpoint map for the custom ingress
HTTP_ENDPOINTS = {
    "reset_prefix_cache": lambda app: app.post("/reset_prefix_cache"),
    **DEFAULT_ENDPOINTS,
}


# =============================================================================
# Server Startup Functions
# =============================================================================


def create_llm_config(model: str) -> LLMConfig:
    """Create the LLM configuration shared by both server modes."""
    return LLMConfig(
        model_loading_config=dict(model_id=model),
        deployment_config=dict(num_replicas=2, name="llm"),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enforce_eager=True,
            max_num_batched_tokens=128,
        ),
    )


def start_server_with_http_endpoint(llm_config: LLMConfig):
    """Start the server with custom HTTP endpoint for cache reset.

    This approach extends the OpenAI ingress with a /reset_prefix_cache
    endpoint that external clients can call to trigger cache reset.
    """
    llm_deployment = build_llm_deployment(llm_config)
    ingress_cls = make_fastapi_ingress(KVCacheResetIngress, endpoint_map=HTTP_ENDPOINTS)
    ingress_options = KVCacheResetIngress.get_deployment_options([llm_config])
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[llm_deployment],
    )

    print("Starting server with HTTP endpoint for cache reset...")
    serve.run(ingress_app)
    print("Server started. /reset_prefix_cache endpoint available.")


def start_server_standard(llm_config: LLMConfig):
    """Start the server with standard OpenAI endpoints.

    Cache reset is performed via in-cluster serve handles, not exposed
    over HTTP.
    """
    app = build_openai_app({"llm_configs": [llm_config]})
    print("Starting server (standard mode)...")
    serve.run(app)
    print("Server started. Cache reset via serve handles only.")


# =============================================================================
# Cache Reset Functions
# =============================================================================


async def reset_cache_via_http(model: str):
    """Reset KV cache via HTTP endpoint.

    This calls the /reset_prefix_cache endpoint exposed by KVCacheResetIngress.
    """
    url = f"http://localhost:8000/reset_prefix_cache?model={model}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url)
        response.raise_for_status()


def reset_cache_via_handle(model: str):
    """Reset KV cache via in-cluster serve handle.

    This uses the dispatch API to send control plane messages directly to
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

    # Create shared LLM config
    llm_config = create_llm_config(model)

    # 1. Start the server
    if use_http:
        start_server_with_http_endpoint(llm_config)
        reset_method = "HTTP endpoint (/reset_prefix_cache)"
    else:
        start_server_standard(llm_config)
        reset_method = "in-cluster serve handle"

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
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Use in-cluster serve handles (recommended for production)
    python reset_kv_cache_example.py

    # Use HTTP endpoint (for external access, less secure)
    python reset_kv_cache_example.py --use-http
        """,
    )
    parser.add_argument(
        "--use-http",
        action="store_true",
        help="Use HTTP endpoint for cache reset instead of serve handles. "
        "This exposes a /reset_prefix_cache endpoint that external clients "
        "can call. Not recommended for production.",
    )
    args = parser.parse_args()

    asyncio.run(main(use_http=args.use_http))
