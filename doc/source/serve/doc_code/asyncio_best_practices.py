# flake8: noqa
"""
Code examples for the asyncio best practices guide.
All examples are structured to be runnable and demonstrate key concepts.
"""

# __imports_begin__
from ray import serve
import asyncio
# __imports_end__


# __echo_async_begin__
@serve.deployment
class Echo:
    async def __call__(self, request):
        await asyncio.sleep(0.1)
        return "ok"
# __echo_async_end__


# __blocking_echo_begin__
@serve.deployment
class BlockingEcho:
    def __call__(self, request):
        # Blocking.
        import time
        time.sleep(1)
        return "ok"
# __blocking_echo_end__


# __fastapi_deployment_begin__
from fastapi import FastAPI

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class FastAPIDeployment:
    @app.get("/sync")
    def sync_endpoint(self):
        # FastAPI runs this in a threadpool.
        import time
        time.sleep(1)
        return "ok"

    @app.get("/async")
    async def async_endpoint(self):
        # Runs directly on FastAPI's asyncio loop.
        await asyncio.sleep(1)
        return "ok"
# __fastapi_deployment_end__


# __blocking_http_begin__
@serve.deployment
class BlockingHTTP:
    async def __call__(self, request):
        # ❌ This blocks the event loop until the HTTP call finishes.
        import requests
        resp = requests.get("https://example.com/")
        return resp.text
# __blocking_http_end__


# __async_http_begin__
@serve.deployment
class AsyncHTTP:
    async def __call__(self, request):
        import httpx

        async with httpx.AsyncClient() as client:
            resp = await client.get("https://example.com/")
        return resp.text
# __async_http_end__


# __threaded_http_begin__
@serve.deployment
class ThreadedHTTP:
    async def __call__(self, request):
        import requests

        def fetch():
            return requests.get("https://example.com/").text

        # ✅ Offload blocking I/O to a worker thread.
        return await asyncio.to_thread(fetch)
# __threaded_http_end__


# __numpy_deployment_begin__
@serve.deployment
class NumpyDeployment:
    def _heavy_numpy(self, array):
        import numpy as np
        # Many NumPy ops release the GIL while executing C/Fortran code.
        return np.linalg.svd(array)[0]

    async def __call__(self, request):
        import numpy as np
        # Create a sample array from request data
        array = np.random.rand(100, 100)
        # ✅ Multiple threads can run _heavy_numpy in parallel if
        # the underlying implementation releases the GIL.
        return await asyncio.to_thread(self._heavy_numpy, array)
# __numpy_deployment_end__


# __max_ongoing_requests_begin__
@serve.deployment(max_ongoing_requests=32)
class MyService:
    async def __call__(self, request):
        await asyncio.sleep(1)
        return "ok"
# __max_ongoing_requests_end__


# __async_io_bound_begin__
@serve.deployment(max_ongoing_requests=100)
class AsyncIOBound:
    async def __call__(self, request):
        # Mostly waiting on an external system.
        await asyncio.sleep(0.1)
        return "ok"
# __async_io_bound_end__


# __blocking_cpu_begin__
@serve.deployment(max_ongoing_requests=100)
class BlockingCPU:
    def __call__(self, request):
        # ❌ Blocks the user event loop.
        import time
        time.sleep(1)
        return "ok"
# __blocking_cpu_end__


# __cpu_with_threadpool_begin__
@serve.deployment(max_ongoing_requests=100)
class CPUWithThreadpool:
    def __call__(self, request):
        # With RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1, each call runs in a thread.
        import time
        time.sleep(1)
        return "ok"
# __cpu_with_threadpool_end__


# __batched_model_begin__

@serve.deployment(max_ongoing_requests=64)
class BatchedModel:
    @serve.batch(max_batch_size=32)
    async def __call__(self, requests):
        # requests is a list of request objects.
        inputs = [r for r in requests]
        outputs = await self._run_model(inputs)
        return outputs
    
    async def _run_model(self, inputs):
        # Placeholder model function
        return [f"result_{i}" for i in inputs]
# __batched_model_end__


# __batched_model_offload_begin__
@serve.deployment(max_ongoing_requests=64)
class BatchedModelOffload:
    @serve.batch(max_batch_size=32)
    async def __call__(self, requests):
        # requests is a list of request objects.
        inputs = [r for r in requests]
        outputs = await self._run_model(inputs)
        return outputs

    async def _run_model(self, inputs):
        def run_sync():
            # Heavy CPU or GIL-releasing native code here.
            # Placeholder model function
            return [f"result_{i}" for i in inputs]

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, run_sync)
# __batched_model_offload_end__


# __blocking_stream_begin__
@serve.deployment
class BlockingStream:
    def __call__(self, request):
        # ❌ Blocks the event loop between yields.
        import time
        for i in range(10):
            time.sleep(1)
            yield f"{i}\n"
# __blocking_stream_end__


# __async_stream_begin__
@serve.deployment
class AsyncStream:
    async def __call__(self, request):
        # ✅ Yields items without blocking the loop.
        async def generator():
            for i in range(10):
                await asyncio.sleep(1)
                yield f"{i}\n"

        return generator()
# __async_stream_end__


# __offload_io_begin__
@serve.deployment
class OffloadIO:
    async def __call__(self, request):
        import requests

        def fetch():
            return requests.get("https://example.com/").text

        # Offload to a thread, free the event loop.
        body = await asyncio.to_thread(fetch)
        return body
# __offload_io_end__


# __offload_cpu_begin__
@serve.deployment
class OffloadCPU:
    def _compute(self, x):
        # CPU-intensive work.
        total = 0
        for i in range(10_000_000):
            total += (i * x) % 7
        return total

    async def __call__(self, request):
        x = 123
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._compute, x)
        return str(result)
# __offload_cpu_end__


# __ray_parallel_begin__
import ray

@ray.remote
def heavy_task(x):
    # Heavy compute runs in its own worker process.
    return x * x


@serve.deployment
class RayParallel:
    async def __call__(self, request):
        values = [1, 2, 3, 4]
        refs = [heavy_task.remote(v) for v in values]
        results = await asyncio.gather(*[r for r in refs])
        return {"results": results}
# __ray_parallel_end__


if __name__ == "__main__":
    import ray
    
    # Initialize Ray if not already running
    if not ray.is_initialized():
        ray.init()
    
    print("Testing Echo deployment...")
    # Test Echo
    echo_handle = serve.run(Echo.bind())
    result = echo_handle.remote(None).result()
    print(f"Echo result: {result}")
    assert result == "ok"
    
    print("\nTesting BlockingEcho deployment...")
    # Test BlockingEcho
    blocking_handle = serve.run(BlockingEcho.bind())
    result = blocking_handle.remote(None).result()
    print(f"BlockingEcho result: {result}")
    assert result == "ok"
    
    print("\nTesting MyService deployment...")
    # Test MyService
    service_handle = serve.run(MyService.bind())
    result = service_handle.remote(None).result()
    print(f"MyService result: {result}")
    assert result == "ok"
    
    print("\nTesting AsyncIOBound deployment...")
    # Test AsyncIOBound
    io_bound_handle = serve.run(AsyncIOBound.bind())
    result = io_bound_handle.remote(None).result()
    print(f"AsyncIOBound result: {result}")
    assert result == "ok"
    
    print("\nTesting AsyncStream deployment...")
    # Test AsyncStream (just create it, don't fully consume)
    stream_handle = serve.run(AsyncStream.bind())
    print("AsyncStream deployment created successfully")
    
    print("\nTesting OffloadCPU deployment...")
    # Test OffloadCPU
    cpu_handle = serve.run(OffloadCPU.bind())
    result = cpu_handle.remote(None).result()
    print(f"OffloadCPU result: {result}")
    
    print("\nTesting NumpyDeployment...")
    # Test NumpyDeployment
    numpy_handle = serve.run(NumpyDeployment.bind())
    result = numpy_handle.remote(None).result()
    print(f"NumpyDeployment result shape: {result.shape}")
    assert result.shape == (100, 100)
    
    print("\nTesting BlockingCPU deployment...")
    # Test BlockingCPU
    blocking_cpu_handle = serve.run(BlockingCPU.bind())
    result = blocking_cpu_handle.remote(None).result()
    print(f"BlockingCPU result: {result}")
    assert result == "ok"
    
    print("\nTesting CPUWithThreadpool deployment...")
    # Test CPUWithThreadpool
    cpu_threadpool_handle = serve.run(CPUWithThreadpool.bind())
    result = cpu_threadpool_handle.remote(None).result()
    print(f"CPUWithThreadpool result: {result}")
    assert result == "ok"
    
    print("\nTesting BlockingStream deployment...")
    # Test BlockingStream - just verify it can be created and called
    blocking_stream_handle = serve.run(BlockingStream.bind())
    # For generator responses, we need to handle them differently
    # Just verify deployment works
    print("BlockingStream deployment created successfully")

    print("\nTesting RayParallel deployment...")
    # Test RayParallel
    ray_parallel_handle = serve.run(RayParallel.bind())
    result = ray_parallel_handle.remote(None).result()
    print(f"RayParallel result: {result}")
    assert result == {"results": [1, 4, 9, 16]}

    print("\nTesting BatchedModel deployment...")
    # Test BatchedModel
    batched_model_handle = serve.run(BatchedModel.bind())
    result = batched_model_handle.remote(1).result()
    print(f"BatchedModel result: {result}")
    assert result == "result_1"

    print("\nTesting BatchedModelOffload deployment...")
    # Test BatchedModelOffload
    batched_model_offload_handle = serve.run(BatchedModelOffload.bind())
    result = batched_model_offload_handle.remote(1).result()
    print(f"BatchedModelOffload result: {result}")
    assert result == "result_1"
    
    # Test HTTP-related deployments with try-except
    print("\n--- Testing HTTP-related deployments (may fail due to network) ---")
    
    print("\nTesting BlockingHTTP deployment...")
    try:
        blocking_http_handle = serve.run(BlockingHTTP.bind())
        result = blocking_http_handle.remote(None).result()
        print(f"BlockingHTTP result (first 50 chars): {result[:50]}...")
        print("✅ BlockingHTTP test passed")
    except Exception as e:
        print(f"⚠️  BlockingHTTP test failed (expected): {type(e).__name__}: {e}")
    
    print("\nTesting AsyncHTTP deployment...")
    try:
        async_http_handle = serve.run(AsyncHTTP.bind())
        result = async_http_handle.remote(None).result()
        print(f"AsyncHTTP result (first 50 chars): {result[:50]}...")
        print("✅ AsyncHTTP test passed")
    except Exception as e:
        print(f"⚠️  AsyncHTTP test failed (expected): {type(e).__name__}: {e}")
    
    print("\nTesting ThreadedHTTP deployment...")
    try:
        threaded_http_handle = serve.run(ThreadedHTTP.bind())
        result = threaded_http_handle.remote(None).result()
        print(f"ThreadedHTTP result (first 50 chars): {result[:50]}...")
        print("✅ ThreadedHTTP test passed")
    except Exception as e:
        print(f"⚠️  ThreadedHTTP test failed (expected): {type(e).__name__}: {e}")
    
    print("\nTesting OffloadIO deployment...")
    try:
        offload_io_handle = serve.run(OffloadIO.bind())
        result = offload_io_handle.remote(None).result()
        print(f"OffloadIO result (first 50 chars): {result[:50]}...")
        print("✅ OffloadIO test passed")
    except Exception as e:
        print(f"⚠️  OffloadIO test failed (expected): {type(e).__name__}: {e}")
    
    print("\nTesting FastAPIDeployment...")
    fastapi_handle = serve.run(FastAPIDeployment.bind())
    # Give it a moment to start
    import time
    import requests
    time.sleep(2)
    # Test the sync endpoint
    response = requests.get("http://127.0.0.1:8000/sync", timeout=5)
    print(f"FastAPIDeployment /sync result: {response.json()}")
    # Test the async endpoint
    response = requests.get("http://127.0.0.1:8000/async", timeout=5)
    print(f"FastAPIDeployment /async result: {response.json()}")
    print("✅ FastAPIDeployment test passed")

    print("\n✅ All core tests passed!")
