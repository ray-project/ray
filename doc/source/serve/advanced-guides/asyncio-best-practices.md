(serve-asyncio-best-practices)=

# Asyncio and concurrency best practices in Ray Serve

Ray Serve is built on top of Python's asyncio event loop. Your handler code runs inside that loop, so whether your code is *blocking* or *non-blocking* directly affects latency and throughput.

This guide explains:

- When to use `async def` versus `def` in Ray Serve.
- How Ray Serve executes your code (loops, threads, and the router).
- How `max_ongoing_requests` interacts with asyncio concurrency.
- How to think about Python's GIL, native code, and true parallelism.

The examples assume the following imports unless stated otherwise:

```python
from ray import serve
import asyncio
```

## Quick guide: choose between `async def` and `def`

Use this decision table as a starting point:

| Workload type | Recommended handler | Why |
| --- | --- | --- |
| I/O-bound (databases, HTTP calls, queues) | `async def` | Lets the event loop handle many requests while each waits on I/O. |
| CPU-bound (model inference, heavy numeric compute) | `def` or `async def` + offload | Async alone does not make CPU work faster. You need more replicas, threads, or native parallelism. |
| Streaming responses | `async def` generator | Integrates with backpressure and non-blocking iteration. |
| FastAPI ingress (`@serve.ingress`) | `def` or `async def` | FastAPI runs `def` endpoints in a threadpool, so they do not block the loop. |

You can always mix styles inside a deployment. For example, use `async def` for request handling and offload CPU-heavy parts to a threadpool or Ray tasks.

## How Ray Serve executes your code

At a high level, requests go through a router to a replica actor that runs your code:

```text
Client
   ↓
Serve router (asyncio loop A)
   ↓
Replica actor
   ├─ System / control loop
   └─ User code loop (your handlers)
       └─ Optional threadpool for sync methods
```

Key ideas:

- Serve uses asyncio event loops for routing and for running replicas.
- By default, user code runs on a separate event loop from the replica's main/control loop, so blocking user code does not interfere with health checks and autoscaling.
- Depending on configuration, `def` handlers may run directly on the user event loop (blocking) or in a threadpool (non-blocking for the loop).

### Pure Serve deployments (no FastAPI ingress)

For a simple deployment:

```python
@serve.deployment
class Echo:
    async def __call__(self, request):
        await asyncio.sleep(0.1)
        return "ok"
```

- `async def __call__` runs directly on the replica's user event loop.
- While this handler awaits `asyncio.sleep`, the loop is free to start handling other requests.

For a synchronous deployment:

```python
@serve.deployment
class BlockingEcho:
    def __call__(self, request):
        # Blocking.
        import time
        time.sleep(1)
        return "ok"
```

How this method executes depends on configuration:

- With `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=0` (current default), `__call__` runs directly on the user event loop and blocks it for 1 second.
- With `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1`, Serve offloads `__call__` to a threadpool so the event loop stays responsive.

### FastAPI ingress (`@serve.ingress`)

When you use FastAPI ingress, FastAPI controls how endpoints run:

```python
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
```

Important differences:

- In FastAPI, `def` endpoints are always dispatched to a threadpool.
- In pure Serve, `def` methods currently run on the event loop unless you opt into threadpool behavior.

## Blocking vs non-blocking in practice

Blocking code keeps the event loop from processing other work. Non-blocking code yields control back to the loop when it is waiting on something.

### Blocking in `async def` is still blocking

This handler looks asynchronous but is not:

```python
@serve.deployment
class BadHandler:
    async def __call__(self, request):
        # ❌ Blocking: time.sleep blocks the entire event loop.
        import time
        time.sleep(2)
        return "done"
```

Even though the method is `async def`, `time.sleep` blocks the loop. No other requests can run on this replica during those 2 seconds.

The correct version uses `asyncio.sleep`:

```python
@serve.deployment
class GoodHandler:
    async def __call__(self, request):
        # ✅ Non-blocking: allows other requests to run.
        await asyncio.sleep(2)
        return "done"
```

### Blocking I/O vs asynchronous I/O

Blocking I/O example:

```python
@serve.deployment
class BlockingHTTP:
    async def __call__(self, request):
        # ❌ This blocks the event loop until the HTTP call finishes.
        import requests
        resp = requests.get("https://example.com/")
        return resp.text
```

Non-blocking equivalent with async HTTP client:

```python
@serve.deployment
class AsyncHTTP:
    async def __call__(self, request):
        import httpx

        async with httpx.AsyncClient() as client:
            resp = await client.get("https://example.com/")
        return resp.text
```

Non-blocking equivalent using a threadpool:

```python
@serve.deployment
class ThreadedHTTP:
    async def __call__(self, request):
        import requests

        def fetch():
            return requests.get("https://example.com/").text

        # ✅ Offload blocking I/O to a worker thread.
        return await asyncio.to_thread(fetch)
```

## Concurrency does not equal parallelism in Python

It is common to expect `async` code to "use all the cores" or make CPU-heavy code faster. That is not what asyncio does.

### Concurrency: handling many waiting operations

Asyncio gives you **concurrency** for I/O-bound workloads:

- While one request waits on the database, another can wait on an HTTP call.
- Handlers yield back to the event loop at each `await`.

This is ideal for high-throughput APIs that mostly wait on external systems.

### Parallelism: using multiple CPU cores

True CPU parallelism usually comes from:

- Multiple processes (for example, multiple Serve replicas or Ray tasks).
- Native code that releases the GIL and runs across cores.

Python's GIL means that pure Python bytecode runs one thread at a time in a process, even if you use a threadpool.

### Using GIL-releasing native code

Many numeric and ML libraries release the GIL while doing heavy work in native code:

- NumPy, many linear algebra routines.
- PyTorch and some other deep learning frameworks.
- Some image-processing or compression libraries.

In these cases, you can still get useful parallelism from threads inside a single replica process:

```python
@serve.deployment
class NumpyDeployment:
    def _heavy_numpy(self, array):
        import numpy as np
        # Many NumPy ops release the GIL while executing C/Fortran code.
        return np.linalg.svd(array)[0]

    async def __call__(self, request):
        array = ...
        # ✅ Multiple threads can run _heavy_numpy in parallel if
        # the underlying implementation releases the GIL.
        return await asyncio.to_thread(self._heavy_numpy, array)
```

However:

- GIL-releasing behavior is library-specific and sometimes operation-specific.
- Some libraries use their own internal threadpools; combining them with your own threadpools can oversubscribe CPUs.
- You should verify that your model stack is thread-safe before relying on this form of parallelism.

For predictable CPU scaling, it is usually simpler to increase the number of replicas or offload work to Ray tasks.

### Summary

- `async def` improves **concurrency** for I/O-bound code.
- CPU-bound code does not become faster just because it is `async`.
- Parallel CPU scaling comes mostly from **more processes** (replicas or tasks) and, in some cases, native code that releases the GIL.

## How `max_ongoing_requests` and replica concurrency work

Each deployment has a `max_ongoing_requests` configuration that controls how many in-flight requests a replica will handle at once.

```python
@serve.deployment(max_ongoing_requests=32)
class MyService:
    async def __call__(self, request):
        await asyncio.sleep(1)
        return "ok"
```

Key points:

- Ray Serve uses an internal semaphore to limit concurrent in-flight requests per replica to `max_ongoing_requests`.
- Requests beyond that limit queue in the router or handle until capacity becomes available, or they fail with backpressure depending on configuration.

How useful `max_ongoing_requests` is depends on how your handler behaves.

### `async` handlers and `max_ongoing_requests`

With an `async def` handler that spends most of its time awaiting I/O, `max_ongoing_requests` directly controls concurrency:

```python
@serve.deployment(max_ongoing_requests=100)
class AsyncIOBound:
    async def __call__(self, request):
        # Mostly waiting on an external system.
        await asyncio.sleep(0.1)
        return "ok"
```

- Up to 100 requests can be in-flight per replica.
- While one request is waiting, the event loop can work on others.

### Blocking `def` handlers and `max_ongoing_requests`

With a blocking `def` handler that runs on the event loop (threadpool disabled), `max_ongoing_requests` does not give you the concurrency you expect:

```python
@serve.deployment(max_ongoing_requests=100)
class BlockingCPU:
    def __call__(self, request):
        # ❌ Blocks the user event loop.
        import time
        time.sleep(1)
        return "ok"
```

In this case:

- The event loop can only run one handler at a time.
- Even though `max_ongoing_requests=100`, the replica effectively processes requests serially.

If you enable the sync-in-threadpool behavior (see the next section), each in-flight request can run in a thread:

```python
@serve.deployment(max_ongoing_requests=100)
class CPUWithThreadpool:
    def __call__(self, request):
        # With RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1, each call runs in a thread.
        import time
        time.sleep(1)
        return "ok"
```

Now:

- Up to `max_ongoing_requests` calls can be running at once.
- Real throughput depends on:
  - How many threads the threadpool uses.
  - Whether your workload is CPU-bound or GIL-releasing.
  - Underlying native libraries and system resources.

For heavily CPU-bound workloads, it is usually better to:

- Keep `max_ongoing_requests` modest (to avoid queueing too many heavy tasks), and
- Scale **replicas** (`num_replicas`) rather than pushing a single replica's concurrency too high.

## Environment flags and sync-in-threadpool warning

Ray Serve exposes several environment variables that control how user code interacts with event loops and threads.

### `RAY_SERVE_RUN_SYNC_IN_THREADPOOL`

By default (`RAY_SERVE_RUN_SYNC_IN_THREADPOOL=0`), synchronous methods in a deployment run directly on the user event loop. To help you migrate to a safer model, Serve emits a warning like:

> `RAY_SERVE_RUN_SYNC_IN_THREADPOOL_WARNING`: Calling sync method '...' directly on the asyncio loop. In a future version, sync methods will be run in a threadpool by default...

This warning means:

- You have a `def` method that is currently running on the event loop.
- In a future version, that method will run in a threadpool instead.

You can opt in to the future behavior now by setting:

```bash
export RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1
```

When this flag is `1`:

- Serve runs synchronous methods in a threadpool.
- The event loop is free to keep serving other requests while sync methods run.

Before enabling this in production, make sure:

- Your handler code and any shared state are thread-safe.
- Your model objects can safely be used from multiple threads, or you protect them with locks.

### `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD`

By default, Serve runs user code in a separate event loop from the replica's main/control loop:

```bash
export RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=1  # default
```

This isolation:

- Protects system tasks (health checks, controller communication) from being blocked by user code.
- Adds a some amount of overhead to cross-loop communication.

You can disable this behavior:

```bash
export RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=0
```

Only advanced users should change this. When user code and system tasks share a loop, any blocking operation in user code can interfere with replica health and control-plane operations.

### `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP`

Serve's request router is also run on its own event loop by default:

```bash
export RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=1  # default
```

This ensures:

- The router can continue routing and load balancing requests even if some replicas are running slow user code.

Disabling this:

```bash
export RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0
```

makes the router share an event loop with other work. This can reduce overhead in advanced, highly optimized scenarios, but makes the system more sensitive to blocking operations.

For most production deployments, you should keep the defaults (`1`) for both separate-loop flags.

## Batching and streaming semantics

Batching and streaming both rely on the event loop to stay responsive. They do not change where your code runs: batched handlers and streaming handlers still run on the same user event loop as any other handler. This means that if you add batching or streaming on top of blocking code, you can make event loop blocking effects much worse.

### Batching

When you enable batching, Serve groups multiple incoming requests together and passes them to your handler as a list. The handler still runs on the user event loop, but each call now processes many requests at once instead of just one. If that batched work is blocking, it blocks the event loop for all of those requests at the same time.

The following example shows a batched deployment:

```python

class BatchedModel:
    @serve.deployment(batch_size=32, max_ongoing_requests=64)
    async def __call__(self, requests):
        # requests is a list of request objects.
        inputs = [await r.json() for r in requests]
        outputs = await self._run_model(inputs)
        return outputs
```

The batch handler runs on the user event loop:

- If `_run_model` is CPU-heavy and runs inline, it blocks the loop for the duration of the batch.
- You can offload the batch computation:

```python
    async def _run_model(self, inputs):
        def run_sync():
            # Heavy CPU or GIL-releasing native code here.
            return [self.model(i) for i in inputs]

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, run_sync)
```

This keeps the event loop responsive while the model runs in a thread.

### Streaming

Streaming is different from a regular response because the client starts receiving data while your handler is still running. Serve calls your handler once, gets back a generator or async generator, and then repeatedly asks it for the next chunk. That generator code still runs on the user event loop (or in a worker thread if you offload it).

Streaming is especially sensitive to blocking:

- If you block between chunks, you delay the next piece of data to the client.
- While the generator is blocked on the event loop, other requests on that loop cannot make progress.
- The system also cannot react quickly to slow clients (backpressure) or cancellation.

Bad streaming example:

```python
@serve.deployment
class BlockingStream:
    def __call__(self, request):
        # ❌ Blocks the event loop between yields.
        import time
        for i in range(10):
            time.sleep(1)
            yield f"{i}\n"
```

Better streaming example:

```python
@serve.deployment
class AsyncStream:
    async def __call__(self, request):
        # ✅ Yields items without blocking the loop.
        async def generator():
            for i in range(10):
                await asyncio.sleep(1)
                yield f"{i}\n"

        return generator()
```

In streaming scenarios:

- Prefer `async def` generators that use `await` between yields.
- Avoid long CPU-bound loops between yields; offload them if needed.

## Offloading patterns: I/O, CPU, and Ray tasks

This section summarizes common offloading patterns you can use inside `async` handlers.

### Blocking I/O in `async def`

```python
@serve.deployment
class OffloadIO:
    async def __call__(self, request):
        import requests

        def fetch():
            return requests.get("https://example.com/").text

        # Offload to a thread, free the event loop.
        body = await asyncio.to_thread(fetch)
        return body
```

### CPU-heavy code in `async def`

```python
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
```

### Using Ray tasks or remote actors for true parallelism

```python
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
```

This pattern:

- Uses multiple Ray workers and processes.
- Bypasses the GIL limitation of a single Python process.

## Summary

- Use `async def` for I/O-bound and streaming work so the event loop can stay responsive.
- Use `max_ongoing_requests` to bound concurrency per replica, but remember that blocking `def` handlers can still serialize work if they run on the event loop.
- Consider enabling `RAY_SERVE_RUN_SYNC_IN_THREADPOOL` once your code is thread-safe, and be aware of the sync-in-threadpool warning.
- For CPU-heavy workloads, scale replicas or use Ray tasks and GIL-releasing native code for real parallelism.
