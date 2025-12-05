(serve-asyncio-best-practices)=

# Asyncio and concurrency best practices in Ray Serve

The code that runs inside of each replica in a Ray Serve deployment runs on an asyncio event loop.
Asyncio enables efficient I/O bound concurrency but requires following a few best practices for optimal performance.

This guide explains:

- When to use `async def` versus `def` in Ray Serve.
- How Ray Serve executes your code (loops, threads, and the router).
- How `max_ongoing_requests` interacts with asyncio concurrency.
- How to think about Python's GIL, native code, and true parallelism.

The examples assume the following imports unless stated otherwise:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __imports_begin__
:end-before: __imports_end__
:language: python
```

## How to choose between `async def` and `def`

Use this decision table as a starting point:

| Workload type | Recommended handler | Reason |
| --- | --- | --- |
| I/O-bound (databases, HTTP calls, queues) | `async def` | Lets the event loop handle many requests while each waits on I/O. |
| CPU-bound (model inference, heavy numeric compute) | `def` or `async def` with offload | Async alone doesn't make CPU work faster. You need more replicas, threads, or native parallelism. |
| Streaming responses | `async def` generator | Integrates with backpressure and non-blocking iteration. |
| FastAPI ingress (`@serve.ingress`) | `def` or `async def` | FastAPI runs `def` endpoints in a threadpool, so they don't block the loop. |

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

The following are the key ideas to consider when deciding to use `async def` or `def`:

- Serve uses asyncio event loops for routing and for running replicas.
- By default, user code runs on a separate event loop from the replica's main/control loop, so blocking user code doesn't interfere with health checks and autoscaling.
- Depending on the value of `RAY_SERVE_RUN_SYNC_IN_THREADPOOL`, `def` handlers may run directly on the user event loop (blocking) or in a threadpool (non-blocking for the loop).

### Pure Serve deployments (no FastAPI ingress)

For a simple deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __echo_async_begin__
:end-before: __echo_async_end__
:language: python
```

- `async def __call__` runs directly on the replica's user event loop.
- While this handler awaits `asyncio.sleep`, the loop is free to start handling other requests.

For a synchronous deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_echo_begin__
:end-before: __blocking_echo_end__
:language: python
```

How this method executes depends on configuration:

- With `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=0` (current default), `__call__` runs directly on the user event loop and blocks it for 1 second.
- With `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1`, Serve offloads `__call__` to a threadpool so the event loop stays responsive.

### FastAPI ingress (`@serve.ingress`)

When you use FastAPI ingress, FastAPI controls how endpoints run:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __fastapi_deployment_begin__
:end-before: __fastapi_deployment_end__
:language: python
```

Important differences:

- FastAPI  always dispatches `def` endpoints to a threadpool.
- In pure Serve, `def` methods run on the event loop unless you opt into threadpool behavior.

## Blocking versus non-blocking in practice

Blocking code keeps the event loop from processing other work. Non-blocking code yields control back to the loop when it's waiting on something.

### Blocking I/O versus asynchronous I/O

Blocking I/O example:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_http_begin__
:end-before: __blocking_http_end__
:language: python
```

Even though the method is `async def`, `requests.get` blocks the loop. No other requests can run on this replica during the request call. Blocking in `async def` is still blocking.

Non-blocking equivalent with async HTTP client:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __async_http_begin__
:end-before: __async_http_end__
:language: python
```

Non-blocking equivalent using a threadpool:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __threaded_http_begin__
:end-before: __threaded_http_end__
:language: python
```

## Concurrency doesn't equal parallelism in Python

It's common to expect `async` code to "use all the cores" or make CPU-heavy code faster. asyncio doesn't do that.

### Concurrency: Handling many waiting operations

Asyncio gives you **concurrency** for I/O-bound workloads:

- While one request waits on the database, another can wait on an HTTP call.
- Handlers yield back to the event loop at each `await`.

This is ideal for high-throughput APIs that mostly wait on external systems.

### Parallelism: Using multiple CPU cores

True CPU parallelism usually comes from:

- Multiple processes (for example, multiple Serve replicas).
- Native code that releases the GIL and runs across cores.

Python's GIL means that pure Python bytecode runs one thread at a time in a process, even if you use a threadpool.

### Using GIL-releasing native code

Many numeric and ML libraries release the GIL while doing heavy work in native code:

- NumPy, many linear algebra routines.
- PyTorch and some other deep learning frameworks.
- Some image-processing or compression libraries.

In these cases, you can still get useful parallelism from threads inside a single replica process:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __numpy_deployment_begin__
:end-before: __numpy_deployment_end__
:language: python
```

However:

- GIL-releasing behavior is library-specific and sometimes operation-specific.
- Some libraries use their own internal threadpools; combining them with your own threadpools can oversubscribe CPUs.
- You should verify that your model stack is thread-safe before relying on this form of parallelism.

For predictable CPU scaling, it's usually simpler to increase the number of replicas.

### Summary

- `async def` improves **concurrency** for I/O-bound code.
- CPU-bound code doesn't become faster merely because it's `async`.
- Parallel CPU scaling comes mostly from **more processes** (replicas or tasks) and, in some cases, native code that releases the GIL.

## How `max_ongoing_requests` and replica concurrency work

Each deployment has a `max_ongoing_requests` configuration that controls how many in-flight requests a replica handles at once.

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __max_ongoing_requests_begin__
:end-before: __max_ongoing_requests_end__
:language: python
```

Key points:

- Ray Serve uses an internal semaphore to limit concurrent in-flight requests per replica to `max_ongoing_requests`.
- Requests beyond that limit queue in the router or handle until capacity becomes available, or they fail with backpressure depending on configuration.

How useful `max_ongoing_requests` is depends on how your handler behaves.

### `async` handlers and `max_ongoing_requests`

With an `async def` handler that spends most of its time awaiting I/O, `max_ongoing_requests` directly controls concurrency:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __async_io_bound_begin__
:end-before: __async_io_bound_end__
:language: python
```

- Up to 100 requests can be in-flight per replica.
- While one request is waiting, the event loop can work on others.

### Blocking `def` handlers and `max_ongoing_requests`

With a blocking `def` handler that runs on the event loop (threadpool disabled), `max_ongoing_requests` doesn't give you the concurrency you expect:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_cpu_begin__
:end-before: __blocking_cpu_end__
:language: python
```

In this case:

- The event loop can only run one handler at a time.
- Even though `max_ongoing_requests=100`, the replica effectively processes requests serially.

If you enable the sync-in-threadpool behavior (see the next section), each in-flight request can run in a thread:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __cpu_with_threadpool_begin__
:end-before: __cpu_with_threadpool_end__
:language: python
```

Now:

- Up to `max_ongoing_requests` calls can be running at once.
- Real throughput depends on:
  - How many threads the threadpool uses.
  - Whether your workload is CPU-bound or GIL-releasing.
  - Underlying native libraries and system resources.

For heavily CPU-bound workloads, it's usually better to:

- Keep `max_ongoing_requests` modest (to avoid queueing too many heavy tasks), and
- Scale **replicas** (`num_replicas`) rather than pushing a single replica's concurrency too high.

## Environment flags and sync-in-threadpool warning

Ray Serve exposes several environment variables that control how user code interacts with event loops and threads.

### `RAY_SERVE_RUN_SYNC_IN_THREADPOOL`

By default (`RAY_SERVE_RUN_SYNC_IN_THREADPOOL=0`), which means synchronous methods in a deployment run directly on the user event loop. To help you migrate to a safer model, Serve emits a warning like:

> `RAY_SERVE_RUN_SYNC_IN_THREADPOOL_WARNING`: Calling sync method '...' directly on the asyncio loop. In a future version, sync methods will be run in a threadpool by default...

This warning means:

- You have a `def` method that is currently running on the event loop.
- In a future version, that method runs in a threadpool instead.

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
- Adds some amount of overhead to cross-loop communication, resulting in higher latency in request. For throughput-optimized configurations, see [High throughput optimization](serve-high-throughput). 

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

makes the router share an event loop with other work. This can reduce overhead in advanced, highly optimized scenarios, but makes the system more sensitive to blocking operations. See [High throughput optimization](serve-high-throughput). 

For most production deployments, you should keep the defaults (`1`) for both separate-loop flags.

## Batching and streaming semantics

Batching and streaming both rely on the event loop to stay responsive. They don't change where your code runs: batched handlers and streaming handlers still run on the same user event loop as any other handler. This means that if you add batching or streaming on top of blocking code, you can make event loop blocking effects much worse.

### Batching

When you enable batching, Serve groups multiple incoming requests together and passes them to your handler as a list. The handler still runs on the user event loop, but each call now processes many requests at once instead of just one. If that batched work is blocking, it blocks the event loop for all of those requests at the same time.

The following example shows a batched deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __batched_model_begin__
:end-before: __batched_model_end__
:language: python
```

The batch handler runs on the user event loop:

- If `_run_model` is CPU-heavy and runs inline, it blocks the loop for the duration of the batch.
- You can offload the batch computation:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __batched_model_offload_begin__
:end-before: __batched_model_offload_end__
:language: python
:emphasize-lines: 9-16
```

This keeps the event loop responsive while the model runs in a thread.

#### `max_concurrent_batches` and event loop yielding

The `@serve.batch` decorator accepts a `max_concurrent_batches` argument that controls how many batches can be processed concurrently. However, this argument only works effectively if your batch handler yields control back to the event loop during processing.

If your batch handler blocks the event loop (for example, by doing heavy CPU work without awaiting or offloading), `max_concurrent_batches` won't provide the concurrency you expect. The event loop can only start processing a new batch when the current batch yields control.

To get the benefit of `max_concurrent_batches`:

- Use `async def` for your batch handler and `await` I/O operations or offloaded CPU work.
- Offload CPU-heavy batch processing to a threadpool with `asyncio.to_thread()` or `loop.run_in_executor()`.
- Avoid blocking operations that prevent the event loop from scheduling other batches.

In the offloaded batch example above, the handler yields to the event loop when awaiting the threadpool executor, which allows multiple batches to be in flight simultaneously (up to the `max_concurrent_batches` limit).

### Streaming

Streaming is different from a regular response because the client starts receiving data while your handler is still running. Serve calls your handler once, gets back a generator or async generator, and then repeatedly asks it for the next chunk. That generator code still runs on the user event loop (or in a worker thread if you offload it).

Streaming is especially sensitive to blocking:

- If you block between chunks, you delay the next piece of data to the client.
- While the generator is blocked on the event loop, other requests on that loop can't make progress.
- The system also cannot react quickly to slow clients (backpressure) or cancellation.

Bad streaming example:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_stream_begin__
:end-before: __blocking_stream_end__
:language: python
```

Better streaming example:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __async_stream_begin__
:end-before: __async_stream_end__
:language: python
```

In streaming scenarios:

- Prefer `async def` generators that use `await` between yields.
- Avoid long CPU-bound loops between yields; offload them if needed.

## Offloading patterns: I/O, CPU

This section summarizes common offloading patterns you can use inside `async` handlers.

### Blocking I/O in `async def`

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __offload_io_begin__
:end-before: __offload_io_end__
:language: python
```

### CPU-heavy code in `async def`

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __offload_cpu_begin__
:end-before: __offload_cpu_end__
:language: python
```

### (Advance) Using Ray tasks or remote actors for true parallelism

:::{note}
While you can spawn Ray tasks from Ray Serve deployments, this approach isn't recommended because it lacks tooling for observability and debugging.
:::

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __ray_parallel_begin__
:end-before: __ray_parallel_end__
:language: python
```

This pattern:

- Uses multiple Ray workers and processes.
- Bypasses the GIL limitation of a single Python process.

## Summary

- Use `async def` for I/O-bound and streaming work so the event loop can stay responsive.
- Use `max_ongoing_requests` to bound concurrency per replica, but remember that blocking `def` handlers can still serialize work if they run on the event loop.
- Consider enabling `RAY_SERVE_RUN_SYNC_IN_THREADPOOL` once your code is thread-safe, and be aware of the sync-in-threadpool warning.
- For CPU-heavy workloads, scale replicas or GIL-releasing native code for real parallelism.
